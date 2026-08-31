use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Instant,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use arrow_array::{Array, FixedSizeListArray, Float32Array};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use half::f16;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use std::sync::Arc;

use crate::{
    BorsukError, Result, VectorMetric,
    v23_diagnostic::{V23DecodedPage, V23PageRef, V23QuantizerFamily, decode_v23_page},
    v23_incidence_eval::{
        V23IncidenceCampaignInput, V23IncidenceCampaignResult, V23IncidenceCell,
        V23IncidenceDevelopmentArtifact, V23IncidenceDevelopmentAuthority,
        V23IncidenceHoldoutResult, V23IncidenceHoldoutTruthArtifact,
        V23IncidenceHoldoutTruthAuthority, V23IncidenceQueryWorkspace,
        bind_v23_incidence_holdout_truth, canonical_v23_incidence_development_artifact_bytes,
        canonical_v23_incidence_holdout_truth_bytes, canonical_v23_incidence_result_bytes,
        classify_v23_incidence_campaign, decode_v23_incidence_development_latency_bundle,
        encode_v23_incidence_development_latency_bundle, evaluate_v23_incidence_cell,
        measure_v23_incidence_evaluation_preflight, measure_v23_incidence_latency,
        read_v23_incidence_development_queries, read_v23_incidence_development_truth,
        read_v23_incidence_holdout_neighbors, read_v23_incidence_holdout_queries,
        recompute_v23_incidence_layout_quality, score_incidence_query_native,
    },
    v23_incidence_postings::{
        PostingAssignmentArm, V23_POSTING_MAX_PAGES, V23_POSTING_RUN_BYTES, V23PostingArmRecords,
        V23PostingRecord, build_both_posting_plane_files, build_posting_plane,
        decode_posting_plane, page_posting_records_both,
    },
    v23_incidence_tree::{
        V23_INCIDENCE_PROGRESS_SOURCE_ROWS, V23IncidenceTrainingMilestone, V23TrainingRow,
        V23TreeNode, decode_incidence_tree, encode_incidence_tree, split_score_simd,
        train_incidence_tree,
    },
};

const V23_INCIDENCE_RECEIPT_SCHEMA: &str = "borsuk-v23-incidence-receipt-v2";
const V23_INCIDENCE_MANIFEST_SCHEMA: &str = "borsuk-v23-incidence-manifest-v1";
const V23_INCIDENCE_PREFLIGHT_WALL_LIMIT_NS: u64 = 5_400_000_000_000;
const V23_INCIDENCE_SOURCE_COMMIT: &str = "c339a546f8f9370cb2e6e9fb3b0fd4bdefa3cb05";
const V23_INCIDENCE_SOURCE_ARCHIVE_SHA256: &str =
    "77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d";
const V23_INCIDENCE_INDEX_ID: &str = "index-bcda7bb66812e162d45077e6";
const V23_INCIDENCE_DATASET_ID: &str = "deep-image-96";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Capability-separated phase in the claim-ineligible V23 incidence falsifier.
pub enum V23IncidencePhase {
    /// Corpus-only tree construction.
    TreeTraining,
    /// Query-blind leaf-to-page posting construction.
    PostingConstruction,
    /// Burned-query development-cell evaluation.
    DevelopmentEvaluation,
    /// Sealed holdout neighbor-to-page truth binding.
    HoldoutBinding,
    /// Single-use sealed-cell holdout evaluation.
    HoldoutEvaluation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V23IncidenceReceiptRunMode {
    Preflight,
    Execute,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// One explicit local preflight or execution gate.
pub enum V23IncidenceRunMode {
    /// Run only the fixed resource preflight for the phase.
    Preflight(V23IncidencePhase),
    /// Execute the phase after authenticating its successful preflight.
    Execute(V23IncidencePhase),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V23FmaBackend {
    Aarch64NeonFma,
    X86AvxFma,
    ScalarControl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum V23IncidenceStopClass {
    #[serde(rename = "authority-stop")]
    Authority,
    #[serde(rename = "resource-stop")]
    Resource,
    #[serde(rename = "determinism-stop")]
    Determinism,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Immutable role-specific object identity mounted into one local phase.
pub struct V23IncidenceObjectIdentity {
    /// Registered phase role.
    pub role: String,
    /// Immutable source URI.
    pub uri: String,
    /// Role-specific digest algorithm.
    pub digest_algorithm: String,
    /// Lowercase registered content digest.
    pub digest: String,
    /// Exact encoded object length.
    pub encoded_bytes: u64,
    /// Immutable object generation or version identity.
    pub generation: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One authenticated object identity paired with its sandbox-local path.
pub struct V23IncidenceLocalRolePath {
    /// Registered immutable object identity.
    pub identity: V23IncidenceObjectIdentity,
    /// Absolute local path mounted for the role.
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Complete local-only request for one capability-separated phase process.
pub struct V23IncidenceLocalPhaseRequest {
    /// Explicit preflight or execution gate.
    pub mode: V23IncidenceRunMode,
    /// Absolute authenticated construction or phase manifest path.
    pub manifest_path: PathBuf,
    /// Absolute prior scientific receipt path for later phases.
    pub parent_receipt_path: Option<PathBuf>,
    /// Absolute successful preflight receipt path for execution.
    pub preflight_receipt_path: Option<PathBuf>,
    /// Exact ordered mounted input roles.
    pub input_paths: Vec<V23IncidenceLocalRolePath>,
    /// Phase-private bounded scratch directory.
    pub scratch_path: PathBuf,
    /// Phase-private canonical output receipt path.
    pub output_path: PathBuf,
    /// SHA-256 of the executing release binary.
    pub executable_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Bounded local request whose corpus-sized inputs are represented by one directory.
pub struct V23IncidenceLocalDirectoryPhaseRequest {
    /// Explicit preflight or execution gate.
    pub mode: V23IncidenceRunMode,
    /// Complete scientific phase manifest and its immutable identity.
    pub manifest: V23IncidenceLocalRolePath,
    /// Ordered manifest for only the objects staged into the bulk directory.
    pub bulk_manifest: V23IncidenceLocalRolePath,
    /// Absolute read-only directory containing exact role-named staged objects.
    pub staging_directory_path: PathBuf,
    /// Canonical receipt emitted by the credentialed stager.
    pub staging_receipt: V23IncidenceLocalRolePath,
    /// Successful preflight receipt required only for execution.
    pub preflight_receipt: Option<V23IncidenceLocalRolePath>,
    /// Phase-private bounded scratch directory.
    pub scratch_path: PathBuf,
    /// Phase-private canonical output receipt path.
    pub output_path: PathBuf,
    /// SHA-256 of the executing release binary.
    pub executable_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V23IncidenceStagedObject {
    digest: String,
    digest_algorithm: String,
    encoded_bytes: u64,
    generation: String,
    relative_path: String,
    role: String,
    uri: String,
}

impl V23IncidenceStagedObject {
    fn identity(&self) -> V23IncidenceObjectIdentity {
        V23IncidenceObjectIdentity {
            role: self.role.clone(),
            uri: self.uri.clone(),
            digest_algorithm: self.digest_algorithm.clone(),
            digest: self.digest.clone(),
            encoded_bytes: self.encoded_bytes,
            generation: self.generation.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V23IncidenceStagingReceipt {
    claim_eligible: bool,
    manifest_sha256: String,
    ordered_objects: Vec<V23IncidenceStagedObject>,
    schema: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct V23IncidenceManifestBinding {
    parent_receipt_sha256: Option<String>,
    full_input_bytes: u64,
    ordered_inputs: Vec<V23IncidenceObjectIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "authority_kind", rename_all = "kebab-case")]
pub(crate) enum V23IncidenceInputAuthority {
    DatasetMeta {
        identity: V23IncidenceObjectIdentity,
        physical_schema: String,
        dimensions: u32,
        metric: String,
        train_rows: u64,
        test_rows: u64,
        neighbors_per_query: u32,
    },
    TrainingShard {
        identity: V23IncidenceObjectIdentity,
        ordinal_start: u64,
        ordinal_end: u64,
        physical_schema: String,
        dimensions: u32,
        metric: String,
        rows: u64,
    },
    PhaseObject {
        identity: V23IncidenceObjectIdentity,
    },
}

impl V23IncidenceInputAuthority {
    fn identity(&self) -> &V23IncidenceObjectIdentity {
        match self {
            Self::DatasetMeta { identity, .. }
            | Self::TrainingShard { identity, .. }
            | Self::PhaseObject { identity } => identity,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidenceCapabilityProbes {
    pub(crate) network_namespace_changed: bool,
    pub(crate) host_canary_denied: bool,
    pub(crate) network_canary_denied: bool,
    pub(crate) allowlisted_inputs_opened: bool,
    pub(crate) output_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidenceAlgorithm {
    pub(crate) dimensions: u32,
    pub(crate) reservoir_rows: u32,
    pub(crate) tree_depth: u8,
    pub(crate) leaf_count: u32,
    pub(crate) lloyd_iterations: u8,
    pub(crate) posting_caps: [u16; 3],
    pub(crate) probe_counts: [u16; 3],
    pub(crate) selection_width: u8,
    pub(crate) aggregate_recall_ppm: u64,
    pub(crate) minimum_query_recall_ppm: u64,
    pub(crate) oracle_attainment_ppm: u64,
}

impl V23IncidenceAlgorithm {
    const REGISTERED: Self = Self {
        dimensions: 96,
        reservoir_rows: 2_097_152,
        tree_depth: 16,
        leaf_count: 65_536,
        lloyd_iterations: 4,
        posting_caps: [512, 1024, 2048],
        probe_counts: [32, 64, 128],
        selection_width: 8,
        aggregate_recall_ppm: 975_000,
        minimum_query_recall_ppm: 800_000,
        oracle_attainment_ppm: 995_000,
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidencePreflightWork {
    pub(crate) phase: V23IncidencePhase,
    pub(crate) sample_vectors: u64,
    pub(crate) sample_page_bodies: u64,
    pub(crate) sample_queries: u64,
    pub(crate) sample_records: u64,
    pub(crate) full_distance_dimensions: u64,
    pub(crate) full_records: u64,
    pub(crate) record_kind: V23IncidencePreflightRecordKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V23IncidencePreflightRecordKind {
    None,
    ExternalSort,
    PostingVisits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidencePreflightMeasurement {
    pub(crate) distance_dimensions: u64,
    pub(crate) distance_elapsed_ns: u64,
    pub(crate) input_bytes: u64,
    pub(crate) input_elapsed_ns: u64,
    pub(crate) records: u64,
    pub(crate) records_elapsed_ns: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct V23IncidenceInputMeasurement {
    input_bytes: u64,
    input_elapsed_ns: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct V23IncidenceKernelMeasurement {
    distance_dimensions: u64,
    distance_elapsed_ns: u64,
    fma_backend: V23FmaBackend,
}

fn measure_v23_incidence_tree_preflight(
    rows: &[[f32; 96]],
) -> Result<V23IncidenceKernelMeasurement> {
    if rows.is_empty() || rows.iter().flatten().any(|value| !value.is_finite()) {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence tree preflight rows differ".to_string(),
        ));
    }
    let mut child_zero = [f16::ZERO; 96];
    child_zero[0] = f16::ONE;
    let mut child_one = [f16::ZERO; 96];
    child_one[1] = f16::ONE;
    let node = V23TreeNode {
        child_zero,
        child_one,
        child_zero_inverse_norm: 1.0,
        child_one_inverse_norm: 1.0,
        boundary_score_bits: 0,
        boundary_source_ordinal: 0,
        child_zero_index: 0,
        child_one_index: 0,
    };
    let started = Instant::now();
    let mut backend = None;
    for row in rows {
        let (score, observed_backend) = split_score_simd(&node, row)?;
        std::hint::black_box(score);
        if backend
            .replace(observed_backend)
            .is_some_and(|prior| prior != observed_backend)
        {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence tree preflight backend differs".to_string(),
            ));
        }
    }
    let distance_dimensions = u64::try_from(rows.len())
        .ok()
        .and_then(|rows| rows.checked_mul(2 * 96))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence tree preflight work overflows".to_string())
        })?;
    Ok(V23IncidenceKernelMeasurement {
        distance_dimensions,
        distance_elapsed_ns: u64::try_from(started.elapsed().as_nanos())
            .unwrap_or(u64::MAX)
            .max(1),
        fma_backend: backend.unwrap(),
    })
}

fn measure_v23_incidence_posting_sort_preflight(
    scratch: &Path,
) -> Result<V23IncidencePreflightMeasurement> {
    const SAMPLE_RECORDS: u64 = 1_048_576;
    let started = Instant::now();
    let records = (0..SAMPLE_RECORDS).map(|ordinal| {
        Ok(V23PostingRecord {
            leaf: (ordinal % 65_536) as u16,
            page: ((ordinal / 65_536) % 256) as u32,
            reserved: 0,
        })
    });
    let plane = build_posting_plane(
        records,
        PostingAssignmentArm::OneLeaf,
        scratch,
        SAMPLE_RECORDS as usize,
        2_048,
    )?;
    if plane.source_records != SAMPLE_RECORDS {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence posting preflight record count differs".to_string(),
        ));
    }
    std::hint::black_box(plane);
    Ok(V23IncidencePreflightMeasurement {
        distance_dimensions: 0,
        distance_elapsed_ns: 0,
        input_bytes: 0,
        input_elapsed_ns: 0,
        records: SAMPLE_RECORDS,
        records_elapsed_ns: u64::try_from(started.elapsed().as_nanos())
            .unwrap_or(u64::MAX)
            .max(1),
    })
}

fn decode_v23_incidence_preflight_pages(
    pages: &[(V23IncidenceObjectIdentity, Bytes)],
) -> Result<Vec<V23DecodedPage>> {
    if pages.len() != 256 {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence posting preflight page count differs".to_string(),
        ));
    }
    pages
        .iter()
        .enumerate()
        .map(|(ordinal, (identity, bytes))| {
            decode_v23_incidence_page(identity, bytes.clone(), ordinal)
        })
        .collect()
}

fn decode_v23_incidence_page(
    identity: &V23IncidenceObjectIdentity,
    bytes: Bytes,
    ordinal: usize,
) -> Result<V23DecodedPage> {
    let expected_role = format!("page-body-{ordinal:05}");
    if identity.role != expected_role || identity.digest_algorithm != "blake3" {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence posting page identity differs".to_string(),
        ));
    }
    validate_object_bytes(identity, &bytes)?;
    let generation_checksum: [u8; 32] = bytes
        .get(32..64)
        .and_then(|value| value.try_into().ok())
        .ok_or_else(|| {
        BorsukError::InvalidStorage("V23 incidence posting page header differs".to_string())
    })?;
    let read_u32 = |start: usize| {
        bytes
            .get(start..start + 4)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
    };
    let code_width = bytes
        .get(64..66)
        .and_then(|value| value.try_into().ok())
        .map(u16::from_le_bytes)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence posting page code width differs".to_string())
        })?;
    let page_ref = V23PageRef {
        generation_checksum,
        page_ordinal: u32::try_from(ordinal).map_err(|_| {
            BorsukError::InvalidStorage("V23 incidence posting page ordinal overflows".to_string())
        })?,
        metric: VectorMetric::Cosine,
        dimensions: 96,
        family: V23QuantizerFamily::F16Flat,
        code_width,
        path: format!("pages/{}", identity.digest),
        checksum: identity.digest.clone(),
        encoded_bytes: identity.encoded_bytes,
        primary_rows: read_u32(16).ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence posting primary rows differ".to_string())
        })?,
        replicated_rows: read_u32(20).ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence posting replica rows differ".to_string())
        })?,
    };
    decode_v23_page(bytes, &page_ref)
}

struct V23IncidencePagePostingStream<'a> {
    tree: &'a crate::v23_incidence_tree::V23IncidenceTree,
    pages: std::vec::IntoIter<V23IncidenceLocalRolePath>,
    ordinal: usize,
    pending: std::vec::IntoIter<V23PostingArmRecords>,
    failed: bool,
}

impl Iterator for V23IncidencePagePostingStream<'_> {
    type Item = Result<V23PostingArmRecords>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.failed {
                return None;
            }
            if let Some(record) = self.pending.next() {
                return Some(Ok(record));
            }
            let page = self.pages.next()?;
            let result = (|| {
                authenticate_v23_incidence_local_path(&page.path, &page.identity)?;
                let bytes = fs::read(&page.path).map_err(|source| BorsukError::Io {
                    path: page.path.clone(),
                    source,
                })?;
                let decoded =
                    decode_v23_incidence_page(&page.identity, Bytes::from(bytes), self.ordinal)?;
                self.ordinal += 1;
                page_posting_records_both(self.tree, &decoded).collect::<Result<Vec<_>>>()
            })();
            match result {
                Ok(records) => self.pending = records.into_iter(),
                Err(error) => {
                    self.failed = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

fn v23_incidence_page_posting_stream(
    tree: &crate::v23_incidence_tree::V23IncidenceTree,
    pages: Vec<V23IncidenceLocalRolePath>,
) -> V23IncidencePagePostingStream<'_> {
    V23IncidencePagePostingStream {
        tree,
        pages: pages.into_iter(),
        ordinal: 0,
        pending: Vec::new().into_iter(),
        failed: false,
    }
}

fn measure_v23_incidence_posting_pages_preflight(
    tree_bytes: &[u8],
    pages: &[(V23IncidenceObjectIdentity, Bytes)],
) -> Result<V23IncidenceKernelMeasurement> {
    let tree = decode_incidence_tree(tree_bytes)?;
    let started = Instant::now();
    let decoded_pages = decode_v23_incidence_preflight_pages(pages)?;
    let mut physical_rows = 0_u64;
    for decoded in decoded_pages {
        physical_rows = physical_rows
            .checked_add(
                u64::try_from(decoded.primary_rows() + decoded.replicated_rows()).map_err(
                    |_| {
                        BorsukError::InvalidStorage(
                            "V23 incidence posting preflight row count overflows".to_string(),
                        )
                    },
                )?,
            )
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "V23 incidence posting preflight row count overflows".to_string(),
                )
            })?;
        for records in page_posting_records_both(&tree, &decoded) {
            std::hint::black_box(records?);
        }
    }
    let scores_per_row = u64::try_from(tree.shape.depth)
        .ok()
        .and_then(|depth| depth.checked_mul(5))
        .and_then(|scores| scores.checked_sub(2))
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "V23 incidence posting preflight tree depth differs".to_string(),
            )
        })?;
    let distance_dimensions = physical_rows
        .checked_mul(scores_per_row)
        .and_then(|value| value.checked_mul(96))
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "V23 incidence posting preflight work overflows".to_string(),
            )
        })?;
    let mut probe = [0.0_f32; 96];
    probe[0] = 1.0;
    let (_, fma_backend) = split_score_simd(
        tree.nodes.first().ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence posting preflight tree is empty".to_string())
        })?,
        &probe,
    )?;
    Ok(V23IncidenceKernelMeasurement {
        distance_dimensions,
        distance_elapsed_ns: u64::try_from(started.elapsed().as_nanos())
            .unwrap_or(u64::MAX)
            .max(1),
        fma_backend,
    })
}

fn read_v23_incidence_training_preflight_rows(path: &Path, count: usize) -> Result<Vec<[f32; 96]>> {
    if count == 0 {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence training preflight count differs".to_string(),
        ));
    }
    let file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let child = Arc::new(Field::new("element", DataType::Float32, false));
    let expected = Schema::new(vec![Field::new(
        "emb",
        DataType::FixedSizeList(child, 96),
        false,
    )]);
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    if builder.schema().as_ref() != &expected
        || builder.metadata().file_metadata().num_rows() < count as i64
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence training Parquet schema differs".to_string(),
        ));
    }
    let mut rows = Vec::with_capacity(count);
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 1 || batch.column(0).null_count() != 0 {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence training Parquet batch differs".to_string(),
            ));
        }
        let vectors = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "V23 incidence training vector column differs".to_string(),
                )
            })?;
        let values = vectors
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "V23 incidence training vector values differ".to_string(),
                )
            })?;
        if values.null_count() != 0 {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence training vector nullability differs".to_string(),
            ));
        }
        for index in 0..vectors.len() {
            if rows.len() == count {
                break;
            }
            let start = index * 96;
            let values = &values.values()[start..start + 96];
            let squared_norm = values.iter().try_fold(0.0_f64, |sum, value| {
                value
                    .is_finite()
                    .then_some(sum + f64::from(*value) * f64::from(*value))
            });
            if squared_norm.is_none_or(|norm| !norm.is_finite() || norm == 0.0) {
                return Err(BorsukError::InvalidStorage(
                    "V23 incidence training vector differs".to_string(),
                ));
            }
            rows.push(values.try_into().unwrap());
        }
        if rows.len() == count {
            break;
        }
    }
    if rows.len() != count {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence training preflight rows are truncated".to_string(),
        ));
    }
    Ok(rows)
}

struct V23IncidenceTrainingRowStream {
    shards: std::vec::IntoIter<(PathBuf, u64, u64)>,
    current: Option<(ParquetRecordBatchReader, PathBuf, u64, u64)>,
    pending: std::vec::IntoIter<V23TrainingRow>,
    failed: bool,
}

impl V23IncidenceTrainingRowStream {
    fn fail(&mut self, error: BorsukError) -> Option<Result<V23TrainingRow>> {
        self.failed = true;
        Some(Err(error))
    }

    fn open_next_shard(&mut self, path: PathBuf, start: u64, end: u64) -> Result<()> {
        let file = File::open(&path).map_err(|source| BorsukError::Io {
            path: path.clone(),
            source,
        })?;
        let child = Arc::new(Field::new("element", DataType::Float32, false));
        let expected = Schema::new(vec![Field::new(
            "emb",
            DataType::FixedSizeList(child, 96),
            false,
        )]);
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        if builder.schema().as_ref() != &expected
            || builder.metadata().file_metadata().num_rows()
                != i64::try_from(end - start).unwrap_or(-1)
        {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence training shard schema differs".to_string(),
            ));
        }
        self.current = Some((builder.build()?, path, start, end));
        Ok(())
    }
}

impl Iterator for V23IncidenceTrainingRowStream {
    type Item = Result<V23TrainingRow>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.failed {
                return None;
            }
            if let Some(row) = self.pending.next() {
                return Some(Ok(row));
            }
            if let Some((reader, _, next_ordinal, end)) = self.current.as_mut() {
                match reader.next() {
                    Some(Err(error)) => return self.fail(error.into()),
                    Some(Ok(batch)) => {
                        if batch.num_columns() != 1 || batch.column(0).null_count() != 0 {
                            return self.fail(BorsukError::InvalidStorage(
                                "V23 incidence training shard batch differs".to_string(),
                            ));
                        }
                        let Some(vectors) = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<FixedSizeListArray>()
                        else {
                            return self.fail(BorsukError::InvalidStorage(
                                "V23 incidence training shard vectors differ".to_string(),
                            ));
                        };
                        let Some(values) = vectors.values().as_any().downcast_ref::<Float32Array>()
                        else {
                            return self.fail(BorsukError::InvalidStorage(
                                "V23 incidence training shard values differ".to_string(),
                            ));
                        };
                        if values.null_count() != 0 {
                            return self.fail(BorsukError::InvalidStorage(
                                "V23 incidence training shard nullability differs".to_string(),
                            ));
                        }
                        let mut rows = Vec::with_capacity(vectors.len());
                        for index in 0..vectors.len() {
                            if *next_ordinal >= *end {
                                return self.fail(BorsukError::InvalidStorage(
                                    "V23 incidence training shard row count differs".to_string(),
                                ));
                            }
                            let start = index * 96;
                            let row = &values.values()[start..start + 96];
                            let squared_norm = row.iter().try_fold(0.0_f64, |sum, value| {
                                value
                                    .is_finite()
                                    .then_some(sum + f64::from(*value) * f64::from(*value))
                            });
                            if squared_norm.is_none_or(|norm| !norm.is_finite() || norm == 0.0) {
                                return self.fail(BorsukError::InvalidStorage(
                                    "V23 incidence training shard vector differs".to_string(),
                                ));
                            }
                            rows.push(V23TrainingRow {
                                source_ordinal: *next_ordinal,
                                vector: row.try_into().unwrap(),
                            });
                            *next_ordinal += 1;
                        }
                        self.pending = rows.into_iter();
                    }
                    None => {
                        if *next_ordinal != *end {
                            return self.fail(BorsukError::InvalidStorage(
                                "V23 incidence training shard is truncated".to_string(),
                            ));
                        }
                        self.current = None;
                    }
                }
                continue;
            }
            let (path, start, end) = self.shards.next()?;
            if start >= end {
                return self.fail(BorsukError::InvalidStorage(
                    "V23 incidence training shard ordinal range differs".to_string(),
                ));
            }
            if let Err(error) = self.open_next_shard(path, start, end) {
                return self.fail(error);
            }
        }
    }
}

fn v23_incidence_training_row_stream(
    shards: Vec<(PathBuf, u64, u64)>,
) -> Result<V23IncidenceTrainingRowStream> {
    if shards.is_empty() || shards.windows(2).any(|pair| pair[0].2 != pair[1].1) {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence training shard sequence differs".to_string(),
        ));
    }
    Ok(V23IncidenceTrainingRowStream {
        shards: shards.into_iter(),
        current: None,
        pending: Vec::new().into_iter(),
        failed: false,
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct V23IncidenceSandboxProbeEnvelope {
    allowlisted_inputs_opened: bool,
    host_canary_denied: bool,
    network_canary_denied: bool,
    network_namespace_changed: bool,
    network_namespace_inode: u64,
    output_writable: bool,
}

fn parse_v23_incidence_sandbox_probes(raw: &str) -> Result<(V23IncidenceCapabilityProbes, u64)> {
    let envelope: V23IncidenceSandboxProbeEnvelope =
        serde_json::from_str(raw).map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "V23 incidence sandbox probe JSON differs: {error}"
            ))
        })?;
    let probes = V23IncidenceCapabilityProbes {
        network_namespace_changed: envelope.network_namespace_changed,
        host_canary_denied: envelope.host_canary_denied,
        network_canary_denied: envelope.network_canary_denied,
        allowlisted_inputs_opened: envelope.allowlisted_inputs_opened,
        output_writable: envelope.output_writable,
    };
    if envelope.network_namespace_inode == 0 || !probes.all_passed() {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence sandbox probes differ".to_string(),
        ));
    }
    Ok((probes, envelope.network_namespace_inode))
}

fn validate_v23_incidence_execution_namespace(
    _preflight_network_namespace_inode: u64,
    execution_network_namespace_inode: u64,
) -> Result<()> {
    if execution_network_namespace_inode == 0 {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence execution network namespace differs".to_string(),
        ));
    }
    Ok(())
}

fn authenticate_v23_incidence_local_path(
    path: &Path,
    identity: &V23IncidenceObjectIdentity,
) -> Result<V23IncidenceInputMeasurement> {
    validate_object_identity(identity)?;
    let metadata = path.symlink_metadata().map_err(|source| BorsukError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if !metadata.file_type().is_file() || metadata.len() != identity.encoded_bytes {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence local object shape differs".to_string(),
        ));
    }
    let mut file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let started = Instant::now();
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut read_bytes = 0_u64;
    let digest = match identity.digest_algorithm.as_str() {
        "sha256" => {
            let mut hasher = Sha256::new();
            loop {
                let read = file.read(&mut buffer).map_err(|source| BorsukError::Io {
                    path: path.to_path_buf(),
                    source,
                })?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
                read_bytes = read_bytes.checked_add(read as u64).ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V23 incidence local object length overflows".to_string(),
                    )
                })?;
            }
            format!("{:x}", hasher.finalize())
        }
        "blake3" => {
            let mut hasher = blake3::Hasher::new();
            loop {
                let read = file.read(&mut buffer).map_err(|source| BorsukError::Io {
                    path: path.to_path_buf(),
                    source,
                })?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
                read_bytes = read_bytes.checked_add(read as u64).ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V23 incidence local object length overflows".to_string(),
                    )
                })?;
            }
            hasher.finalize().to_hex().to_string()
        }
        _ => {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence local digest algorithm differs".to_string(),
            ));
        }
    };
    if read_bytes != identity.encoded_bytes || digest != identity.digest {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence local object bytes differ".to_string(),
        ));
    }
    Ok(V23IncidenceInputMeasurement {
        input_bytes: read_bytes,
        input_elapsed_ns: u64::try_from(started.elapsed().as_nanos())
            .unwrap_or(u64::MAX)
            .max(1),
    })
}

fn write_v23_incidence_local_output(
    role: &str,
    digest_algorithm: &str,
    bytes: &[u8],
    scratch: &Path,
    receipt_path: &Path,
) -> Result<(V23IncidenceObjectIdentity, PathBuf)> {
    if bytes.is_empty()
        || !valid_role_algorithm(role, digest_algorithm)
        || !scratch.is_dir()
        || scratch
            .read_dir()
            .map_err(|source| BorsukError::Io {
                path: scratch.to_path_buf(),
                source,
            })?
            .next()
            .is_some()
        || receipt_path.exists()
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence local output boundary differs".to_string(),
        ));
    }
    let output_directory = receipt_path
        .parent()
        .filter(|path| path.is_dir())
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence local output directory differs".to_string())
        })?;
    let digest = match digest_algorithm {
        "sha256" => format!("{:x}", Sha256::digest(bytes)),
        "blake3" => blake3::hash(bytes).to_hex().to_string(),
        _ => unreachable!(),
    };
    let file_name = format!("{role}-{digest}.bin");
    let temporary = scratch.join(format!(".{file_name}.tmp"));
    let output = output_directory.join(&file_name);
    if temporary.exists() || output.exists() {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence local output already exists".to_string(),
        ));
    }
    let result: Result<()> = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|source| BorsukError::Io {
                path: temporary.clone(),
                source,
            })?;
        file.write_all(bytes).map_err(|source| BorsukError::Io {
            path: temporary.clone(),
            source,
        })?;
        file.sync_all().map_err(|source| BorsukError::Io {
            path: temporary.clone(),
            source,
        })?;
        fs::rename(&temporary, &output).map_err(|source| BorsukError::Io {
            path: output.clone(),
            source,
        })?;
        Ok(())
    })();
    if result.is_err() && temporary.exists() {
        let _ = fs::remove_file(&temporary);
    }
    result?;
    let identity = V23IncidenceObjectIdentity {
        role: role.to_string(),
        uri: format!("file://{}", output.display()),
        digest_algorithm: digest_algorithm.to_string(),
        digest: digest.clone(),
        encoded_bytes: bytes.len() as u64,
        generation: format!("content-{digest}"),
    };
    validate_object_identity(&identity)?;
    Ok((identity, output))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23IncidencePreflightAuthority {
    pub(crate) parent_receipt_sha256: Option<String>,
    pub(crate) executable_sha256: String,
    pub(crate) fma_backend: V23FmaBackend,
    pub(crate) network_namespace_inode: u64,
    pub(crate) probes: V23IncidenceCapabilityProbes,
    pub(crate) full_input_bytes: u64,
    pub(crate) ordered_inputs: Vec<V23IncidenceObjectIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidencePreflightEvidence {
    pub(crate) phase: V23IncidencePhase,
    pub(crate) sample_vectors: u64,
    pub(crate) sample_page_bodies: u64,
    pub(crate) sample_queries: u64,
    pub(crate) sample_records: u64,
    pub(crate) full_distance_dimensions: u64,
    pub(crate) full_input_bytes: u64,
    pub(crate) full_records: u64,
    pub(crate) record_kind: V23IncidencePreflightRecordKind,
    pub(crate) measurement: V23IncidencePreflightMeasurement,
    pub(crate) distance_dimensions_per_second: u64,
    pub(crate) input_bytes_per_second: u64,
    pub(crate) records_per_second: u64,
    pub(crate) projected_wall_ns: u64,
    pub(crate) wall_limit_ns: u64,
    pub(crate) resource_stop: bool,
}

pub(crate) const fn v23_incidence_preflight_work(
    phase: V23IncidencePhase,
) -> V23IncidencePreflightWork {
    match phase {
        V23IncidencePhase::TreeTraining => V23IncidencePreflightWork {
            phase,
            sample_vectors: 65_536,
            sample_page_bodies: 0,
            sample_queries: 0,
            sample_records: 0,
            full_distance_dimensions: 35_433_480_192,
            full_records: 0,
            record_kind: V23IncidencePreflightRecordKind::None,
        },
        V23IncidencePhase::PostingConstruction => V23IncidencePreflightWork {
            phase,
            sample_vectors: 0,
            sample_page_bodies: 256,
            sample_queries: 0,
            sample_records: 1_048_576,
            full_distance_dimensions: 168_027_881_664,
            full_records: 55_860_333,
            record_kind: V23IncidencePreflightRecordKind::ExternalSort,
        },
        V23IncidencePhase::DevelopmentEvaluation => V23IncidencePreflightWork {
            phase,
            sample_vectors: 0,
            sample_page_bodies: 0,
            sample_queries: 10_000,
            sample_records: 2_621_440_000,
            full_distance_dimensions: 1_252_050_075_648,
            full_records: 52_168_753_152,
            record_kind: V23IncidencePreflightRecordKind::PostingVisits,
        },
        V23IncidencePhase::HoldoutEvaluation => V23IncidencePreflightWork {
            phase,
            sample_vectors: 0,
            sample_page_bodies: 0,
            sample_queries: 10_000,
            sample_records: 2_621_440_000,
            full_distance_dimensions: 70_162_317_312,
            full_records: 2_923_429_888,
            record_kind: V23IncidencePreflightRecordKind::PostingVisits,
        },
        V23IncidencePhase::HoldoutBinding => V23IncidencePreflightWork {
            phase,
            sample_vectors: 0,
            sample_page_bodies: 256,
            sample_queries: 0,
            sample_records: 0,
            full_distance_dimensions: 0,
            full_records: 0,
            record_kind: V23IncidencePreflightRecordKind::None,
        },
    }
}

fn measured_rate(units: u64, elapsed_ns: u64, required: bool) -> Result<u64> {
    if !required {
        if units != 0 || elapsed_ns != 0 {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence unused preflight measurement differs".to_string(),
            ));
        }
        return Ok(0);
    }
    if units == 0 || elapsed_ns == 0 {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence preflight measurement is empty".to_string(),
        ));
    }
    let rate = u128::from(units)
        .checked_mul(1_000_000_000)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence preflight rate overflows".to_string())
        })?
        / u128::from(elapsed_ns);
    if rate == 0 || rate > u128::from(u64::MAX) {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence preflight rate differs".to_string(),
        ));
    }
    Ok(rate as u64)
}

fn projected_component_ns(full_units: u64, rate_per_second: u64) -> Result<u64> {
    if full_units == 0 {
        if rate_per_second != 0 {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence unused preflight rate differs".to_string(),
            ));
        }
        return Ok(0);
    }
    if rate_per_second == 0 {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence preflight rate is zero".to_string(),
        ));
    }
    let numerator = u128::from(full_units)
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_mul(5))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence preflight projection overflows".to_string())
        })?;
    let denominator = u128::from(rate_per_second) * 4;
    let projected = numerator.div_ceil(denominator);
    u64::try_from(projected).map_err(|_| {
        BorsukError::InvalidStorage("V23 incidence preflight projection exceeds u64".to_string())
    })
}

pub(crate) fn project_v23_incidence_preflight(
    work: V23IncidencePreflightWork,
    authority: V23IncidencePreflightAuthority,
    measurement: V23IncidencePreflightMeasurement,
) -> Result<V23IncidencePreflightEvidence> {
    let parent_is_valid = match work.phase {
        V23IncidencePhase::TreeTraining => authority.parent_receipt_sha256.is_none(),
        _ => authority
            .parent_receipt_sha256
            .as_deref()
            .is_some_and(|digest| valid_lower_hex(digest, 64)),
    };
    let roles = authority
        .ordered_inputs
        .iter()
        .map(|identity| identity.role.as_str())
        .collect::<BTreeSet<_>>();
    let parent_input = authority
        .ordered_inputs
        .iter()
        .find(|identity| identity.role == "parent-receipt");
    let mounted_input_bytes = authority
        .ordered_inputs
        .iter()
        .try_fold(0_u64, |sum, identity| {
            sum.checked_add(identity.encoded_bytes)
        })
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence mounted input bytes overflow".to_string())
        })?;
    let full_input_bytes = authority.full_input_bytes;
    let distance_measurement_is_valid = match work.phase {
        V23IncidencePhase::TreeTraining => {
            measurement.distance_dimensions == work.sample_vectors * 2 * 96
        }
        V23IncidencePhase::PostingConstruction => measurement.distance_dimensions != 0,
        V23IncidencePhase::DevelopmentEvaluation | V23IncidencePhase::HoldoutEvaluation => {
            measurement.distance_dimensions == work.sample_queries * 65_536 * 96
        }
        V23IncidencePhase::HoldoutBinding => measurement.distance_dimensions == 0,
    };
    let record_measurement_is_valid = match work.phase {
        V23IncidencePhase::PostingConstruction => measurement.records == work.sample_records,
        V23IncidencePhase::DevelopmentEvaluation | V23IncidencePhase::HoldoutEvaluation => {
            (1..=work.sample_records).contains(&measurement.records)
        }
        V23IncidencePhase::TreeTraining | V23IncidencePhase::HoldoutBinding => {
            measurement.records == 0
        }
    };
    if work != v23_incidence_preflight_work(work.phase)
        || full_input_bytes < mounted_input_bytes
        || measurement.input_bytes != mounted_input_bytes
        || !distance_measurement_is_valid
        || !record_measurement_is_valid
        || !parent_is_valid
        || !valid_lower_hex(&authority.executable_sha256, 64)
        || authority.fma_backend == V23FmaBackend::ScalarControl
        || authority.network_namespace_inode == 0
        || !authority.probes.all_passed()
        || !phase_preflight_roles_are_complete(work.phase, &roles)
        || authority
            .ordered_inputs
            .iter()
            .any(|identity| !phase_preflight_role_is_allowed(work.phase, &identity.role))
        || validate_identity_list(&authority.ordered_inputs).is_err()
        || match (authority.parent_receipt_sha256.as_deref(), parent_input) {
            (None, None) => false,
            (Some(parent), Some(identity)) => {
                identity.digest_algorithm != "sha256" || identity.digest != parent
            }
            _ => true,
        }
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence preflight work authority differs".to_string(),
        ));
    }
    let distance_dimensions_per_second = measured_rate(
        measurement.distance_dimensions,
        measurement.distance_elapsed_ns,
        work.full_distance_dimensions != 0,
    )?;
    let input_bytes_per_second =
        measured_rate(measurement.input_bytes, measurement.input_elapsed_ns, true)?;
    let records_per_second = measured_rate(
        measurement.records,
        measurement.records_elapsed_ns,
        work.full_records != 0,
    )?;
    let projected_distance_ns = projected_component_ns(
        work.full_distance_dimensions,
        distance_dimensions_per_second,
    )?;
    let projected_input_ns = projected_component_ns(full_input_bytes, input_bytes_per_second)?;
    let projected_records_ns = projected_component_ns(work.full_records, records_per_second)?;
    let projected_wall_ns = projected_distance_ns
        .checked_add(projected_input_ns)
        .and_then(|value| value.checked_add(projected_records_ns))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence preflight projection overflows".to_string())
        })?;
    Ok(V23IncidencePreflightEvidence {
        phase: work.phase,
        sample_vectors: work.sample_vectors,
        sample_page_bodies: work.sample_page_bodies,
        sample_queries: work.sample_queries,
        sample_records: work.sample_records,
        full_distance_dimensions: work.full_distance_dimensions,
        full_input_bytes,
        full_records: work.full_records,
        record_kind: work.record_kind,
        measurement,
        distance_dimensions_per_second,
        input_bytes_per_second,
        records_per_second,
        projected_wall_ns,
        wall_limit_ns: V23_INCIDENCE_PREFLIGHT_WALL_LIMIT_NS,
        resource_stop: projected_wall_ns > V23_INCIDENCE_PREFLIGHT_WALL_LIMIT_NS,
    })
}

pub(crate) fn canonical_v23_incidence_preflight_bytes(
    evidence: &V23IncidencePreflightEvidence,
    expected_authority: &V23IncidencePreflightAuthority,
    parent_receipt_bytes: Option<&[u8]>,
) -> Result<Vec<u8>> {
    let recomputed = project_v23_incidence_preflight(
        v23_incidence_preflight_work(evidence.phase),
        expected_authority.clone(),
        evidence.measurement,
    )?;
    if *evidence != recomputed {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence preflight evidence differs".to_string(),
        ));
    }
    let receipt = V23IncidenceReceipt {
        schema: V23_INCIDENCE_RECEIPT_SCHEMA.to_string(),
        claim_eligible: false,
        phase: evidence.phase,
        run_mode: V23IncidenceReceiptRunMode::Preflight,
        parent_receipt_sha256: expected_authority.parent_receipt_sha256.clone(),
        executable_sha256: expected_authority.executable_sha256.clone(),
        fma_backend: expected_authority.fma_backend,
        network_namespace_inode: expected_authority.network_namespace_inode,
        ordered_mounts: expected_authority.ordered_inputs.clone(),
        probes: expected_authority.probes.clone(),
        preflight_evidence: Some(evidence.clone()),
        final_progress_sha256: None,
        outputs: Vec::new(),
        stop: evidence
            .resource_stop
            .then_some(V23IncidenceStopClass::Resource),
    };
    canonical_v23_incidence_receipt_bytes(&receipt, parent_receipt_bytes, &[])
}

fn read_v23_incidence_preflight_receipt(
    bytes: &[u8],
    expected_authority: &V23IncidencePreflightAuthority,
    parent_receipt_bytes: Option<&[u8]>,
) -> Result<V23IncidenceReceipt> {
    let receipt: V23IncidenceReceipt = serde_json::from_slice(bytes).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 incidence preflight receipt JSON differs: {error}"
        ))
    })?;
    let evidence = receipt.preflight_evidence.as_ref().ok_or_else(|| {
        BorsukError::InvalidStorage("V23 incidence preflight evidence is absent".to_string())
    })?;
    let canonical = canonical_v23_incidence_preflight_bytes(
        evidence,
        expected_authority,
        parent_receipt_bytes,
    )?;
    if receipt.run_mode != V23IncidenceReceiptRunMode::Preflight || canonical != bytes {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence preflight receipt bytes differ".to_string(),
        ));
    }
    Ok(receipt)
}

fn validate_v23_incidence_execution_preflight(
    bytes: &[u8],
    identity: &V23IncidenceObjectIdentity,
    expected_authority: &V23IncidencePreflightAuthority,
    parent_receipt_bytes: Option<&[u8]>,
) -> Result<V23IncidenceReceipt> {
    if identity.role != "preflight-receipt" || identity.digest_algorithm != "sha256" {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence execution preflight identity differs".to_string(),
        ));
    }
    validate_object_bytes(identity, bytes)?;
    let receipt =
        read_v23_incidence_preflight_receipt(bytes, expected_authority, parent_receipt_bytes)?;
    if receipt.stop.is_some()
        || receipt
            .preflight_evidence
            .as_ref()
            .is_none_or(|evidence| evidence.resource_stop)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence execution preflight did not pass".to_string(),
        ));
    }
    Ok(receipt)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidenceManifest {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) phase: V23IncidencePhase,
    pub(crate) parent_receipt_sha256: Option<String>,
    pub(crate) source_commit: String,
    pub(crate) source_archive_sha256: String,
    pub(crate) index_id: String,
    pub(crate) dataset_id: String,
    pub(crate) algorithm: V23IncidenceAlgorithm,
    pub(crate) ordered_inputs: Vec<V23IncidenceInputAuthority>,
}

impl V23IncidenceCapabilityProbes {
    fn all_passed(&self) -> bool {
        self.network_namespace_changed
            && self.host_canary_denied
            && self.network_canary_denied
            && self.allowlisted_inputs_opened
            && self.output_writable
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidenceReceipt {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) phase: V23IncidencePhase,
    pub(crate) run_mode: V23IncidenceReceiptRunMode,
    pub(crate) parent_receipt_sha256: Option<String>,
    pub(crate) executable_sha256: String,
    pub(crate) fma_backend: V23FmaBackend,
    pub(crate) network_namespace_inode: u64,
    pub(crate) ordered_mounts: Vec<V23IncidenceObjectIdentity>,
    pub(crate) probes: V23IncidenceCapabilityProbes,
    pub(crate) preflight_evidence: Option<V23IncidencePreflightEvidence>,
    pub(crate) final_progress_sha256: Option<String>,
    pub(crate) outputs: Vec<V23IncidenceObjectIdentity>,
    pub(crate) stop: Option<V23IncidenceStopClass>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct V23IncidenceProgress {
    completed_units: u64,
    last_object_digest: String,
    phase: V23IncidencePhase,
    previous_progress_sha256: Option<String>,
    sequence: u64,
    total_units: u64,
}

struct V23IncidenceProgressChain {
    path: PathBuf,
    phase: V23IncidencePhase,
    total_units: u64,
    sequence: u64,
    previous_record_bytes: Vec<u8>,
    history_bytes: Vec<u8>,
    #[cfg(test)]
    records: Vec<Vec<u8>>,
}

impl V23IncidenceProgressChain {
    fn start(
        path: &Path,
        phase: V23IncidencePhase,
        total_units: u64,
        initial_object_digest: &str,
    ) -> Result<Self> {
        match fs::symlink_metadata(path) {
            Ok(_) => {
                return Err(BorsukError::InvalidStorage(
                    "V23 incidence progress already exists".to_string(),
                ));
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(BorsukError::Io {
                    path: path.to_path_buf(),
                    source,
                });
            }
        }
        let root = V23IncidenceProgress {
            completed_units: 0,
            last_object_digest: initial_object_digest.to_string(),
            phase,
            previous_progress_sha256: None,
            sequence: 0,
            total_units,
        };
        let root_bytes = canonical_v23_incidence_progress_bytes(&root, None)?;
        write_v23_incidence_progress_snapshot(path, &root_bytes)?;
        Ok(Self {
            path: path.to_path_buf(),
            phase,
            total_units,
            sequence: 0,
            #[cfg(test)]
            records: vec![root_bytes.clone()],
            previous_record_bytes: root_bytes.clone(),
            history_bytes: root_bytes,
        })
    }

    fn advance(&mut self, completed_units: u64, last_object_digest: &str) -> Result<String> {
        let previous_progress_sha256 = format!("{:x}", Sha256::digest(&self.previous_record_bytes));
        let sequence = self.sequence.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence progress sequence overflows".to_string())
        })?;
        let progress = V23IncidenceProgress {
            completed_units,
            last_object_digest: last_object_digest.to_string(),
            phase: self.phase,
            previous_progress_sha256: Some(previous_progress_sha256),
            sequence,
            total_units: self.total_units,
        };
        let record_bytes = canonical_v23_incidence_progress_bytes(
            &progress,
            Some(self.previous_record_bytes.as_slice()),
        )?;
        let mut history_bytes = self.history_bytes.clone();
        history_bytes.extend_from_slice(&record_bytes);
        write_v23_incidence_progress_snapshot(&self.path, &history_bytes)?;
        let digest = format!("{:x}", Sha256::digest(&history_bytes));
        self.sequence = sequence;
        self.previous_record_bytes = record_bytes.clone();
        self.history_bytes = history_bytes;
        #[cfg(test)]
        self.records.push(record_bytes);
        Ok(digest)
    }

    #[cfg(test)]
    fn records(&self) -> &[Vec<u8>] {
        &self.records
    }
}

fn valid_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_role_algorithm(role: &str, algorithm: &str) -> bool {
    let sha256_role = matches!(
        role,
        "construction-manifest"
            | "bulk-manifest"
            | "phase-manifest"
            | "page-roster"
            | "query-parquet"
            | "neighbors-parquet"
            | "dataset-meta"
            | "d2-report"
            | "parent-receipt"
            | "preflight-receipt"
            | "holdout-truth"
            | "development-result"
            | "campaign-result"
            | "executable"
            | "staging-receipt"
    ) || role.starts_with("training-shard-");
    let blake3_role = matches!(
        role,
        "incidence-tree"
            | "incidence-postings-one"
            | "incidence-postings-two"
            | "development-latency"
            | "holdout-latency"
    ) || role.starts_with("page-body-");
    (sha256_role && algorithm == "sha256") || (blake3_role && algorithm == "blake3")
}

fn validate_object_identity(identity: &V23IncidenceObjectIdentity) -> Result<()> {
    if identity.role.is_empty()
        || identity.uri.is_empty()
        || identity.generation.is_empty()
        || identity.encoded_bytes == 0
        || !valid_role_algorithm(&identity.role, &identity.digest_algorithm)
        || !valid_lower_hex(&identity.digest, 64)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence object identity differs".to_string(),
        ));
    }
    Ok(())
}

impl V23IncidenceRunMode {
    fn phase(self) -> V23IncidencePhase {
        match self {
            Self::Preflight(phase) | Self::Execute(phase) => phase,
        }
    }

    fn is_execute(self) -> bool {
        matches!(self, Self::Execute(_))
    }
}

fn phase_role_is_allowed(phase: V23IncidencePhase, role: &str) -> bool {
    match phase {
        V23IncidencePhase::TreeTraining => {
            matches!(role, "construction-manifest" | "dataset-meta")
                || role.starts_with("training-shard-")
        }
        V23IncidencePhase::PostingConstruction => {
            matches!(
                role,
                "phase-manifest" | "parent-receipt" | "incidence-tree" | "page-roster"
            ) || role.starts_with("page-body-")
        }
        V23IncidencePhase::DevelopmentEvaluation => matches!(
            role,
            "phase-manifest"
                | "parent-receipt"
                | "incidence-tree"
                | "incidence-postings-one"
                | "incidence-postings-two"
                | "d2-report"
                | "query-parquet"
        ),
        V23IncidencePhase::HoldoutBinding => {
            matches!(
                role,
                "phase-manifest"
                    | "parent-receipt"
                    | "development-result"
                    | "page-roster"
                    | "neighbors-parquet"
            ) || role.starts_with("page-body-")
        }
        V23IncidencePhase::HoldoutEvaluation => matches!(
            role,
            "phase-manifest"
                | "parent-receipt"
                | "development-result"
                | "development-latency"
                | "incidence-tree"
                | "incidence-postings-one"
                | "incidence-postings-two"
                | "query-parquet"
                | "holdout-truth"
        ),
    }
}

fn phase_roles_are_complete(phase: V23IncidencePhase, roles: &BTreeSet<&str>) -> bool {
    match phase {
        V23IncidencePhase::TreeTraining => {
            roles.contains("construction-manifest")
                && roles.contains("dataset-meta")
                && roles.iter().any(|role| role.starts_with("training-shard-"))
        }
        V23IncidencePhase::PostingConstruction => {
            [
                "phase-manifest",
                "parent-receipt",
                "incidence-tree",
                "page-roster",
            ]
            .into_iter()
            .all(|role| roles.contains(role))
                && roles.iter().any(|role| role.starts_with("page-body-"))
        }
        V23IncidencePhase::DevelopmentEvaluation => [
            "phase-manifest",
            "parent-receipt",
            "incidence-tree",
            "incidence-postings-one",
            "incidence-postings-two",
            "d2-report",
            "query-parquet",
        ]
        .into_iter()
        .all(|role| roles.contains(role)),
        V23IncidencePhase::HoldoutBinding => {
            [
                "phase-manifest",
                "parent-receipt",
                "development-result",
                "page-roster",
                "neighbors-parquet",
            ]
            .into_iter()
            .all(|role| roles.contains(role))
                && roles.iter().any(|role| role.starts_with("page-body-"))
        }
        V23IncidencePhase::HoldoutEvaluation => [
            "phase-manifest",
            "parent-receipt",
            "development-result",
            "development-latency",
            "incidence-tree",
            "incidence-postings-one",
            "incidence-postings-two",
            "query-parquet",
            "holdout-truth",
        ]
        .into_iter()
        .all(|role| roles.contains(role)),
    }
}

fn phase_preflight_role_is_allowed(phase: V23IncidencePhase, role: &str) -> bool {
    match phase {
        V23IncidencePhase::TreeTraining => {
            role == "construction-manifest" || role.starts_with("training-shard-")
        }
        V23IncidencePhase::PostingConstruction => {
            matches!(
                role,
                "phase-manifest" | "parent-receipt" | "incidence-tree" | "page-roster"
            ) || role.starts_with("page-body-")
        }
        V23IncidencePhase::DevelopmentEvaluation | V23IncidencePhase::HoldoutEvaluation => {
            matches!(
                role,
                "phase-manifest"
                    | "parent-receipt"
                    | "incidence-tree"
                    | "incidence-postings-one"
                    | "incidence-postings-two"
            )
        }
        V23IncidencePhase::HoldoutBinding => {
            matches!(role, "phase-manifest" | "parent-receipt" | "page-roster")
                || role.starts_with("page-body-")
        }
    }
}

fn phase_preflight_roles_are_complete(phase: V23IncidencePhase, roles: &BTreeSet<&str>) -> bool {
    match phase {
        V23IncidencePhase::TreeTraining => {
            roles.contains("construction-manifest")
                && roles.iter().any(|role| role.starts_with("training-shard-"))
        }
        V23IncidencePhase::PostingConstruction => {
            [
                "phase-manifest",
                "parent-receipt",
                "incidence-tree",
                "page-roster",
            ]
            .into_iter()
            .all(|role| roles.contains(role))
                && roles.iter().any(|role| role.starts_with("page-body-"))
        }
        V23IncidencePhase::DevelopmentEvaluation | V23IncidencePhase::HoldoutEvaluation => [
            "phase-manifest",
            "parent-receipt",
            "incidence-tree",
            "incidence-postings-one",
            "incidence-postings-two",
        ]
        .into_iter()
        .all(|role| roles.contains(role)),
        V23IncidencePhase::HoldoutBinding => {
            ["phase-manifest", "parent-receipt", "page-roster"]
                .into_iter()
                .all(|role| roles.contains(role))
                && roles.iter().any(|role| role.starts_with("page-body-"))
        }
    }
}

impl V23IncidenceLocalPhaseRequest {
    /// Validates the local request shape and phase capability allowlist.
    pub fn validate(&self) -> Result<()> {
        let phase = self.mode.phase();
        if !self.manifest_path.is_absolute()
            || !self.scratch_path.is_absolute()
            || !self.output_path.is_absolute()
            || self.scratch_path == self.output_path
            || self.scratch_path.starts_with(&self.output_path)
            || self.output_path.starts_with(&self.scratch_path)
            || !valid_lower_hex(&self.executable_sha256, 64)
            || self.input_paths.is_empty()
        {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence local request shape differs".to_string(),
            ));
        }
        let later_phase = phase != V23IncidencePhase::TreeTraining;
        if later_phase != self.parent_receipt_path.is_some()
            || self.mode.is_execute() != self.preflight_receipt_path.is_some()
            || self
                .parent_receipt_path
                .iter()
                .chain(self.preflight_receipt_path.iter())
                .any(|path| !path.is_absolute())
        {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence local receipt path differs".to_string(),
            ));
        }

        let mut roles = BTreeSet::new();
        let mut paths = BTreeSet::new();
        let identities = self
            .input_paths
            .iter()
            .map(|input| input.identity.clone())
            .collect::<Vec<_>>();
        validate_identity_list(&identities)?;
        for input in &self.input_paths {
            let handoff_role = matches!(
                input.identity.role.as_str(),
                "bulk-manifest" | "staging-receipt"
            );
            let role_allowed = if self.mode.is_execute() {
                phase_role_is_allowed(phase, &input.identity.role)
                    || input.identity.role == "preflight-receipt"
                    || handoff_role
            } else {
                phase_preflight_role_is_allowed(phase, &input.identity.role) || handoff_role
            };
            if !input.path.is_absolute() || !paths.insert(input.path.as_path()) || !role_allowed {
                return Err(BorsukError::InvalidStorage(
                    "V23 incidence local input capability differs".to_string(),
                ));
            }
            roles.insert(input.identity.role.as_str());
        }
        let roles_are_complete = if self.mode.is_execute() {
            phase_roles_are_complete(phase, &roles) && roles.contains("preflight-receipt")
        } else {
            phase_preflight_roles_are_complete(phase, &roles)
        };
        if !roles_are_complete
            || self
                .input_paths
                .iter()
                .find(|input| {
                    input.identity.role
                        == if phase == V23IncidencePhase::TreeTraining {
                            "construction-manifest"
                        } else {
                            "phase-manifest"
                        }
                })
                .map(|input| input.path.as_path())
                != Some(self.manifest_path.as_path())
            || self.parent_receipt_path.as_deref()
                != self
                    .input_paths
                    .iter()
                    .find(|input| input.identity.role == "parent-receipt")
                    .map(|input| input.path.as_path())
            || self.preflight_receipt_path.as_deref()
                != self
                    .input_paths
                    .iter()
                    .find(|input| input.identity.role == "preflight-receipt")
                    .map(|input| input.path.as_path())
        {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence local role binding differs".to_string(),
            ));
        }
        Ok(())
    }
}

fn preflight_registered_inputs(
    manifest: &V23IncidenceManifest,
) -> Result<Vec<V23IncidenceObjectIdentity>> {
    let identities = manifest
        .ordered_inputs
        .iter()
        .map(|input| input.identity().clone())
        .collect::<Vec<_>>();
    let selected = match manifest.phase {
        V23IncidencePhase::TreeTraining => {
            if identities.len() < 2 {
                return Err(BorsukError::InvalidStorage(
                    "V23 incidence tree preflight inputs differ".to_string(),
                ));
            }
            vec![identities[1].clone()]
        }
        V23IncidencePhase::PostingConstruction => identities
            .get(..3 + 256)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "V23 incidence posting preflight inputs differ".to_string(),
                )
            })?
            .to_vec(),
        V23IncidencePhase::DevelopmentEvaluation => identities
            .get(..4)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "V23 incidence evaluation preflight inputs differ".to_string(),
                )
            })?
            .to_vec(),
        V23IncidencePhase::HoldoutEvaluation => {
            if identities.len() < 6 {
                return Err(BorsukError::InvalidStorage(
                    "V23 incidence evaluation preflight inputs differ".to_string(),
                ));
            }
            vec![
                identities[0].clone(),
                identities[3].clone(),
                identities[4].clone(),
                identities[5].clone(),
            ]
        }
        V23IncidencePhase::HoldoutBinding => {
            if identities.len() < 4 + 256 {
                return Err(BorsukError::InvalidStorage(
                    "V23 incidence holdout binding preflight inputs differ".to_string(),
                ));
            }
            let mut selected = vec![identities[0].clone(), identities[2].clone()];
            selected.extend_from_slice(&identities[4..4 + 256]);
            selected
        }
    };
    Ok(selected)
}

fn expected_v23_incidence_bulk_inputs(
    manifest: &V23IncidenceManifest,
    mode: V23IncidenceRunMode,
) -> Result<Vec<V23IncidenceInputAuthority>> {
    let identities = if mode.is_execute() {
        manifest
            .ordered_inputs
            .iter()
            .map(|input| input.identity().clone())
            .collect()
    } else {
        preflight_registered_inputs(manifest)?
    };
    identities
        .into_iter()
        .map(|identity| {
            manifest
                .ordered_inputs
                .iter()
                .find(|input| input.identity() == &identity)
                .cloned()
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V23 incidence bulk manifest subset differs".to_string(),
                    )
                })
        })
        .collect()
}

fn canonical_json_document(bytes: &[u8], role: &str) -> Result<serde_json::Value> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|error| {
        BorsukError::InvalidStorage(format!("V23 incidence {role} JSON differs: {error}"))
    })?;
    let mut canonical =
        serde_json::to_vec(&canonical_json_value(value.clone())).map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "V23 incidence {role} canonical JSON failed: {error}"
            ))
        })?;
    canonical.push(b'\n');
    if canonical != bytes {
        return Err(BorsukError::InvalidStorage(format!(
            "V23 incidence {role} canonical bytes differ"
        )));
    }
    Ok(value)
}

fn read_authenticated_local_object(
    input: &V23IncidenceLocalRolePath,
    expected_role: &str,
) -> Result<Vec<u8>> {
    if input.identity.role != expected_role || !input.path.is_absolute() {
        return Err(BorsukError::InvalidStorage(format!(
            "V23 incidence {expected_role} path authority differs"
        )));
    }
    authenticate_v23_incidence_local_path(&input.path, &input.identity)?;
    fs::read(&input.path).map_err(|source| BorsukError::Io {
        path: input.path.clone(),
        source,
    })
}

fn expand_v23_incidence_local_directory_request(
    request: V23IncidenceLocalDirectoryPhaseRequest,
) -> Result<V23IncidenceLocalPhaseRequest> {
    let phase = request.mode.phase();
    let manifest_role = if phase == V23IncidencePhase::TreeTraining {
        "construction-manifest"
    } else {
        "phase-manifest"
    };
    let fixed_paths = [
        request.manifest.path.as_path(),
        request.bulk_manifest.path.as_path(),
        request.staging_receipt.path.as_path(),
        request.scratch_path.as_path(),
        request.output_path.as_path(),
    ];
    if !request.staging_directory_path.is_absolute()
        || fixed_paths.iter().any(|path| {
            !path.is_absolute()
                || path.starts_with(&request.staging_directory_path)
                || *path == request.staging_directory_path
        })
        || request.preflight_receipt.as_ref().is_some_and(|input| {
            !input.path.is_absolute() || input.path.starts_with(&request.staging_directory_path)
        })
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence local directory request shape differs".to_string(),
        ));
    }

    let manifest_bytes = read_authenticated_local_object(&request.manifest, manifest_role)?;
    let manifest: V23IncidenceManifest =
        serde_json::from_value(canonical_json_document(&manifest_bytes, "phase manifest")?)
            .map_err(|error| {
                BorsukError::InvalidStorage(format!(
                    "V23 incidence manifest schema differs: {error}"
                ))
            })?;
    validate_manifest(&manifest)?;
    if manifest.phase != phase {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence local directory phase differs".to_string(),
        ));
    }

    let bulk_manifest_bytes =
        read_authenticated_local_object(&request.bulk_manifest, "bulk-manifest")?;
    let bulk_manifest: V23IncidenceManifest = serde_json::from_value(canonical_json_document(
        &bulk_manifest_bytes,
        "bulk manifest",
    )?)
    .map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 incidence bulk manifest schema differs: {error}"
        ))
    })?;
    let mut expected_bulk_manifest = manifest.clone();
    expected_bulk_manifest.ordered_inputs =
        expected_v23_incidence_bulk_inputs(&manifest, request.mode)?;
    if bulk_manifest != expected_bulk_manifest {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence bulk manifest authority differs".to_string(),
        ));
    }

    let staging_receipt_bytes =
        read_authenticated_local_object(&request.staging_receipt, "staging-receipt")?;
    let staging_receipt: V23IncidenceStagingReceipt = serde_json::from_value(
        canonical_json_document(&staging_receipt_bytes, "staging receipt")?,
    )
    .map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 incidence staging receipt schema differs: {error}"
        ))
    })?;
    let expected_identities = expected_bulk_manifest
        .ordered_inputs
        .iter()
        .map(V23IncidenceInputAuthority::identity)
        .cloned()
        .collect::<Vec<_>>();
    if staging_receipt.schema != "borsuk-v23-incidence-staging-receipt-v1"
        || staging_receipt.claim_eligible
        || staging_receipt.manifest_sha256 != request.bulk_manifest.identity.digest
        || staging_receipt.ordered_objects.len() != expected_identities.len()
        || staging_receipt
            .ordered_objects
            .iter()
            .zip(&expected_identities)
            .any(|(observed, expected)| {
                observed.relative_path != expected.role || observed.identity() != *expected
            })
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence staging receipt authority differs".to_string(),
        ));
    }

    let directory_metadata =
        request
            .staging_directory_path
            .symlink_metadata()
            .map_err(|source| BorsukError::Io {
                path: request.staging_directory_path.clone(),
                source,
            })?;
    if !directory_metadata.file_type().is_dir() {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence staging directory shape differs".to_string(),
        ));
    }
    let expected_names = staging_receipt
        .ordered_objects
        .iter()
        .map(|object| object.relative_path.as_str())
        .collect::<BTreeSet<_>>();
    let mut observed_names = BTreeSet::new();
    for entry in
        fs::read_dir(&request.staging_directory_path).map_err(|source| BorsukError::Io {
            path: request.staging_directory_path.clone(),
            source,
        })?
    {
        let entry = entry.map_err(|source| BorsukError::Io {
            path: request.staging_directory_path.clone(),
            source,
        })?;
        let name = entry.file_name().into_string().map_err(|_| {
            BorsukError::InvalidStorage("V23 incidence staging entry name differs".to_string())
        })?;
        let metadata = fs::symlink_metadata(entry.path()).map_err(|source| BorsukError::Io {
            path: entry.path(),
            source,
        })?;
        if !metadata.file_type().is_file() || !observed_names.insert(name) {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence staging entry shape differs".to_string(),
            ));
        }
    }
    if observed_names
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        != expected_names
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence staging directory membership differs".to_string(),
        ));
    }

    let mut input_paths = vec![
        request.manifest.clone(),
        request.bulk_manifest.clone(),
        request.staging_receipt.clone(),
    ];
    for identity in expected_identities {
        let path = request.staging_directory_path.join(&identity.role);
        authenticate_v23_incidence_local_path(&path, &identity)?;
        input_paths.push(V23IncidenceLocalRolePath { identity, path });
    }
    if let Some(preflight_receipt) = &request.preflight_receipt {
        input_paths.push(preflight_receipt.clone());
    }
    let parent_receipt_path = input_paths
        .iter()
        .find(|input| input.identity.role == "parent-receipt")
        .map(|input| input.path.clone());
    let preflight_receipt_path = request
        .preflight_receipt
        .as_ref()
        .map(|input| input.path.clone());
    let expanded = V23IncidenceLocalPhaseRequest {
        mode: request.mode,
        manifest_path: request.manifest.path,
        parent_receipt_path,
        preflight_receipt_path,
        input_paths,
        scratch_path: request.scratch_path,
        output_path: request.output_path,
        executable_sha256: request.executable_sha256,
    };
    expanded.validate()?;
    Ok(expanded)
}

fn validate_v23_incidence_request_manifest(
    request: &V23IncidenceLocalPhaseRequest,
    manifest: &V23IncidenceManifest,
    manifest_identity: &V23IncidenceObjectIdentity,
) -> Result<V23IncidenceManifestBinding> {
    validate_manifest(manifest)?;
    let manifest_role = if manifest.phase == V23IncidencePhase::TreeTraining {
        "construction-manifest"
    } else {
        "phase-manifest"
    };
    if request.mode.phase() != manifest.phase
        || manifest_identity.role != manifest_role
        || request.input_paths.first().is_none_or(|input| {
            input.path != request.manifest_path || input.identity != *manifest_identity
        })
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence local manifest identity differs".to_string(),
        ));
    }
    let registered = manifest
        .ordered_inputs
        .iter()
        .map(|input| input.identity().clone())
        .collect::<Vec<_>>();
    let mut expected_mounted = vec![manifest_identity.clone()];
    if request.mode.is_execute() {
        expected_mounted.extend(registered.iter().cloned());
    } else {
        expected_mounted.extend(preflight_registered_inputs(manifest)?);
    }
    let observed = request
        .input_paths
        .iter()
        .filter(|input| {
            !matches!(
                input.identity.role.as_str(),
                "preflight-receipt" | "bulk-manifest" | "staging-receipt"
            )
        })
        .map(|input| input.identity.clone())
        .collect::<Vec<_>>();
    if observed != expected_mounted {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence mounted manifest subset differs".to_string(),
        ));
    }
    let full_input_bytes = std::iter::once(manifest_identity)
        .chain(registered.iter())
        .try_fold(0_u64, |sum, identity| {
            sum.checked_add(identity.encoded_bytes)
        })
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence registered input bytes overflow".to_string())
        })?;
    Ok(V23IncidenceManifestBinding {
        parent_receipt_sha256: manifest.parent_receipt_sha256.clone(),
        full_input_bytes,
        ordered_inputs: expected_mounted,
    })
}

fn validate_v23_incidence_parent_receipt(
    phase: V23IncidencePhase,
    manifest: &V23IncidenceManifest,
    bytes: &[u8],
    executable_sha256: &str,
) -> Result<V23IncidenceReceipt> {
    let receipt: V23IncidenceReceipt = serde_json::from_slice(bytes).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 incidence parent receipt JSON differs: {error}"
        ))
    })?;
    validate_receipt(&receipt)?;
    let value = serde_json::to_value(&receipt).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 incidence parent receipt serialization failed: {error}"
        ))
    })?;
    let mut canonical = serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 incidence parent receipt canonical JSON failed: {error}"
        ))
    })?;
    canonical.push(b'\n');
    let observed_parent = format!("{:x}", Sha256::digest(bytes));
    let predecessor = match phase {
        V23IncidencePhase::TreeTraining => {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence tree phase has no parent".to_string(),
            ));
        }
        V23IncidencePhase::PostingConstruction => V23IncidencePhase::TreeTraining,
        V23IncidencePhase::DevelopmentEvaluation => V23IncidencePhase::PostingConstruction,
        V23IncidencePhase::HoldoutBinding => V23IncidencePhase::DevelopmentEvaluation,
        V23IncidencePhase::HoldoutEvaluation => V23IncidencePhase::HoldoutBinding,
    };
    if canonical != bytes
        || manifest.parent_receipt_sha256.as_deref() != Some(observed_parent.as_str())
        || receipt.phase != predecessor
        || receipt.run_mode != V23IncidenceReceiptRunMode::Execute
        || receipt.stop.is_some()
        || receipt.executable_sha256 != executable_sha256
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence parent receipt authority differs".to_string(),
        ));
    }
    let manifest_identity = |role: &str| {
        manifest
            .ordered_inputs
            .iter()
            .map(V23IncidenceInputAuthority::identity)
            .find(|identity| identity.role == role)
    };
    let parent_identity = |role: &str| {
        receipt
            .outputs
            .iter()
            .chain(receipt.ordered_mounts.iter())
            .find(|identity| identity.role == role)
    };
    let bound_roles: &[&str] = match phase {
        V23IncidencePhase::TreeTraining => &[],
        V23IncidencePhase::PostingConstruction => &["incidence-tree"],
        V23IncidencePhase::DevelopmentEvaluation => &[
            "incidence-tree",
            "incidence-postings-one",
            "incidence-postings-two",
        ],
        V23IncidencePhase::HoldoutBinding => &["development-result"],
        V23IncidencePhase::HoldoutEvaluation => &["development-result", "holdout-truth"],
    };
    if bound_roles
        .iter()
        .any(|role| manifest_identity(role) != parent_identity(role))
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence parent receipt output binding differs".to_string(),
        ));
    }
    Ok(receipt)
}

fn authenticate_v23_incidence_request_inputs(
    inputs: &[V23IncidenceLocalRolePath],
) -> Result<V23IncidenceInputMeasurement> {
    inputs.iter().try_fold(
        V23IncidenceInputMeasurement {
            input_bytes: 0,
            input_elapsed_ns: 0,
        },
        |total, input| {
            let measured = authenticate_v23_incidence_local_path(&input.path, &input.identity)?;
            if matches!(
                input.identity.role.as_str(),
                "bulk-manifest" | "staging-receipt"
            ) {
                return Ok(total);
            }
            Ok(V23IncidenceInputMeasurement {
                input_bytes: total
                    .input_bytes
                    .checked_add(measured.input_bytes)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "V23 incidence authenticated input bytes overflow".to_string(),
                        )
                    })?,
                input_elapsed_ns: total
                    .input_elapsed_ns
                    .checked_add(measured.input_elapsed_ns)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "V23 incidence authenticated input time overflows".to_string(),
                        )
                    })?,
            })
        },
    )
}

fn authenticate_v23_incidence_tree_inputs_with_progress(
    inputs: &[V23IncidenceLocalRolePath],
    mut progress: impl FnMut(&V23IncidenceObjectIdentity) -> Result<()>,
) -> Result<()> {
    for input in inputs {
        authenticate_v23_incidence_local_path(&input.path, &input.identity)?;
        if input.identity.role.starts_with("training-shard-") {
            progress(&input.identity)?;
        }
    }
    Ok(())
}

fn run_v23_incidence_tree_preflight(
    request: &V23IncidenceLocalPhaseRequest,
    binding: V23IncidenceManifestBinding,
    sandbox_probes: &str,
) -> Result<Vec<u8>> {
    let measured_inputs = authenticate_v23_incidence_request_inputs(&request.input_paths)?;
    let training_path = request
        .input_paths
        .iter()
        .find(|input| input.identity.role == "training-shard-0000")
        .map(|input| input.path.as_path())
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "V23 incidence tree preflight training shard is absent".to_string(),
            )
        })?;
    let work = v23_incidence_preflight_work(V23IncidencePhase::TreeTraining);
    let rows = read_v23_incidence_training_preflight_rows(
        training_path,
        usize::try_from(work.sample_vectors).map_err(|_| {
            BorsukError::InvalidStorage(
                "V23 incidence tree preflight sample count differs".to_string(),
            )
        })?,
    )?;
    let measured_kernel = measure_v23_incidence_tree_preflight(&rows)?;
    let (probes, network_namespace_inode) = parse_v23_incidence_sandbox_probes(sandbox_probes)?;
    let authority = V23IncidencePreflightAuthority {
        parent_receipt_sha256: binding.parent_receipt_sha256,
        executable_sha256: request.executable_sha256.clone(),
        fma_backend: measured_kernel.fma_backend,
        network_namespace_inode,
        probes,
        full_input_bytes: binding.full_input_bytes,
        ordered_inputs: binding.ordered_inputs,
    };
    let evidence = project_v23_incidence_preflight(
        work,
        authority.clone(),
        V23IncidencePreflightMeasurement {
            distance_dimensions: measured_kernel.distance_dimensions,
            distance_elapsed_ns: measured_kernel.distance_elapsed_ns,
            input_bytes: measured_inputs.input_bytes,
            input_elapsed_ns: measured_inputs.input_elapsed_ns,
            records: 0,
            records_elapsed_ns: 0,
        },
    )?;
    canonical_v23_incidence_preflight_bytes(&evidence, &authority, None)
}

fn run_v23_incidence_posting_preflight(
    request: &V23IncidenceLocalPhaseRequest,
    binding: V23IncidenceManifestBinding,
    sandbox_probes: &str,
) -> Result<Vec<u8>> {
    let measured_inputs = authenticate_v23_incidence_request_inputs(&request.input_paths)?;
    let tree_path = request
        .input_paths
        .iter()
        .find(|input| input.identity.role == "incidence-tree")
        .map(|input| input.path.as_path())
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "V23 incidence posting preflight tree is absent".to_string(),
            )
        })?;
    let tree_bytes = fs::read(tree_path).map_err(|source| BorsukError::Io {
        path: tree_path.to_path_buf(),
        source,
    })?;
    let pages = request
        .input_paths
        .iter()
        .filter(|input| input.identity.role.starts_with("page-body-"))
        .map(|input| {
            fs::read(&input.path)
                .map(Bytes::from)
                .map(|bytes| (input.identity.clone(), bytes))
                .map_err(|source| BorsukError::Io {
                    path: input.path.clone(),
                    source,
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let measured_kernel = measure_v23_incidence_posting_pages_preflight(&tree_bytes, &pages)?;
    let measured_records = measure_v23_incidence_posting_sort_preflight(&request.scratch_path)?;
    let (probes, network_namespace_inode) = parse_v23_incidence_sandbox_probes(sandbox_probes)?;
    let authority = V23IncidencePreflightAuthority {
        parent_receipt_sha256: binding.parent_receipt_sha256,
        executable_sha256: request.executable_sha256.clone(),
        fma_backend: measured_kernel.fma_backend,
        network_namespace_inode,
        probes,
        full_input_bytes: binding.full_input_bytes,
        ordered_inputs: binding.ordered_inputs,
    };
    let evidence = project_v23_incidence_preflight(
        v23_incidence_preflight_work(V23IncidencePhase::PostingConstruction),
        authority.clone(),
        V23IncidencePreflightMeasurement {
            distance_dimensions: measured_kernel.distance_dimensions,
            distance_elapsed_ns: measured_kernel.distance_elapsed_ns,
            input_bytes: measured_inputs.input_bytes,
            input_elapsed_ns: measured_inputs.input_elapsed_ns,
            records: measured_records.records,
            records_elapsed_ns: measured_records.records_elapsed_ns,
        },
    )?;
    let parent_path = request.parent_receipt_path.as_deref().ok_or_else(|| {
        BorsukError::InvalidStorage("V23 incidence posting preflight parent is absent".to_string())
    })?;
    let parent_bytes = fs::read(parent_path).map_err(|source| BorsukError::Io {
        path: parent_path.to_path_buf(),
        source,
    })?;
    canonical_v23_incidence_preflight_bytes(&evidence, &authority, Some(&parent_bytes))
}

fn run_v23_incidence_holdout_binding_preflight(
    request: &V23IncidenceLocalPhaseRequest,
    binding: V23IncidenceManifestBinding,
    sandbox_probes: &str,
) -> Result<Vec<u8>> {
    let mut measured_inputs = authenticate_v23_incidence_request_inputs(&request.input_paths)?;
    let decode_started = Instant::now();
    let pages = request
        .input_paths
        .iter()
        .filter(|input| input.identity.role.starts_with("page-body-"))
        .map(|input| {
            fs::read(&input.path)
                .map(Bytes::from)
                .map(|bytes| (input.identity.clone(), bytes))
                .map_err(|source| BorsukError::Io {
                    path: input.path.clone(),
                    source,
                })
        })
        .collect::<Result<Vec<_>>>()?;
    std::hint::black_box(decode_v23_incidence_preflight_pages(&pages)?);
    measured_inputs.input_elapsed_ns = measured_inputs
        .input_elapsed_ns
        .checked_add(
            u64::try_from(decode_started.elapsed().as_nanos())
                .unwrap_or(u64::MAX)
                .max(1),
        )
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "V23 incidence holdout preflight input time overflows".to_string(),
            )
        })?;
    let mut probe = [0.0_f32; 96];
    probe[0] = 1.0;
    let measured_kernel = measure_v23_incidence_tree_preflight(&[probe])?;
    let (probes, network_namespace_inode) = parse_v23_incidence_sandbox_probes(sandbox_probes)?;
    let authority = V23IncidencePreflightAuthority {
        parent_receipt_sha256: binding.parent_receipt_sha256,
        executable_sha256: request.executable_sha256.clone(),
        fma_backend: measured_kernel.fma_backend,
        network_namespace_inode,
        probes,
        full_input_bytes: binding.full_input_bytes,
        ordered_inputs: binding.ordered_inputs,
    };
    let evidence = project_v23_incidence_preflight(
        v23_incidence_preflight_work(V23IncidencePhase::HoldoutBinding),
        authority.clone(),
        V23IncidencePreflightMeasurement {
            distance_dimensions: 0,
            distance_elapsed_ns: 0,
            input_bytes: measured_inputs.input_bytes,
            input_elapsed_ns: measured_inputs.input_elapsed_ns,
            records: 0,
            records_elapsed_ns: 0,
        },
    )?;
    let parent_path = request.parent_receipt_path.as_deref().ok_or_else(|| {
        BorsukError::InvalidStorage("V23 incidence holdout preflight parent is absent".to_string())
    })?;
    let parent_bytes = fs::read(parent_path).map_err(|source| BorsukError::Io {
        path: parent_path.to_path_buf(),
        source,
    })?;
    canonical_v23_incidence_preflight_bytes(&evidence, &authority, Some(&parent_bytes))
}

fn run_v23_incidence_evaluation_preflight(
    request: &V23IncidenceLocalPhaseRequest,
    binding: V23IncidenceManifestBinding,
    sandbox_probes: &str,
    phase: V23IncidencePhase,
) -> Result<Vec<u8>> {
    let measured_inputs = authenticate_v23_incidence_request_inputs(&request.input_paths)?;
    let read_role = |role: &str| -> Result<Vec<u8>> {
        let input = request
            .input_paths
            .iter()
            .find(|input| input.identity.role == role)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "V23 incidence evaluation preflight {role} is absent"
                ))
            })?;
        fs::read(&input.path).map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })
    };
    let tree = decode_incidence_tree(&read_role("incidence-tree")?)?;
    let postings = decode_posting_plane(&read_role("incidence-postings-two")?)?;
    let work = v23_incidence_preflight_work(phase);
    let measured = measure_v23_incidence_evaluation_preflight(
        &tree,
        &postings,
        usize::try_from(work.sample_queries).map_err(|_| {
            BorsukError::InvalidStorage(
                "V23 incidence evaluation preflight query count overflows".to_string(),
            )
        })?,
        28_282,
    )?;
    let mut probe = [0.0_f32; 96];
    probe[0] = 1.0;
    let measured_kernel = measure_v23_incidence_tree_preflight(&[probe])?;
    let (probes, network_namespace_inode) = parse_v23_incidence_sandbox_probes(sandbox_probes)?;
    let authority = V23IncidencePreflightAuthority {
        parent_receipt_sha256: binding.parent_receipt_sha256,
        executable_sha256: request.executable_sha256.clone(),
        fma_backend: measured_kernel.fma_backend,
        network_namespace_inode,
        probes,
        full_input_bytes: binding.full_input_bytes,
        ordered_inputs: binding.ordered_inputs,
    };
    let evidence = project_v23_incidence_preflight(
        work,
        authority.clone(),
        V23IncidencePreflightMeasurement {
            distance_dimensions: measured.distance_dimensions,
            distance_elapsed_ns: measured.distance_elapsed_ns,
            input_bytes: measured_inputs.input_bytes,
            input_elapsed_ns: measured_inputs.input_elapsed_ns,
            records: measured.posting_visits,
            records_elapsed_ns: measured.posting_elapsed_ns,
        },
    )?;
    let parent_path = request.parent_receipt_path.as_deref().ok_or_else(|| {
        BorsukError::InvalidStorage(
            "V23 incidence evaluation preflight parent is absent".to_string(),
        )
    })?;
    let parent_bytes = fs::read(parent_path).map_err(|source| BorsukError::Io {
        path: parent_path.to_path_buf(),
        source,
    })?;
    canonical_v23_incidence_preflight_bytes(&evidence, &authority, Some(&parent_bytes))
}

fn validate_v23_incidence_request_execution_preflight(
    request: &V23IncidenceLocalPhaseRequest,
    manifest: &V23IncidenceManifest,
    binding: &V23IncidenceManifestBinding,
    manifest_identity: &V23IncidenceObjectIdentity,
) -> Result<(V23IncidenceReceipt, Vec<u8>)> {
    let preflight_path = request.preflight_receipt_path.as_deref().ok_or_else(|| {
        BorsukError::InvalidStorage("V23 incidence execution preflight path is absent".to_string())
    })?;
    let preflight_input = request
        .input_paths
        .iter()
        .find(|input| input.identity.role == "preflight-receipt")
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "V23 incidence execution preflight identity is absent".to_string(),
            )
        })?;
    if preflight_input.path != preflight_path {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence execution preflight path differs".to_string(),
        ));
    }
    let bytes = fs::read(preflight_path).map_err(|source| BorsukError::Io {
        path: preflight_path.to_path_buf(),
        source,
    })?;
    let claimed: V23IncidenceReceipt = serde_json::from_slice(&bytes).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 incidence execution preflight JSON differs: {error}"
        ))
    })?;
    let mut ordered_inputs = vec![manifest_identity.clone()];
    ordered_inputs.extend(preflight_registered_inputs(manifest)?);
    let expected_authority = V23IncidencePreflightAuthority {
        parent_receipt_sha256: binding.parent_receipt_sha256.clone(),
        executable_sha256: request.executable_sha256.clone(),
        fma_backend: claimed.fma_backend,
        network_namespace_inode: claimed.network_namespace_inode,
        probes: claimed.probes.clone(),
        full_input_bytes: binding.full_input_bytes,
        ordered_inputs,
    };
    let parent_bytes = request
        .parent_receipt_path
        .as_deref()
        .map(|path| {
            fs::read(path).map_err(|source| BorsukError::Io {
                path: path.to_path_buf(),
                source,
            })
        })
        .transpose()?;
    let receipt = validate_v23_incidence_execution_preflight(
        &bytes,
        &preflight_input.identity,
        &expected_authority,
        parent_bytes.as_deref(),
    )?;
    if receipt.phase != request.mode.phase()
        || receipt.executable_sha256 != request.executable_sha256
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence execution preflight authority differs".to_string(),
        ));
    }
    Ok((receipt, bytes))
}

fn run_v23_incidence_tree_training(
    request: &V23IncidenceLocalPhaseRequest,
    manifest: &V23IncidenceManifest,
    preflight: &V23IncidenceReceipt,
    preflight_bytes: &[u8],
    sandbox_probes: &str,
) -> Result<Vec<u8>> {
    let manifest_identity = request
        .input_paths
        .iter()
        .find(|input| input.identity.role == "construction-manifest")
        .map(|input| &input.identity)
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "V23 incidence construction manifest identity is absent".to_string(),
            )
        })?;
    let shard_identities = manifest
        .ordered_inputs
        .iter()
        .filter_map(|input| match input {
            V23IncidenceInputAuthority::TrainingShard { identity, .. } => Some(identity),
            _ => None,
        })
        .collect::<Vec<_>>();
    let shard_units = u64::try_from(shard_identities.len()).map_err(|_| {
        BorsukError::InvalidStorage("V23 incidence progress shard count overflows".to_string())
    })?;
    let source_rows = manifest
        .ordered_inputs
        .iter()
        .try_fold(0_u64, |sum, input| match input {
            V23IncidenceInputAuthority::TrainingShard {
                ordinal_start,
                ordinal_end,
                ..
            } => ordinal_end
                .checked_sub(*ordinal_start)
                .and_then(|rows| sum.checked_add(rows))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V23 incidence progress source rows overflow".to_string(),
                    )
                }),
            _ => Ok(sum),
        })?;
    let source_row_units = source_rows
        .checked_add(V23_INCIDENCE_PROGRESS_SOURCE_ROWS - 1)
        .map(|rows| rows / V23_INCIDENCE_PROGRESS_SOURCE_ROWS)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence progress work overflows".to_string())
        })?;
    let total_units = shard_units
        .checked_add(source_row_units)
        .and_then(|units| units.checked_add(1))
        .and_then(|units| units.checked_add(u64::from(manifest.algorithm.tree_depth)))
        .and_then(|units| units.checked_add(1))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence progress work overflows".to_string())
        })?;
    let progress_path = request
        .output_path
        .parent()
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "V23 incidence progress output directory is absent".to_string(),
            )
        })?
        .join("progress.json");
    let mut progress = V23IncidenceProgressChain::start(
        &progress_path,
        V23IncidencePhase::TreeTraining,
        total_units,
        &manifest_identity.digest,
    )?;
    let mut completed_units = 0_u64;
    let mut last_input_digest = manifest_identity.digest.clone();
    authenticate_v23_incidence_tree_inputs_with_progress(&request.input_paths, |identity| {
        completed_units = completed_units.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence progress work overflows".to_string())
        })?;
        last_input_digest.clone_from(&identity.digest);
        progress.advance(completed_units, &last_input_digest)?;
        Ok(())
    })?;
    if completed_units != shard_units {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence progress shard count differs".to_string(),
        ));
    }
    let shards = manifest
        .ordered_inputs
        .iter()
        .filter_map(|input| match input {
            V23IncidenceInputAuthority::TrainingShard {
                identity,
                ordinal_start,
                ordinal_end,
                ..
            } => Some((identity, *ordinal_start, *ordinal_end)),
            _ => None,
        })
        .map(|(identity, ordinal_start, ordinal_end)| {
            let path = request
                .input_paths
                .iter()
                .find(|input| input.identity == *identity)
                .map(|input| input.path.clone())
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V23 incidence training shard path is absent".to_string(),
                    )
                })?;
            Ok((path, ordinal_start, ordinal_end))
        })
        .collect::<Result<Vec<_>>>()?;
    let rows = v23_incidence_training_row_stream(shards)?;
    let mut expected_level = 0_u64;
    let mut completed_source_rows = 0_u64;
    let mut reservoir_complete = false;
    let tree = train_incidence_tree(rows, source_rows, 8, 4_096, |milestone| {
        match milestone {
            V23IncidenceTrainingMilestone::SourceRows { completed_rows }
                if !reservoir_complete
                    && completed_rows > completed_source_rows
                    && completed_rows <= source_rows =>
            {
                completed_source_rows = completed_rows;
            }
            V23IncidenceTrainingMilestone::Reservoir {
                source_rows: observed_source_rows,
                reservoir_rows,
            } if !reservoir_complete
                && completed_source_rows == source_rows
                && observed_source_rows == source_rows
                && reservoir_rows == u64::from(manifest.algorithm.reservoir_rows) =>
            {
                reservoir_complete = true;
            }
            V23IncidenceTrainingMilestone::TreeLevel { level, .. }
                if reservoir_complete && level == expected_level + 1 =>
            {
                expected_level = level;
            }
            _ => {
                return Err(BorsukError::InvalidStorage(
                    "V23 incidence training progress milestone differs".to_string(),
                ));
            }
        }
        completed_units = completed_units.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence progress work overflows".to_string())
        })?;
        progress.advance(completed_units, &last_input_digest)?;
        Ok(())
    })?;
    if !reservoir_complete
        || expected_level != u64::from(manifest.algorithm.tree_depth)
        || completed_units + 1 != total_units
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence training progress completion differs".to_string(),
        ));
    }
    let tree_bytes = encode_incidence_tree(&tree)?;

    let mut probe = [0.0_f32; 96];
    probe[0] = 1.0;
    let kernel = measure_v23_incidence_tree_preflight(&[probe])?;
    if kernel.fma_backend != preflight.fma_backend {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence execution FMA backend differs".to_string(),
        ));
    }
    let (probes, network_namespace_inode) = parse_v23_incidence_sandbox_probes(sandbox_probes)?;
    validate_v23_incidence_execution_namespace(
        preflight.network_namespace_inode,
        network_namespace_inode,
    )?;
    let (output, output_path) = write_v23_incidence_local_output(
        "incidence-tree",
        "blake3",
        &tree_bytes,
        &request.scratch_path,
        &request.output_path,
    )?;
    let final_progress_sha256 = progress.advance(total_units, &output.digest)?;
    let receipt = V23IncidenceReceipt {
        schema: V23_INCIDENCE_RECEIPT_SCHEMA.to_string(),
        claim_eligible: false,
        phase: V23IncidencePhase::TreeTraining,
        run_mode: V23IncidenceReceiptRunMode::Execute,
        parent_receipt_sha256: Some(format!("{:x}", Sha256::digest(preflight_bytes))),
        executable_sha256: request.executable_sha256.clone(),
        fma_backend: kernel.fma_backend,
        network_namespace_inode,
        ordered_mounts: request
            .input_paths
            .iter()
            .map(|input| input.identity.clone())
            .collect(),
        probes,
        preflight_evidence: None,
        final_progress_sha256: Some(final_progress_sha256),
        outputs: vec![output],
        stop: None,
    };
    let result = canonical_v23_incidence_receipt_bytes(
        &receipt,
        Some(preflight_bytes),
        &[("incidence-tree", tree_bytes.as_slice())],
    );
    if result.is_err() {
        let _ = fs::remove_file(output_path);
        let _ = fs::remove_file(progress_path);
    }
    result
}

fn run_v23_incidence_posting_build(
    request: &V23IncidenceLocalPhaseRequest,
    preflight: &V23IncidenceReceipt,
    preflight_bytes: &[u8],
    sandbox_probes: &str,
) -> Result<Vec<u8>> {
    authenticate_v23_incidence_request_inputs(&request.input_paths)?;
    let tree_path = request
        .input_paths
        .iter()
        .find(|input| input.identity.role == "incidence-tree")
        .map(|input| input.path.as_path())
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "V23 incidence posting execution tree is absent".to_string(),
            )
        })?;
    let tree_bytes = fs::read(tree_path).map_err(|source| BorsukError::Io {
        path: tree_path.to_path_buf(),
        source,
    })?;
    let tree = decode_incidence_tree(&tree_bytes)?;
    let pages = request
        .input_paths
        .iter()
        .filter(|input| input.identity.role.starts_with("page-body-"))
        .cloned()
        .collect::<Vec<_>>();
    let run_records = usize::try_from(V23_POSTING_RUN_BYTES / 8).map_err(|_| {
        BorsukError::InvalidStorage("V23 incidence posting run size overflows".to_string())
    })?;
    let mut probe = [0.0_f32; 96];
    probe[0] = 1.0;
    let (_, fma_backend) = split_score_simd(
        tree.nodes.first().ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence posting tree is empty".to_string())
        })?,
        &probe,
    )?;
    if fma_backend != preflight.fma_backend {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence execution FMA backend differs".to_string(),
        ));
    }
    let (probes, network_namespace_inode) = parse_v23_incidence_sandbox_probes(sandbox_probes)?;
    validate_v23_incidence_execution_namespace(
        preflight.network_namespace_inode,
        network_namespace_inode,
    )?;
    if request.output_path.exists() {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence posting receipt already exists".to_string(),
        ));
    }
    let output_directory = request
        .output_path
        .parent()
        .filter(|path| path.is_dir())
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "V23 incidence posting output directory differs".to_string(),
            )
        })?;
    let [one, two] = build_both_posting_plane_files(
        v23_incidence_page_posting_stream(&tree, pages),
        &request.scratch_path,
        output_directory,
        run_records,
        V23_POSTING_MAX_PAGES,
    )?;
    if one.source_records != crate::v23_incidence_postings::V23_POSTING_ONE_ARM_RECORDS
        || two.source_records != crate::v23_incidence_postings::V23_POSTING_TWO_ARM_RECORDS
    {
        let _ = fs::remove_file(&one.path);
        let _ = fs::remove_file(&two.path);
        return Err(BorsukError::InvalidStorage(
            "V23 production posting record count differs".to_string(),
        ));
    }
    let identity = |role: &str, artifact: &crate::v23_incidence_postings::V23PostingArtifact| {
        V23IncidenceObjectIdentity {
            role: role.to_string(),
            uri: format!("file://{}", artifact.path.display()),
            digest_algorithm: "blake3".to_string(),
            digest: artifact.digest.clone(),
            encoded_bytes: artifact.encoded_bytes,
            generation: format!("content-{}", artifact.digest),
        }
    };
    let one_identity = identity("incidence-postings-one", &one);
    let two_identity = identity("incidence-postings-two", &two);
    let receipt = V23IncidenceReceipt {
        schema: V23_INCIDENCE_RECEIPT_SCHEMA.to_string(),
        claim_eligible: false,
        phase: V23IncidencePhase::PostingConstruction,
        run_mode: V23IncidenceReceiptRunMode::Execute,
        parent_receipt_sha256: Some(format!("{:x}", Sha256::digest(preflight_bytes))),
        executable_sha256: request.executable_sha256.clone(),
        fma_backend,
        network_namespace_inode,
        ordered_mounts: request
            .input_paths
            .iter()
            .map(|input| input.identity.clone())
            .collect(),
        probes,
        preflight_evidence: None,
        final_progress_sha256: None,
        outputs: vec![one_identity, two_identity],
        stop: None,
    };
    let result = canonical_v23_incidence_receipt_path_bytes(
        &receipt,
        Some(preflight_bytes),
        &[
            ("incidence-postings-one", one.path.as_path()),
            ("incidence-postings-two", two.path.as_path()),
        ],
    );
    if result.is_err() {
        let _ = fs::remove_file(one.path);
        let _ = fs::remove_file(two.path);
    }
    result
}

fn run_v23_incidence_development_evaluation(
    request: &V23IncidenceLocalPhaseRequest,
    preflight: &V23IncidenceReceipt,
    preflight_bytes: &[u8],
    sandbox_probes: &str,
) -> Result<Vec<u8>> {
    authenticate_v23_incidence_request_inputs(&request.input_paths)?;
    let read_role = |role: &str| -> Result<(&V23IncidenceObjectIdentity, Vec<u8>)> {
        let input = request
            .input_paths
            .iter()
            .find(|input| input.identity.role == role)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(format!("V23 incidence development {role} is absent"))
            })?;
        let bytes = fs::read(&input.path).map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        Ok((&input.identity, bytes))
    };
    let (tree_identity, tree_bytes) = read_role("incidence-tree")?;
    let (one_identity, one_bytes) = read_role("incidence-postings-one")?;
    let (two_identity, two_bytes) = read_role("incidence-postings-two")?;
    let (d2_identity, d2_bytes) = read_role("d2-report")?;
    let (query_identity, query_bytes) = read_role("query-parquet")?;
    let tree = decode_incidence_tree(&tree_bytes)?;
    let one = decode_posting_plane(&one_bytes)?;
    let two = decode_posting_plane(&two_bytes)?;
    let queries = read_v23_incidence_development_queries(&query_bytes)?;
    let truth = read_v23_incidence_development_truth(&d2_bytes)?;
    if d2_identity.digest_algorithm != "sha256"
        || query_identity.digest_algorithm != "sha256"
        || queries.len() != truth.len()
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence development input authority differs".to_string(),
        ));
    }
    let mut development = Vec::with_capacity(18);
    let mut latency_artifacts = Vec::with_capacity(18);
    for cell in V23IncidenceCell::registered_ladder() {
        let plane = match cell.arm {
            PostingAssignmentArm::OneLeaf => &one,
            PostingAssignmentArm::TwoBeamLeaves => &two,
        };
        let mut workspace = V23IncidenceQueryWorkspace::new(28_282)?;
        let mut ordinal = 0_usize;
        let latency = measure_v23_incidence_latency(|| {
            let query = &queries[ordinal % queries.len()];
            ordinal += 1;
            score_incidence_query_native(&tree, plane, cell, query, &mut workspace).map(|_| ())
        })?;
        development.push(evaluate_v23_incidence_cell(
            &tree, plane, cell, &queries, &truth, 28_282, &latency,
        )?);
        latency_artifacts.push(latency);
    }
    let authority = V23IncidenceDevelopmentAuthority {
        source_commit: V23_INCIDENCE_SOURCE_COMMIT.to_string(),
        source_archive_sha256: V23_INCIDENCE_SOURCE_ARCHIVE_SHA256.to_string(),
        index_id: V23_INCIDENCE_INDEX_ID.to_string(),
        dataset_id: V23_INCIDENCE_DATASET_ID.to_string(),
        query_cohort_sha256: query_identity.digest.clone(),
        tree_blake3: tree_identity.digest.clone(),
        posting_one_blake3: one_identity.digest.clone(),
        posting_two_blake3: two_identity.digest.clone(),
        executable_sha256: request.executable_sha256.clone(),
    };
    let sealed_cell = development
        .iter()
        .find(|cell| {
            cell.retention_passed
                && cell.quality.passed
                && cell.determinism_passed
                && cell.projected_serving_bytes <= 3 * 1024 * 1024 * 1024
                && cell.maximum_posting_visits <= 262_144
                && cell.maximum_touched_pages <= 8_192
                && cell.p99_ns <= 15_000_000
        })
        .map(|cell| cell.cell);
    let artifact = V23IncidenceDevelopmentArtifact {
        schema: "borsuk-v23-incidence-development-v1".to_string(),
        claim_eligible: false,
        authority: authority.clone(),
        development,
        development_truth: truth.clone(),
        sealed_cell,
    };
    let artifact_bytes = canonical_v23_incidence_development_artifact_bytes(
        &artifact,
        &authority,
        &latency_artifacts,
        &truth,
    )?;
    let latency_bytes = encode_v23_incidence_development_latency_bundle(&latency_artifacts)?;

    let mut probe = [0.0_f32; 96];
    probe[0] = 1.0;
    let (_, fma_backend) = split_score_simd(
        tree.nodes.first().ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence development tree is empty".to_string())
        })?,
        &probe,
    )?;
    if fma_backend != preflight.fma_backend {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence execution FMA backend differs".to_string(),
        ));
    }
    let (probes, network_namespace_inode) = parse_v23_incidence_sandbox_probes(sandbox_probes)?;
    validate_v23_incidence_execution_namespace(
        preflight.network_namespace_inode,
        network_namespace_inode,
    )?;
    let (artifact_identity, artifact_path) = write_v23_incidence_local_output(
        "development-result",
        "sha256",
        &artifact_bytes,
        &request.scratch_path,
        &request.output_path,
    )?;
    let (latency_identity, latency_path) = match write_v23_incidence_local_output(
        "development-latency",
        "blake3",
        &latency_bytes,
        &request.scratch_path,
        &request.output_path,
    ) {
        Ok(output) => output,
        Err(error) => {
            let _ = fs::remove_file(&artifact_path);
            return Err(error);
        }
    };
    let receipt = V23IncidenceReceipt {
        schema: V23_INCIDENCE_RECEIPT_SCHEMA.to_string(),
        claim_eligible: false,
        phase: V23IncidencePhase::DevelopmentEvaluation,
        run_mode: V23IncidenceReceiptRunMode::Execute,
        parent_receipt_sha256: Some(format!("{:x}", Sha256::digest(preflight_bytes))),
        executable_sha256: request.executable_sha256.clone(),
        fma_backend,
        network_namespace_inode,
        ordered_mounts: request
            .input_paths
            .iter()
            .map(|input| input.identity.clone())
            .collect(),
        probes,
        preflight_evidence: None,
        final_progress_sha256: None,
        outputs: vec![artifact_identity, latency_identity],
        stop: None,
    };
    let result = canonical_v23_incidence_receipt_bytes(
        &receipt,
        Some(preflight_bytes),
        &[
            ("development-result", artifact_bytes.as_slice()),
            ("development-latency", latency_bytes.as_slice()),
        ],
    );
    if result.is_err() {
        let _ = fs::remove_file(artifact_path);
        let _ = fs::remove_file(latency_path);
    }
    result
}

fn run_v23_incidence_holdout_binding(
    request: &V23IncidenceLocalPhaseRequest,
    preflight: &V23IncidenceReceipt,
    preflight_bytes: &[u8],
    sandbox_probes: &str,
) -> Result<Vec<u8>> {
    authenticate_v23_incidence_request_inputs(&request.input_paths)?;
    let role = |name: &str| {
        request
            .input_paths
            .iter()
            .find(|input| input.identity.role == name)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "V23 incidence holdout binding {name} is absent"
                ))
            })
    };
    let development_input = role("development-result")?;
    let development_bytes =
        fs::read(&development_input.path).map_err(|source| BorsukError::Io {
            path: development_input.path.clone(),
            source,
        })?;
    let development: V23IncidenceDevelopmentArtifact = serde_json::from_slice(&development_bytes)
        .map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 incidence development artifact JSON differs: {error}"
        ))
    })?;
    let mut canonical_development = serde_json::to_vec(&canonical_json_value(
        serde_json::to_value(&development).map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "V23 incidence development artifact serialization failed: {error}"
            ))
        })?,
    ))
    .map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 incidence development artifact serialization failed: {error}"
        ))
    })?;
    canonical_development.push(b'\n');
    let sealed_cell = development.sealed_cell.ok_or_else(|| {
        BorsukError::InvalidStorage("V23 incidence development did not seal a cell".to_string())
    })?;
    if canonical_development != development_bytes
        || development.schema != "borsuk-v23-incidence-development-v1"
        || development.claim_eligible
        || development.development.len() != 18
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence development artifact authority differs".to_string(),
        ));
    }
    let neighbors_input = role("neighbors-parquet")?;
    let neighbors_bytes = fs::read(&neighbors_input.path).map_err(|source| BorsukError::Io {
        path: neighbors_input.path.clone(),
        source,
    })?;
    let neighbors = read_v23_incidence_holdout_neighbors(&neighbors_bytes)?;
    let target_ids = v23_incidence_holdout_target_ids(&neighbors)?;
    let mut page_assignments = std::collections::BTreeMap::<u64, Vec<u32>>::new();
    for (ordinal, input) in request
        .input_paths
        .iter()
        .filter(|input| input.identity.role.starts_with("page-body-"))
        .enumerate()
    {
        let bytes = fs::read(&input.path).map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        let page = decode_v23_incidence_page(&input.identity, Bytes::from(bytes), ordinal)?;
        for row in 0..page.primary_rows() + page.replicated_rows() {
            let raw = page.record_id(row).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "V23 incidence holdout page record ID is absent".to_string(),
                )
            })?;
            let value = std::str::from_utf8(raw)
                .ok()
                .filter(|value| {
                    !value.is_empty()
                        && value.bytes().all(|byte| byte.is_ascii_digit())
                        && (*value == "0" || !value.starts_with('0'))
                })
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| value.to_string().as_bytes() == raw)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V23 incidence holdout page record ID differs".to_string(),
                    )
                })?;
            if target_ids.contains(&value) {
                let assignments = page_assignments.entry(value).or_default();
                let page_ordinal = page.page_ordinal();
                if assignments.contains(&page_ordinal) || assignments.len() == 2 {
                    return Err(BorsukError::InvalidStorage(
                        "V23 incidence holdout page assignment differs".to_string(),
                    ));
                }
                assignments.push(page_ordinal);
            }
        }
    }
    let truth = bind_v23_incidence_holdout_truth(&neighbors, &page_assignments, 28_282)?;
    let layout = recompute_v23_incidence_layout_quality(&truth)?;
    let roster_input = role("page-roster")?;
    let authority = V23IncidenceHoldoutTruthAuthority {
        development_result_sha256: development_input.identity.digest.clone(),
        neighbors_sha256: neighbors_input.identity.digest.clone(),
        page_roster_sha256: roster_input.identity.digest.clone(),
    };
    let artifact = V23IncidenceHoldoutTruthArtifact {
        schema: "borsuk-v23-incidence-holdout-truth-v1".to_string(),
        claim_eligible: false,
        authority: authority.clone(),
        sealed_cell,
        truth,
        layout,
    };
    let artifact_bytes =
        canonical_v23_incidence_holdout_truth_bytes(&artifact, &authority, sealed_cell)?;
    let mut probe = [0.0_f32; 96];
    probe[0] = 1.0;
    let kernel = measure_v23_incidence_tree_preflight(&[probe])?;
    if kernel.fma_backend != preflight.fma_backend {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence execution FMA backend differs".to_string(),
        ));
    }
    let (probes, network_namespace_inode) = parse_v23_incidence_sandbox_probes(sandbox_probes)?;
    validate_v23_incidence_execution_namespace(
        preflight.network_namespace_inode,
        network_namespace_inode,
    )?;
    let (output, output_path) = write_v23_incidence_local_output(
        "holdout-truth",
        "sha256",
        &artifact_bytes,
        &request.scratch_path,
        &request.output_path,
    )?;
    let receipt = V23IncidenceReceipt {
        schema: V23_INCIDENCE_RECEIPT_SCHEMA.to_string(),
        claim_eligible: false,
        phase: V23IncidencePhase::HoldoutBinding,
        run_mode: V23IncidenceReceiptRunMode::Execute,
        parent_receipt_sha256: Some(format!("{:x}", Sha256::digest(preflight_bytes))),
        executable_sha256: request.executable_sha256.clone(),
        fma_backend: kernel.fma_backend,
        network_namespace_inode,
        ordered_mounts: request
            .input_paths
            .iter()
            .map(|input| input.identity.clone())
            .collect(),
        probes,
        preflight_evidence: None,
        final_progress_sha256: None,
        outputs: vec![output],
        stop: None,
    };
    let result = canonical_v23_incidence_receipt_bytes(
        &receipt,
        Some(preflight_bytes),
        &[("holdout-truth", artifact_bytes.as_slice())],
    );
    if result.is_err() {
        let _ = fs::remove_file(output_path);
    }
    result
}

fn v23_incidence_holdout_target_ids(neighbors: &[(u32, Vec<u64>)]) -> Result<BTreeSet<u64>> {
    if neighbors.len() != 128
        || neighbors.iter().map(|entry| u64::from(entry.0)).ne(32..160)
        || neighbors.iter().any(|(_, ids)| {
            ids.len() != 100 || ids.iter().copied().collect::<BTreeSet<_>>().len() != 100
        })
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence holdout neighbor identities differ".to_string(),
        ));
    }
    Ok(neighbors
        .iter()
        .flat_map(|(_, ids)| ids.iter().copied())
        .collect())
}

fn run_v23_incidence_holdout_evaluation(
    request: &V23IncidenceLocalPhaseRequest,
    preflight: &V23IncidenceReceipt,
    preflight_bytes: &[u8],
    sandbox_probes: &str,
) -> Result<Vec<u8>> {
    authenticate_v23_incidence_request_inputs(&request.input_paths)?;
    let read = |role: &str| -> Result<(&V23IncidenceObjectIdentity, Vec<u8>)> {
        let input = request
            .input_paths
            .iter()
            .find(|input| input.identity.role == role)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "V23 incidence holdout evaluation {role} is absent"
                ))
            })?;
        let bytes = fs::read(&input.path).map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        Ok((&input.identity, bytes))
    };

    let (development_identity, development_bytes) = read("development-result")?;
    let (_, development_latency_bytes) = read("development-latency")?;
    let (tree_identity, tree_bytes) = read("incidence-tree")?;
    let (one_identity, one_bytes) = read("incidence-postings-one")?;
    let (two_identity, two_bytes) = read("incidence-postings-two")?;
    let (query_identity, query_bytes) = read("query-parquet")?;
    let (_, holdout_truth_bytes) = read("holdout-truth")?;

    let development: V23IncidenceDevelopmentArtifact = serde_json::from_slice(&development_bytes)
        .map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 incidence development artifact JSON differs: {error}"
        ))
    })?;
    let development_latencies =
        decode_v23_incidence_development_latency_bundle(&development_latency_bytes)?;
    let expected_development_authority = V23IncidenceDevelopmentAuthority {
        source_commit: V23_INCIDENCE_SOURCE_COMMIT.to_string(),
        source_archive_sha256: V23_INCIDENCE_SOURCE_ARCHIVE_SHA256.to_string(),
        index_id: V23_INCIDENCE_INDEX_ID.to_string(),
        dataset_id: V23_INCIDENCE_DATASET_ID.to_string(),
        query_cohort_sha256: query_identity.digest.clone(),
        tree_blake3: tree_identity.digest.clone(),
        posting_one_blake3: one_identity.digest.clone(),
        posting_two_blake3: two_identity.digest.clone(),
        executable_sha256: request.executable_sha256.clone(),
    };
    let expected_development_bytes = canonical_v23_incidence_development_artifact_bytes(
        &development,
        &expected_development_authority,
        &development_latencies,
        &development.development_truth,
    )?;
    if expected_development_bytes != development_bytes {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence development artifact bytes differ".to_string(),
        ));
    }
    let sealed_cell = development.sealed_cell.ok_or_else(|| {
        BorsukError::InvalidStorage("V23 incidence development cell is not sealed".to_string())
    })?;

    let holdout_truth: V23IncidenceHoldoutTruthArtifact =
        serde_json::from_slice(&holdout_truth_bytes).map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "V23 incidence holdout truth JSON differs: {error}"
            ))
        })?;
    if holdout_truth.authority.development_result_sha256 != development_identity.digest {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence holdout development binding differs".to_string(),
        ));
    }
    let expected_holdout_bytes = canonical_v23_incidence_holdout_truth_bytes(
        &holdout_truth,
        &holdout_truth.authority,
        sealed_cell,
    )?;
    if expected_holdout_bytes != holdout_truth_bytes {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence holdout truth bytes differ".to_string(),
        ));
    }

    let holdout_ordinals = (32_u64..160).collect::<Vec<_>>();
    let queries = read_v23_incidence_holdout_queries(&query_bytes, &holdout_ordinals)?;
    let tree = decode_incidence_tree(&tree_bytes)?;
    let one = decode_posting_plane(&one_bytes)?;
    let two = decode_posting_plane(&two_bytes)?;
    let plane = match sealed_cell.arm {
        PostingAssignmentArm::OneLeaf => &one,
        PostingAssignmentArm::TwoBeamLeaves => &two,
    };
    let mut workspace = V23IncidenceQueryWorkspace::new(28_282)?;
    let mut ordinal = 0_usize;
    let holdout_latency = measure_v23_incidence_latency(|| {
        let query = &queries[ordinal % queries.len()];
        ordinal += 1;
        score_incidence_query_native(&tree, plane, sealed_cell, query, &mut workspace).map(|_| ())
    })?;
    let evaluated = evaluate_v23_incidence_cell(
        &tree,
        plane,
        sealed_cell,
        &queries,
        &holdout_truth.truth,
        28_282,
        &holdout_latency,
    )?;
    let holdout = V23IncidenceHoldoutResult {
        cell: evaluated.cell,
        quality: evaluated.quality,
        projected_serving_bytes: evaluated.projected_serving_bytes,
        maximum_posting_visits: evaluated.maximum_posting_visits,
        maximum_touched_pages: evaluated.maximum_touched_pages,
        p99_ns: evaluated.p99_ns,
        determinism_passed: evaluated.determinism_passed,
        latency_blake3: evaluated.latency_blake3,
        latency_bytes: evaluated.latency_bytes,
        selections: evaluated.selections,
    };
    let campaign = V23IncidenceCampaignInput {
        authority_passed: true,
        resource_passed: true,
        determinism_passed: true,
        development: development.development.clone(),
        holdout_layout: holdout_truth.layout,
        holdout: Some(holdout),
    };
    let classification = classify_v23_incidence_campaign(&campaign);
    let result = V23IncidenceCampaignResult {
        schema: "borsuk-v23-incidence-result-v1".to_string(),
        claim_eligible: false,
        source_commit: V23_INCIDENCE_SOURCE_COMMIT.to_string(),
        source_archive_sha256: V23_INCIDENCE_SOURCE_ARCHIVE_SHA256.to_string(),
        index_id: V23_INCIDENCE_INDEX_ID.to_string(),
        dataset_id: V23_INCIDENCE_DATASET_ID.to_string(),
        query_cohort_sha256: query_identity.digest.clone(),
        tree_blake3: tree_identity.digest.clone(),
        posting_one_blake3: one_identity.digest.clone(),
        posting_two_blake3: two_identity.digest.clone(),
        executable_sha256: request.executable_sha256.clone(),
        campaign,
        sealed_cell: Some(sealed_cell),
        classification,
        page_body_reads: 0,
    };
    let mut latency_artifacts = development_latencies
        .iter()
        .map(Vec::as_slice)
        .collect::<Vec<_>>();
    latency_artifacts.push(holdout_latency.as_slice());
    let result_bytes = canonical_v23_incidence_result_bytes(
        &result,
        &latency_artifacts,
        &development.development_truth,
        &holdout_truth.truth,
    )?;

    let mut probe = [0.0_f32; 96];
    probe[0] = 1.0;
    let (_, fma_backend) = split_score_simd(
        tree.nodes.first().ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence holdout tree is empty".to_string())
        })?,
        &probe,
    )?;
    if fma_backend != preflight.fma_backend {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence execution FMA backend differs".to_string(),
        ));
    }
    let (probes, network_namespace_inode) = parse_v23_incidence_sandbox_probes(sandbox_probes)?;
    validate_v23_incidence_execution_namespace(
        preflight.network_namespace_inode,
        network_namespace_inode,
    )?;
    let (result_identity, result_path) = write_v23_incidence_local_output(
        "campaign-result",
        "sha256",
        &result_bytes,
        &request.scratch_path,
        &request.output_path,
    )?;
    let (latency_identity, latency_path) = match write_v23_incidence_local_output(
        "holdout-latency",
        "blake3",
        &holdout_latency,
        &request.scratch_path,
        &request.output_path,
    ) {
        Ok(output) => output,
        Err(error) => {
            let _ = fs::remove_file(&result_path);
            return Err(error);
        }
    };
    let receipt = V23IncidenceReceipt {
        schema: V23_INCIDENCE_RECEIPT_SCHEMA.to_string(),
        claim_eligible: false,
        phase: V23IncidencePhase::HoldoutEvaluation,
        run_mode: V23IncidenceReceiptRunMode::Execute,
        parent_receipt_sha256: Some(format!("{:x}", Sha256::digest(preflight_bytes))),
        executable_sha256: request.executable_sha256.clone(),
        fma_backend,
        network_namespace_inode,
        ordered_mounts: request
            .input_paths
            .iter()
            .map(|input| input.identity.clone())
            .collect(),
        probes,
        preflight_evidence: None,
        final_progress_sha256: None,
        outputs: vec![result_identity, latency_identity],
        stop: None,
    };
    let canonical = canonical_v23_incidence_receipt_bytes(
        &receipt,
        Some(preflight_bytes),
        &[
            ("campaign-result", result_bytes.as_slice()),
            ("holdout-latency", holdout_latency.as_slice()),
        ],
    );
    if canonical.is_err() {
        let _ = fs::remove_file(result_path);
        let _ = fs::remove_file(latency_path);
    }
    canonical
}

fn run_v23_incidence_local_phase_with_probes(
    request: V23IncidenceLocalPhaseRequest,
    sandbox_probes: &str,
) -> Result<Vec<u8>> {
    request.validate()?;
    let bytes = fs::read(&request.manifest_path).map_err(|source| BorsukError::Io {
        path: request.manifest_path.clone(),
        source,
    })?;
    let manifest: V23IncidenceManifest = serde_json::from_slice(&bytes).map_err(|error| {
        BorsukError::InvalidStorage(format!("V23 incidence manifest JSON differs: {error}"))
    })?;
    let manifest_role = if request.mode.phase() == V23IncidencePhase::TreeTraining {
        "construction-manifest"
    } else {
        "phase-manifest"
    };
    let manifest_identity = request
        .input_paths
        .iter()
        .find(|input| input.identity.role == manifest_role)
        .map(|input| &input.identity)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence manifest identity is absent".to_string())
        })?;
    if canonical_v23_incidence_manifest_bytes(&manifest)? != bytes {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence local manifest binding differs".to_string(),
        ));
    }
    validate_object_bytes(manifest_identity, &bytes)?;
    let binding = validate_v23_incidence_request_manifest(&request, &manifest, manifest_identity)?;
    if request.mode.phase() != V23IncidencePhase::TreeTraining {
        let parent_path = request.parent_receipt_path.as_deref().ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence parent receipt is absent".to_string())
        })?;
        let parent_bytes = fs::read(parent_path).map_err(|source| BorsukError::Io {
            path: parent_path.to_path_buf(),
            source,
        })?;
        validate_v23_incidence_parent_receipt(
            request.mode.phase(),
            &manifest,
            &parent_bytes,
            &request.executable_sha256,
        )?;
    }
    let execution_preflight = if request.mode.is_execute() {
        Some(validate_v23_incidence_request_execution_preflight(
            &request,
            &manifest,
            &binding,
            manifest_identity,
        )?)
    } else {
        None
    };
    match request.mode {
        V23IncidenceRunMode::Preflight(V23IncidencePhase::TreeTraining) => {
            run_v23_incidence_tree_preflight(&request, binding, sandbox_probes)
        }
        V23IncidenceRunMode::Preflight(V23IncidencePhase::PostingConstruction) => {
            run_v23_incidence_posting_preflight(&request, binding, sandbox_probes)
        }
        V23IncidenceRunMode::Preflight(V23IncidencePhase::HoldoutBinding) => {
            run_v23_incidence_holdout_binding_preflight(&request, binding, sandbox_probes)
        }
        V23IncidenceRunMode::Preflight(
            phase @ (V23IncidencePhase::DevelopmentEvaluation
            | V23IncidencePhase::HoldoutEvaluation),
        ) => run_v23_incidence_evaluation_preflight(&request, binding, sandbox_probes, phase),
        V23IncidenceRunMode::Execute(V23IncidencePhase::TreeTraining) => {
            let (preflight, preflight_bytes) = execution_preflight.as_ref().unwrap();
            run_v23_incidence_tree_training(
                &request,
                &manifest,
                preflight,
                preflight_bytes,
                sandbox_probes,
            )
        }
        V23IncidenceRunMode::Execute(V23IncidencePhase::PostingConstruction) => {
            let (preflight, preflight_bytes) = execution_preflight.as_ref().unwrap();
            run_v23_incidence_posting_build(&request, preflight, preflight_bytes, sandbox_probes)
        }
        V23IncidenceRunMode::Execute(V23IncidencePhase::DevelopmentEvaluation) => {
            let (preflight, preflight_bytes) = execution_preflight.as_ref().unwrap();
            run_v23_incidence_development_evaluation(
                &request,
                preflight,
                preflight_bytes,
                sandbox_probes,
            )
        }
        V23IncidenceRunMode::Execute(V23IncidencePhase::HoldoutBinding) => {
            let (preflight, preflight_bytes) = execution_preflight.as_ref().unwrap();
            run_v23_incidence_holdout_binding(&request, preflight, preflight_bytes, sandbox_probes)
        }
        V23IncidenceRunMode::Execute(V23IncidencePhase::HoldoutEvaluation) => {
            let (preflight, preflight_bytes) = execution_preflight.as_ref().unwrap();
            run_v23_incidence_holdout_evaluation(
                &request,
                preflight,
                preflight_bytes,
                sandbox_probes,
            )
        }
    }
}

/// Runs one authenticated local-only V23 incidence preflight or phase.
pub fn run_v23_incidence_local_phase(request: V23IncidenceLocalPhaseRequest) -> Result<Vec<u8>> {
    let sandbox_probes = std::env::var("BORSUK_V23_INCIDENCE_SANDBOX_PROBES").map_err(|_| {
        BorsukError::InvalidStorage("V23 incidence sandbox probes are absent".to_string())
    })?;
    run_v23_incidence_local_phase_with_probes(request, &sandbox_probes)
}

/// Runs one bounded directory-backed, authenticated local-only incidence phase.
pub fn run_v23_incidence_local_directory_phase(
    request: V23IncidenceLocalDirectoryPhaseRequest,
) -> Result<Vec<u8>> {
    let request = expand_v23_incidence_local_directory_request(request)?;
    run_v23_incidence_local_phase(request)
}

pub(crate) fn validate_v23_incidence_identity(
    observed: &V23IncidenceObjectIdentity,
    registered: &V23IncidenceObjectIdentity,
) -> Result<()> {
    validate_object_identity(observed)?;
    validate_object_identity(registered)?;
    if observed != registered {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence registered object identity differs".to_string(),
        ));
    }
    Ok(())
}

fn validate_identity_list(identities: &[V23IncidenceObjectIdentity]) -> Result<()> {
    let mut roles = BTreeSet::new();
    let mut uris = BTreeSet::new();
    for identity in identities {
        validate_object_identity(identity)?;
        if !roles.insert(identity.role.as_str()) || !uris.insert(identity.uri.as_str()) {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence object identities are duplicated".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_receipt(receipt: &V23IncidenceReceipt) -> Result<()> {
    let parent_is_valid = match (receipt.phase, receipt.run_mode) {
        (V23IncidencePhase::TreeTraining, V23IncidenceReceiptRunMode::Preflight) => {
            receipt.parent_receipt_sha256.is_none()
        }
        _ => receipt
            .parent_receipt_sha256
            .as_deref()
            .is_some_and(|digest| valid_lower_hex(digest, 64)),
    };
    let result_shape_is_valid = match receipt.run_mode {
        V23IncidenceReceiptRunMode::Preflight => {
            receipt.outputs.is_empty()
                && receipt.preflight_evidence.as_ref().is_some_and(|evidence| {
                    evidence.phase == receipt.phase
                        && receipt.stop
                            == evidence
                                .resource_stop
                                .then_some(V23IncidenceStopClass::Resource)
                })
        }
        V23IncidenceReceiptRunMode::Execute => {
            receipt.preflight_evidence.is_none()
                && match receipt.stop {
                    Some(_) => receipt.outputs.is_empty(),
                    None => !receipt.outputs.is_empty(),
                }
        }
    };
    let progress_shape_is_valid = match (receipt.phase, receipt.run_mode, receipt.stop) {
        (V23IncidencePhase::TreeTraining, V23IncidenceReceiptRunMode::Execute, None) => receipt
            .final_progress_sha256
            .as_deref()
            .is_some_and(|digest| valid_lower_hex(digest, 64)),
        _ => receipt.final_progress_sha256.is_none(),
    };
    if receipt.schema != V23_INCIDENCE_RECEIPT_SCHEMA
        || receipt.claim_eligible
        || !parent_is_valid
        || !valid_lower_hex(&receipt.executable_sha256, 64)
        || receipt.fma_backend == V23FmaBackend::ScalarControl
        || receipt.network_namespace_inode == 0
        || receipt.ordered_mounts.is_empty()
        || !receipt.probes.all_passed()
        || !result_shape_is_valid
        || !progress_shape_is_valid
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence receipt authority differs".to_string(),
        ));
    }
    validate_identity_list(&receipt.ordered_mounts)?;
    validate_identity_list(&receipt.outputs)?;
    if let Some(evidence) = &receipt.preflight_evidence {
        let authority = V23IncidencePreflightAuthority {
            parent_receipt_sha256: receipt.parent_receipt_sha256.clone(),
            executable_sha256: receipt.executable_sha256.clone(),
            fma_backend: receipt.fma_backend,
            network_namespace_inode: receipt.network_namespace_inode,
            probes: receipt.probes.clone(),
            full_input_bytes: evidence.full_input_bytes,
            ordered_inputs: receipt.ordered_mounts.clone(),
        };
        if project_v23_incidence_preflight(
            v23_incidence_preflight_work(receipt.phase),
            authority,
            evidence.measurement,
        )? != *evidence
        {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence preflight evidence differs".to_string(),
            ));
        }
    }
    let mounted_roles = receipt
        .ordered_mounts
        .iter()
        .map(|identity| identity.role.as_str())
        .collect::<BTreeSet<_>>();
    let mounted_uris = receipt
        .ordered_mounts
        .iter()
        .map(|identity| identity.uri.as_str())
        .collect::<BTreeSet<_>>();
    if receipt
        .outputs
        .iter()
        .any(|identity| mounted_roles.contains(identity.role.as_str()))
        || receipt
            .outputs
            .iter()
            .any(|identity| mounted_uris.contains(identity.uri.as_str()))
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence receipt inputs and outputs overlap".to_string(),
        ));
    }
    Ok(())
}

fn validate_manifest(manifest: &V23IncidenceManifest) -> Result<()> {
    let parent_is_valid = match manifest.phase {
        V23IncidencePhase::TreeTraining => manifest.parent_receipt_sha256.is_none(),
        _ => manifest
            .parent_receipt_sha256
            .as_deref()
            .is_some_and(|digest| valid_lower_hex(digest, 64)),
    };
    if manifest.schema != V23_INCIDENCE_MANIFEST_SCHEMA
        || manifest.claim_eligible
        || !parent_is_valid
        || manifest.source_commit != V23_INCIDENCE_SOURCE_COMMIT
        || manifest.source_archive_sha256 != V23_INCIDENCE_SOURCE_ARCHIVE_SHA256
        || manifest.index_id != V23_INCIDENCE_INDEX_ID
        || manifest.dataset_id != V23_INCIDENCE_DATASET_ID
        || manifest.algorithm != V23IncidenceAlgorithm::REGISTERED
        || manifest.ordered_inputs.is_empty()
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence manifest authority differs".to_string(),
        ));
    }
    validate_manifest_inputs(manifest)
}

fn validate_manifest_inputs(manifest: &V23IncidenceManifest) -> Result<()> {
    if manifest.phase != V23IncidencePhase::TreeTraining {
        return validate_phase_manifest_inputs(manifest);
    }
    if manifest.ordered_inputs.len() < 2 {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence manifest inputs differ".to_string(),
        ));
    }
    let V23IncidenceInputAuthority::DatasetMeta {
        identity,
        physical_schema,
        dimensions,
        metric,
        train_rows,
        test_rows,
        neighbors_per_query,
    } = &manifest.ordered_inputs[0]
    else {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence manifest input order differs".to_string(),
        ));
    };
    validate_object_identity(identity)?;
    if identity.role != "dataset-meta"
        || physical_schema != "deep-image-meta-v1"
        || *dimensions != 96
        || metric != "cosine"
        || *train_rows != 9_990_000
        || *test_rows != 10_000
        || *neighbors_per_query != 100
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence dataset authority differs".to_string(),
        ));
    }

    let mut identities = Vec::with_capacity(manifest.ordered_inputs.len());
    identities.push(identity.clone());
    let mut expected_start = 0_u64;
    for (index, input) in manifest.ordered_inputs[1..].iter().enumerate() {
        let V23IncidenceInputAuthority::TrainingShard {
            identity,
            ordinal_start,
            ordinal_end,
            physical_schema,
            dimensions,
            metric,
            rows,
        } = input
        else {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence training authority differs".to_string(),
            ));
        };
        let expected_role = format!("training-shard-{index:04}");
        if identity.role != expected_role
            || *ordinal_start != expected_start
            || *ordinal_end <= *ordinal_start
            || *rows != ordinal_end - ordinal_start
            || physical_schema != "emb:fixed-size-list<element:f32;96>:non-null"
            || *dimensions != 96
            || metric != "cosine"
        {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence training authority differs".to_string(),
            ));
        }
        validate_object_identity(identity)?;
        expected_start = *ordinal_end;
        identities.push(identity.clone());
    }
    if expected_start != *train_rows {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence training ordinal authority differs".to_string(),
        ));
    }
    validate_identity_list(&identities)
}

fn phase_manifest_roles(phase: V23IncidencePhase) -> Vec<String> {
    match phase {
        V23IncidencePhase::TreeTraining => Vec::new(),
        V23IncidencePhase::PostingConstruction => {
            let mut roles = vec![
                "parent-receipt".to_string(),
                "incidence-tree".to_string(),
                "page-roster".to_string(),
            ];
            roles.extend((0..28_282).map(|ordinal| format!("page-body-{ordinal:05}")));
            roles
        }
        V23IncidencePhase::DevelopmentEvaluation => [
            "parent-receipt",
            "incidence-tree",
            "incidence-postings-one",
            "incidence-postings-two",
            "d2-report",
            "query-parquet",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        V23IncidencePhase::HoldoutBinding => {
            let mut roles = vec![
                "parent-receipt".to_string(),
                "development-result".to_string(),
                "page-roster".to_string(),
                "neighbors-parquet".to_string(),
            ];
            roles.extend((0..28_282).map(|ordinal| format!("page-body-{ordinal:05}")));
            roles
        }
        V23IncidencePhase::HoldoutEvaluation => [
            "parent-receipt",
            "development-result",
            "development-latency",
            "incidence-tree",
            "incidence-postings-one",
            "incidence-postings-two",
            "query-parquet",
            "holdout-truth",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
    }
}

fn validate_phase_manifest_inputs(manifest: &V23IncidenceManifest) -> Result<()> {
    let expected_roles = phase_manifest_roles(manifest.phase);
    if expected_roles.is_empty() || manifest.ordered_inputs.len() != expected_roles.len() {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence phase manifest inputs differ".to_string(),
        ));
    }
    let mut identities = Vec::with_capacity(manifest.ordered_inputs.len());
    for (input, expected_role) in manifest.ordered_inputs.iter().zip(expected_roles) {
        let V23IncidenceInputAuthority::PhaseObject { identity } = input else {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence phase manifest input shape differs".to_string(),
            ));
        };
        if identity.role != expected_role {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence phase manifest input order differs".to_string(),
            ));
        }
        identities.push(identity.clone());
    }
    if identities[0].digest_algorithm != "sha256"
        || manifest.parent_receipt_sha256.as_deref() != Some(identities[0].digest.as_str())
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence phase manifest parent differs".to_string(),
        ));
    }
    validate_identity_list(&identities)
}

pub(crate) fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        serde_json::Value::Object(values) => {
            let mut entries = values.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json_value(value)))
                    .collect(),
            )
        }
        scalar => scalar,
    }
}

fn validate_object_bytes(identity: &V23IncidenceObjectIdentity, bytes: &[u8]) -> Result<()> {
    let digest = match identity.digest_algorithm.as_str() {
        "sha256" => format!("{:x}", Sha256::digest(bytes)),
        "blake3" => blake3::hash(bytes).to_hex().to_string(),
        _ => {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence digest algorithm differs".to_string(),
            ));
        }
    };
    if identity.encoded_bytes != bytes.len() as u64 || identity.digest != digest {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence object bytes differ".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn canonical_v23_incidence_receipt_bytes(
    receipt: &V23IncidenceReceipt,
    parent_receipt_bytes: Option<&[u8]>,
    output_bytes: &[(&str, &[u8])],
) -> Result<Vec<u8>> {
    validate_receipt(receipt)?;
    match (
        receipt.parent_receipt_sha256.as_deref(),
        parent_receipt_bytes,
    ) {
        (None, None) => {}
        (Some(expected), Some(bytes)) if format!("{:x}", Sha256::digest(bytes)) == expected => {}
        _ => {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence parent receipt bytes differ".to_string(),
            ));
        }
    }
    if receipt.outputs.len() != output_bytes.len() {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence output count differs".to_string(),
        ));
    }
    for (identity, (role, bytes)) in receipt.outputs.iter().zip(output_bytes) {
        if identity.role != *role {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence output order differs".to_string(),
            ));
        }
        validate_object_bytes(identity, bytes)?;
    }
    let value = serde_json::to_value(receipt).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 incidence receipt serialization failed: {error}"
        ))
    })?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
        BorsukError::InvalidStorage(format!("V23 incidence canonical JSON failed: {error}"))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn canonical_v23_incidence_receipt_path_bytes(
    receipt: &V23IncidenceReceipt,
    parent_receipt_bytes: Option<&[u8]>,
    output_paths: &[(&str, &Path)],
) -> Result<Vec<u8>> {
    validate_receipt(receipt)?;
    match (
        receipt.parent_receipt_sha256.as_deref(),
        parent_receipt_bytes,
    ) {
        (None, None) => {}
        (Some(expected), Some(bytes)) if format!("{:x}", Sha256::digest(bytes)) == expected => {}
        _ => {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence parent receipt bytes differ".to_string(),
            ));
        }
    }
    if receipt.outputs.len() != output_paths.len() {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence output count differs".to_string(),
        ));
    }
    for (identity, (role, path)) in receipt.outputs.iter().zip(output_paths) {
        if identity.role != *role {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence output order differs".to_string(),
            ));
        }
        authenticate_v23_incidence_local_path(path, identity)?;
    }
    let value = serde_json::to_value(receipt).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 incidence receipt serialization failed: {error}"
        ))
    })?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
        BorsukError::InvalidStorage(format!("V23 incidence canonical JSON failed: {error}"))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn canonical_v23_incidence_manifest_bytes(
    manifest: &V23IncidenceManifest,
) -> Result<Vec<u8>> {
    validate_manifest(manifest)?;
    let value = serde_json::to_value(manifest).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 incidence manifest serialization failed: {error}"
        ))
    })?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
        BorsukError::InvalidStorage(format!("V23 incidence canonical JSON failed: {error}"))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn canonical_v23_incidence_progress_bytes(
    progress: &V23IncidenceProgress,
    previous_bytes: Option<&[u8]>,
) -> Result<Vec<u8>> {
    if progress.total_units == 0
        || progress.completed_units > progress.total_units
        || !valid_lower_hex(&progress.last_object_digest, 64)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence progress authority differs".to_string(),
        ));
    }
    match (progress.sequence, previous_bytes) {
        (0, None)
            if progress.completed_units == 0 && progress.previous_progress_sha256.is_none() => {}
        (0, _) => {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence progress root differs".to_string(),
            ));
        }
        (_, Some(previous_bytes)) => {
            let previous: V23IncidenceProgress =
                serde_json::from_slice(previous_bytes).map_err(|error| {
                    BorsukError::InvalidStorage(format!(
                        "V23 incidence prior progress JSON differs: {error}"
                    ))
                })?;
            let previous_value = serde_json::to_value(&previous).map_err(|error| {
                BorsukError::InvalidStorage(format!(
                    "V23 incidence prior progress serialization failed: {error}"
                ))
            })?;
            let mut previous_canonical = serde_json::to_vec(&canonical_json_value(previous_value))
                .map_err(|error| {
                    BorsukError::InvalidStorage(format!(
                        "V23 incidence prior progress canonical JSON failed: {error}"
                    ))
                })?;
            previous_canonical.push(b'\n');
            if previous_canonical != previous_bytes
                || previous.phase != progress.phase
                || previous.total_units != progress.total_units
                || progress.sequence != previous.sequence + 1
                || progress.completed_units <= previous.completed_units
                || progress.previous_progress_sha256.as_deref()
                    != Some(format!("{:x}", Sha256::digest(previous_bytes)).as_str())
            {
                return Err(BorsukError::InvalidStorage(
                    "V23 incidence progress chain differs".to_string(),
                ));
            }
        }
        (_, None) => {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence prior progress is absent".to_string(),
            ));
        }
    }
    let value = serde_json::to_value(progress).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 incidence progress serialization failed: {error}"
        ))
    })?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 incidence progress canonical JSON failed: {error}"
        ))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn write_v23_incidence_progress_snapshot(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        BorsukError::InvalidStorage("V23 incidence progress parent is absent".to_string())
    })?;
    if !path.is_absolute()
        || path.file_name().and_then(|name| name.to_str()) != Some("progress.json")
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence progress path differs".to_string(),
        ));
    }
    let temporary = path.with_extension("json.tmp");
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|source| BorsukError::Io {
                path: temporary.clone(),
                source,
            })?;
        file.write_all(bytes).map_err(|source| BorsukError::Io {
            path: temporary.clone(),
            source,
        })?;
        file.sync_all().map_err(|source| BorsukError::Io {
            path: temporary.clone(),
            source,
        })?;
        fs::rename(&temporary, path).map_err(|source| BorsukError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| BorsukError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        Ok(())
    })();
    if result.is_err() && temporary.exists() {
        let _ = fs::remove_file(&temporary);
    }
    result?;
    Ok(())
}

#[cfg(test)]
fn write_v23_incidence_progress(
    path: &Path,
    progress: &V23IncidenceProgress,
    previous_bytes: Option<&[u8]>,
) -> Result<String> {
    let bytes = canonical_v23_incidence_progress_bytes(progress, previous_bytes)?;
    write_v23_incidence_progress_snapshot(path, &bytes)?;
    Ok(format!("{:x}", Sha256::digest(&bytes)))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, fs, io::Write, path::Path, process::Command, sync::Arc};

    use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use half::f16;
    use parquet::arrow::ArrowWriter;
    use sha2::{Digest, Sha256};

    use crate::{
        VectorMetric,
        v23_diagnostic::{V23PageInput, V23PageRow, V23QuantizerFamily, encode_v23_page},
        v23_incidence_postings::{
            PostingAssignmentArm, V23PostingRecord, build_posting_plane, decode_posting_plane,
            encode_posting_plane,
        },
        v23_incidence_tree::{
            V23IncidenceTrainingShape, V23IncidenceTree, V23TrainingRow, V23TrainingWork,
            V23TreeLeaf, V23TreeNode, assign_one_leaf, assign_two_beam_leaves,
            decode_incidence_tree, encode_incidence_tree, reservoir_seed,
            train_incidence_tree_test_shape,
        },
    };

    use super::{
        V23_INCIDENCE_DATASET_ID, V23_INCIDENCE_INDEX_ID, V23_INCIDENCE_RECEIPT_SCHEMA,
        V23_INCIDENCE_SOURCE_ARCHIVE_SHA256, V23_INCIDENCE_SOURCE_COMMIT, V23FmaBackend,
        V23IncidenceAlgorithm, V23IncidenceCampaignInput, V23IncidenceCampaignResult,
        V23IncidenceCapabilityProbes, V23IncidenceCell, V23IncidenceDevelopmentArtifact,
        V23IncidenceDevelopmentAuthority, V23IncidenceHoldoutResult,
        V23IncidenceHoldoutTruthArtifact, V23IncidenceHoldoutTruthAuthority,
        V23IncidenceInputAuthority, V23IncidenceLocalPhaseRequest, V23IncidenceLocalRolePath,
        V23IncidenceManifest, V23IncidenceObjectIdentity, V23IncidencePhase,
        V23IncidencePreflightAuthority, V23IncidencePreflightMeasurement, V23IncidenceReceipt,
        V23IncidenceReceiptRunMode, V23IncidenceRunMode, authenticate_v23_incidence_local_path,
        canonical_json_value, canonical_v23_incidence_development_artifact_bytes,
        canonical_v23_incidence_holdout_truth_bytes, canonical_v23_incidence_manifest_bytes,
        canonical_v23_incidence_preflight_bytes, canonical_v23_incidence_progress_bytes,
        canonical_v23_incidence_receipt_bytes, canonical_v23_incidence_receipt_path_bytes,
        canonical_v23_incidence_result_bytes, classify_v23_incidence_campaign,
        decode_v23_incidence_development_latency_bundle,
        encode_v23_incidence_development_latency_bundle,
        measure_v23_incidence_posting_pages_preflight,
        measure_v23_incidence_posting_sort_preflight, measure_v23_incidence_tree_preflight,
        parse_v23_incidence_sandbox_probes, phase_manifest_roles, project_v23_incidence_preflight,
        read_v23_incidence_preflight_receipt, read_v23_incidence_training_preflight_rows,
        recompute_v23_incidence_layout_quality, run_v23_incidence_holdout_evaluation,
        run_v23_incidence_local_phase_with_probes, v23_incidence_page_posting_stream,
        v23_incidence_preflight_work, v23_incidence_training_row_stream,
        validate_v23_incidence_execution_preflight, validate_v23_incidence_identity,
        validate_v23_incidence_parent_receipt, validate_v23_incidence_request_manifest,
        write_v23_incidence_local_output, write_v23_incidence_progress,
    };

    #[test]
    fn v23_incidence_progress_is_canonical_atomic_and_hash_chained() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("progress.json");
        let root = super::V23IncidenceProgress {
            completed_units: 0,
            last_object_digest: "11".repeat(32),
            phase: V23IncidencePhase::TreeTraining,
            previous_progress_sha256: None,
            sequence: 0,
            total_units: 128,
        };
        let root_bytes = canonical_v23_incidence_progress_bytes(&root, None).unwrap();
        let root_digest = write_v23_incidence_progress(&path, &root, None).unwrap();
        assert_eq!(root_digest, format!("{:x}", Sha256::digest(&root_bytes)));
        assert_eq!(fs::read(&path).unwrap(), root_bytes);
        assert!(!path.with_extension("json.tmp").exists());

        let advanced = super::V23IncidenceProgress {
            completed_units: 64,
            last_object_digest: "22".repeat(32),
            phase: V23IncidencePhase::TreeTraining,
            previous_progress_sha256: Some(root_digest),
            sequence: 1,
            total_units: 128,
        };
        let advanced_digest =
            write_v23_incidence_progress(&path, &advanced, Some(&root_bytes)).unwrap();
        assert_eq!(
            advanced_digest,
            format!("{:x}", Sha256::digest(fs::read(&path).unwrap()))
        );

        for changed in [
            super::V23IncidenceProgress {
                completed_units: 64,
                sequence: 2,
                previous_progress_sha256: Some(advanced_digest.clone()),
                ..advanced.clone()
            },
            super::V23IncidenceProgress {
                completed_units: 65,
                sequence: 2,
                previous_progress_sha256: Some("33".repeat(32)),
                ..advanced.clone()
            },
            super::V23IncidenceProgress {
                completed_units: 65,
                sequence: 2,
                total_units: 129,
                previous_progress_sha256: Some(advanced_digest.clone()),
                ..advanced.clone()
            },
        ] {
            assert!(
                canonical_v23_incidence_progress_bytes(&changed, Some(&fs::read(&path).unwrap()),)
                    .is_err()
            );
        }
    }

    #[test]
    fn v23_incidence_tree_progress_terminal_receipt_binds_final_record() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("progress.json");
        let mut progress = super::V23IncidenceProgressChain::start(
            &path,
            V23IncidencePhase::TreeTraining,
            3,
            &"11".repeat(32),
        )
        .unwrap();
        assert!(
            super::V23IncidenceProgressChain::start(
                &path,
                V23IncidencePhase::TreeTraining,
                3,
                &"11".repeat(32),
            )
            .is_err()
        );
        progress.advance(1, &"22".repeat(32)).unwrap();
        progress.advance(2, &"33".repeat(32)).unwrap();
        let tree_digest = blake3::hash(b"tree-output").to_hex().to_string();
        let final_progress_sha256 = progress.advance(3, &tree_digest).unwrap();

        assert_eq!(progress.records().len(), 4);
        assert_eq!(
            fs::read(&path).unwrap(),
            progress.records().concat(),
            "the atomic snapshot must retain every hash-chain record",
        );
        assert_eq!(
            final_progress_sha256,
            format!("{:x}", Sha256::digest(fs::read(&path).unwrap()))
        );

        let mut receipt = receipt_fixture();
        receipt.final_progress_sha256 = Some(final_progress_sha256);
        assert!(canonical_receipt(&receipt).is_ok());

        receipt.final_progress_sha256 = None;
        assert!(canonical_receipt(&receipt).is_err());
        receipt.final_progress_sha256 = Some("44".repeat(31));
        assert!(canonical_receipt(&receipt).is_err());
    }

    #[test]
    fn v23_incidence_tree_progress_authenticates_shards_incrementally() {
        let directory = tempfile::tempdir().unwrap();
        let first = directory.path().join("first.parquet");
        let second = directory.path().join("second.parquet");
        fs::write(&first, b"first-shard").unwrap();
        fs::write(&second, b"second-shard").unwrap();
        let inputs = vec![
            V23IncidenceLocalRolePath {
                identity: V23IncidenceObjectIdentity {
                    role: "training-shard-0000".to_string(),
                    uri: "file:///authority/training-shard-0000".to_string(),
                    digest_algorithm: "sha256".to_string(),
                    digest: format!("{:x}", Sha256::digest(b"first-shard")),
                    encoded_bytes: 11,
                    generation: "generation-0001".to_string(),
                },
                path: first,
            },
            V23IncidenceLocalRolePath {
                identity: V23IncidenceObjectIdentity {
                    role: "training-shard-0001".to_string(),
                    uri: "file:///authority/training-shard-0001".to_string(),
                    digest_algorithm: "sha256".to_string(),
                    digest: "55".repeat(32),
                    encoded_bytes: 12,
                    generation: "generation-0001".to_string(),
                },
                path: second,
            },
        ];
        let mut authenticated = Vec::new();

        assert!(
            super::authenticate_v23_incidence_tree_inputs_with_progress(&inputs, |identity| {
                authenticated.push(identity.role.clone());
                Ok(())
            })
            .is_err()
        );
        assert_eq!(authenticated, vec!["training-shard-0000"]);
    }

    fn object(role: &str, algorithm: &str, digest: &str) -> V23IncidenceObjectIdentity {
        V23IncidenceObjectIdentity {
            role: role.to_string(),
            uri: format!("file:///authority/{role}"),
            digest_algorithm: algorithm.to_string(),
            digest: digest.to_string(),
            encoded_bytes: 17,
            generation: "generation-0001".to_string(),
        }
    }

    fn posting_preflight_authority() -> V23IncidencePreflightAuthority {
        let parent_digest = format!("{:x}", Sha256::digest(b"parent-receipt"));
        let mut identities = vec![
            object("phase-manifest", "sha256", &"6f".repeat(32)),
            object("parent-receipt", "sha256", &parent_digest),
            object("incidence-tree", "blake3", &"72".repeat(32)),
            object("page-roster", "sha256", &"73".repeat(32)),
            object("page-body-0000", "blake3", &"74".repeat(32)),
        ];
        for identity in &mut identities {
            identity.encoded_bytes = 800_000_000;
        }
        V23IncidencePreflightAuthority {
            parent_receipt_sha256: Some(parent_digest),
            executable_sha256: "70".repeat(32),
            fma_backend: V23FmaBackend::Aarch64NeonFma,
            network_namespace_inode: 91,
            probes: V23IncidenceCapabilityProbes {
                network_namespace_changed: true,
                host_canary_denied: true,
                network_canary_denied: true,
                allowlisted_inputs_opened: true,
                output_writable: true,
            },
            full_input_bytes: 4_000_000_000,
            ordered_inputs: identities,
        }
    }

    #[test]
    fn v23_incidence_manifest_registered_training_fixture_is_exact() {
        let bytes =
            include_bytes!("../../../scripts/fixtures/v23_incidence_training_manifest.json");
        let manifest: V23IncidenceManifest = serde_json::from_slice(bytes).unwrap();
        assert_eq!(
            canonical_v23_incidence_manifest_bytes(&manifest).unwrap(),
            bytes
        );
        assert_eq!(manifest.ordered_inputs.len(), 59);
        let rows = manifest
            .ordered_inputs
            .iter()
            .filter_map(|input| match input {
                V23IncidenceInputAuthority::TrainingShard { rows, .. } => Some(*rows),
                _ => None,
            })
            .sum::<u64>();
        assert_eq!(rows, 9_990_000);
        assert_eq!(
            manifest
                .ordered_inputs
                .iter()
                .map(V23IncidenceInputAuthority::identity)
                .map(|identity| identity.encoded_bytes)
                .sum::<u64>(),
            3_839_147_293,
        );
    }

    fn evaluation_preflight_authority(_phase: V23IncidencePhase) -> V23IncidencePreflightAuthority {
        let parent_digest = format!("{:x}", Sha256::digest(b"parent-receipt"));
        let mut identities = vec![
            object("phase-manifest", "sha256", &"7a".repeat(32)),
            object("parent-receipt", "sha256", &parent_digest),
            object("incidence-tree", "blake3", &"7b".repeat(32)),
            object("incidence-postings-one", "blake3", &"7c".repeat(32)),
            object("incidence-postings-two", "blake3", &"7d".repeat(32)),
        ];
        for identity in &mut identities {
            identity.encoded_bytes = 1_000_000;
        }
        V23IncidencePreflightAuthority {
            parent_receipt_sha256: Some(parent_digest),
            executable_sha256: "7e".repeat(32),
            fma_backend: V23FmaBackend::Aarch64NeonFma,
            network_namespace_inode: 91,
            probes: V23IncidenceCapabilityProbes {
                network_namespace_changed: true,
                host_canary_denied: true,
                network_canary_denied: true,
                allowlisted_inputs_opened: true,
                output_writable: true,
            },
            full_input_bytes: 5_000_000,
            ordered_inputs: identities,
        }
    }

    fn receipt_fixture() -> V23IncidenceReceipt {
        let parent_receipt_sha256 = format!("{:x}", Sha256::digest(b"preflight-receipt"));
        let tree_digest = blake3::hash(b"tree-output").to_hex().to_string();
        let progress_digest = format!("{:x}", Sha256::digest(b"progress-record"));
        V23IncidenceReceipt {
            schema: V23_INCIDENCE_RECEIPT_SCHEMA.to_string(),
            claim_eligible: false,
            phase: V23IncidencePhase::TreeTraining,
            run_mode: V23IncidenceReceiptRunMode::Execute,
            parent_receipt_sha256: Some(parent_receipt_sha256),
            executable_sha256: "11".repeat(32),
            fma_backend: V23FmaBackend::Aarch64NeonFma,
            network_namespace_inode: 91,
            ordered_mounts: vec![object("construction-manifest", "sha256", &"22".repeat(32))],
            probes: V23IncidenceCapabilityProbes {
                network_namespace_changed: true,
                host_canary_denied: true,
                network_canary_denied: true,
                allowlisted_inputs_opened: true,
                output_writable: true,
            },
            preflight_evidence: None,
            final_progress_sha256: Some(progress_digest.clone()),
            outputs: vec![V23IncidenceObjectIdentity {
                encoded_bytes: 11,
                ..object("incidence-tree", "blake3", &tree_digest)
            }],
            stop: None,
        }
    }

    fn canonical_receipt(receipt: &V23IncidenceReceipt) -> crate::Result<Vec<u8>> {
        let parent_bytes = receipt
            .parent_receipt_sha256
            .as_ref()
            .map(|_| b"preflight-receipt".as_slice());
        let outputs = if receipt.outputs.is_empty() {
            Vec::new()
        } else {
            vec![("incidence-tree", b"tree-output".as_slice())]
        };
        canonical_v23_incidence_receipt_bytes(receipt, parent_bytes, &outputs)
    }

    #[test]
    fn v23_incidence_receipt_stream_authenticates_output_paths_after_rename() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("tree-output.bin");
        fs::write(&output, b"tree-output").unwrap();
        let receipt = receipt_fixture();
        let canonical = canonical_v23_incidence_receipt_path_bytes(
            &receipt,
            Some(b"preflight-receipt"),
            &[("incidence-tree", output.as_path())],
        )
        .unwrap();
        assert_eq!(canonical, canonical_receipt(&receipt).unwrap());

        fs::write(&output, b"tree-outpuu").unwrap();
        assert!(
            canonical_v23_incidence_receipt_path_bytes(
                &receipt,
                Some(b"preflight-receipt"),
                &[("incidence-tree", output.as_path())],
            )
            .is_err()
        );
    }

    fn manifest_fixture() -> V23IncidenceManifest {
        V23IncidenceManifest {
            schema: "borsuk-v23-incidence-manifest-v1".to_string(),
            claim_eligible: false,
            phase: V23IncidencePhase::TreeTraining,
            parent_receipt_sha256: None,
            source_commit: "c339a546f8f9370cb2e6e9fb3b0fd4bdefa3cb05".to_string(),
            source_archive_sha256:
                "77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d".to_string(),
            index_id: "index-bcda7bb66812e162d45077e6".to_string(),
            dataset_id: "deep-image-96".to_string(),
            algorithm: V23IncidenceAlgorithm {
                dimensions: 96,
                reservoir_rows: 2_097_152,
                tree_depth: 16,
                leaf_count: 65_536,
                lloyd_iterations: 4,
                posting_caps: [512, 1024, 2048],
                probe_counts: [32, 64, 128],
                selection_width: 8,
                aggregate_recall_ppm: 975_000,
                minimum_query_recall_ppm: 800_000,
                oracle_attainment_ppm: 995_000,
            },
            ordered_inputs: vec![
                V23IncidenceInputAuthority::DatasetMeta {
                    identity: object("dataset-meta", "sha256", &"88".repeat(32)),
                    physical_schema: "deep-image-meta-v1".to_string(),
                    dimensions: 96,
                    metric: "cosine".to_string(),
                    train_rows: 9_990_000,
                    test_rows: 10_000,
                    neighbors_per_query: 100,
                },
                V23IncidenceInputAuthority::TrainingShard {
                    identity: object("training-shard-0000", "sha256", &"89".repeat(32)),
                    ordinal_start: 0,
                    ordinal_end: 9_990_000,
                    physical_schema: "emb:fixed-size-list<element:f32;96>:non-null".to_string(),
                    dimensions: 96,
                    metric: "cosine".to_string(),
                    rows: 9_990_000,
                },
            ],
        }
    }

    fn local_directory_fixture(
        root: &Path,
    ) -> (
        super::V23IncidenceLocalDirectoryPhaseRequest,
        V23IncidenceManifest,
    ) {
        let mut manifest = manifest_fixture();
        let dataset_meta = b"dataset-meta\n";
        let training_shard = b"training-shard\n";
        for (input, bytes) in manifest
            .ordered_inputs
            .iter_mut()
            .zip([dataset_meta.as_slice(), training_shard.as_slice()])
        {
            let identity = match input {
                V23IncidenceInputAuthority::DatasetMeta { identity, .. }
                | V23IncidenceInputAuthority::TrainingShard { identity, .. } => identity,
                V23IncidenceInputAuthority::PhaseObject { .. } => unreachable!(),
            };
            identity.digest = format!("{:x}", Sha256::digest(bytes));
            identity.encoded_bytes = bytes.len() as u64;
        }

        let phase_manifest_bytes = canonical_v23_incidence_manifest_bytes(&manifest).unwrap();
        let phase_manifest_path = root.join("construction-manifest.json");
        fs::write(&phase_manifest_path, &phase_manifest_bytes).unwrap();
        let phase_manifest_identity = V23IncidenceObjectIdentity {
            role: "construction-manifest".to_string(),
            uri: "s3://borsuk-evidence/construction-manifest.json".to_string(),
            digest_algorithm: "sha256".to_string(),
            digest: format!("{:x}", Sha256::digest(&phase_manifest_bytes)),
            encoded_bytes: phase_manifest_bytes.len() as u64,
            generation: "generation-construction-manifest".to_string(),
        };

        let mut bulk_manifest = manifest.clone();
        bulk_manifest.ordered_inputs = vec![manifest.ordered_inputs[1].clone()];
        let value = serde_json::to_value(&bulk_manifest).unwrap();
        let mut bulk_manifest_bytes = serde_json::to_vec(&canonical_json_value(value)).unwrap();
        bulk_manifest_bytes.push(b'\n');
        let bulk_manifest_path = root.join("bulk-manifest.json");
        fs::write(&bulk_manifest_path, &bulk_manifest_bytes).unwrap();
        let bulk_manifest_identity = V23IncidenceObjectIdentity {
            role: "bulk-manifest".to_string(),
            uri: "s3://borsuk-evidence/tree-preflight-bulk-manifest.json".to_string(),
            digest_algorithm: "sha256".to_string(),
            digest: format!("{:x}", Sha256::digest(&bulk_manifest_bytes)),
            encoded_bytes: bulk_manifest_bytes.len() as u64,
            generation: "generation-bulk-manifest".to_string(),
        };

        let staging_directory_path = root.join("staged");
        fs::create_dir(&staging_directory_path).unwrap();
        fs::write(
            staging_directory_path.join("training-shard-0000"),
            training_shard,
        )
        .unwrap();
        let identity = manifest.ordered_inputs[1].identity();
        let receipt_value = serde_json::json!({
            "claim_eligible": false,
            "manifest_sha256": bulk_manifest_identity.digest,
            "ordered_objects": [{
                "digest": identity.digest,
                "digest_algorithm": identity.digest_algorithm,
                "encoded_bytes": identity.encoded_bytes,
                "generation": identity.generation,
                "relative_path": identity.role,
                "role": identity.role,
                "uri": identity.uri,
            }],
            "schema": "borsuk-v23-incidence-staging-receipt-v1",
        });
        let mut staging_receipt_bytes =
            serde_json::to_vec(&canonical_json_value(receipt_value)).unwrap();
        staging_receipt_bytes.push(b'\n');
        let staging_receipt_path = root.join("staging-receipt.json");
        fs::write(&staging_receipt_path, &staging_receipt_bytes).unwrap();
        let staging_receipt_identity = V23IncidenceObjectIdentity {
            role: "staging-receipt".to_string(),
            uri: "file:///authority/staging-receipt.json".to_string(),
            digest_algorithm: "sha256".to_string(),
            digest: format!("{:x}", Sha256::digest(&staging_receipt_bytes)),
            encoded_bytes: staging_receipt_bytes.len() as u64,
            generation: "generation-staging-receipt".to_string(),
        };

        (
            super::V23IncidenceLocalDirectoryPhaseRequest {
                mode: V23IncidenceRunMode::Preflight(V23IncidencePhase::TreeTraining),
                manifest: V23IncidenceLocalRolePath {
                    identity: phase_manifest_identity,
                    path: phase_manifest_path,
                },
                bulk_manifest: V23IncidenceLocalRolePath {
                    identity: bulk_manifest_identity,
                    path: bulk_manifest_path,
                },
                staging_directory_path,
                staging_receipt: V23IncidenceLocalRolePath {
                    identity: staging_receipt_identity,
                    path: staging_receipt_path,
                },
                preflight_receipt: None,
                scratch_path: root.join("scratch"),
                output_path: root.join("output.json"),
                executable_sha256: "95".repeat(32),
            },
            manifest,
        )
    }

    #[test]
    fn v23_incidence_local_directory_expands_only_the_registered_bulk_subset() {
        let directory = tempfile::tempdir().unwrap();
        let (request, manifest) = local_directory_fixture(directory.path());
        let expanded = super::expand_v23_incidence_local_directory_request(request).unwrap();
        assert_eq!(
            expanded
                .input_paths
                .iter()
                .map(|input| input.identity.role.as_str())
                .collect::<Vec<_>>(),
            [
                "construction-manifest",
                "bulk-manifest",
                "staging-receipt",
                "training-shard-0000",
            ]
        );
        assert_eq!(
            expanded.input_paths[3].identity,
            *manifest.ordered_inputs[1].identity()
        );
        assert!(expanded.parent_receipt_path.is_none());
        assert!(expanded.preflight_receipt_path.is_none());
    }

    #[test]
    fn v23_incidence_local_directory_rejects_unregistered_and_unsafe_entries() {
        let directory = tempfile::tempdir().unwrap();
        let (request, _) = local_directory_fixture(directory.path());

        fs::write(request.staging_directory_path.join("unexpected"), b"x").unwrap();
        assert!(super::expand_v23_incidence_local_directory_request(request.clone()).is_err());
        fs::remove_file(request.staging_directory_path.join("unexpected")).unwrap();

        fs::remove_file(request.staging_directory_path.join("training-shard-0000")).unwrap();
        assert!(super::expand_v23_incidence_local_directory_request(request).is_err());
    }

    #[test]
    fn v23_incidence_local_directory_preflight_measures_only_scientific_inputs() {
        let directory = tempfile::tempdir().unwrap();
        let (request, _) = local_directory_fixture(directory.path());
        let expanded = super::expand_v23_incidence_local_directory_request(request).unwrap();
        let measured =
            super::authenticate_v23_incidence_request_inputs(&expanded.input_paths).unwrap();
        let expected_bytes = expanded
            .input_paths
            .iter()
            .filter(|input| {
                !matches!(
                    input.identity.role.as_str(),
                    "bulk-manifest" | "staging-receipt"
                )
            })
            .map(|input| input.identity.encoded_bytes)
            .sum::<u64>();
        assert_eq!(measured.input_bytes, expected_bytes);
    }

    #[test]
    fn v23_incidence_local_directory_preflight_receipt_rejects_handoff_roles() {
        let work = v23_incidence_preflight_work(V23IncidencePhase::PostingConstruction);
        let mut authority = posting_preflight_authority();
        authority
            .ordered_inputs
            .push(object("bulk-manifest", "sha256", &"75".repeat(32)));
        authority.full_input_bytes += 17;
        let measurement = V23IncidencePreflightMeasurement {
            distance_dimensions: 1_000_000,
            distance_elapsed_ns: 1_000_000,
            input_bytes: 4_000_000_017,
            input_elapsed_ns: 1_000_000,
            records: 1_048_576,
            records_elapsed_ns: 1_000_000,
        };
        assert!(project_v23_incidence_preflight(work, authority, measurement).is_err());
    }

    fn posting_manifest_fixture() -> V23IncidenceManifest {
        let parent_digest = "91".repeat(32);
        let mut ordered_inputs = vec![
            V23IncidenceInputAuthority::PhaseObject {
                identity: object("parent-receipt", "sha256", &parent_digest),
            },
            V23IncidenceInputAuthority::PhaseObject {
                identity: object("incidence-tree", "blake3", &"92".repeat(32)),
            },
            V23IncidenceInputAuthority::PhaseObject {
                identity: object("page-roster", "sha256", &"93".repeat(32)),
            },
        ];
        ordered_inputs.extend(
            (0..28_282).map(|ordinal| V23IncidenceInputAuthority::PhaseObject {
                identity: object(
                    &format!("page-body-{ordinal:05}"),
                    "blake3",
                    &format!("{:064x}", ordinal + 1),
                ),
            }),
        );
        V23IncidenceManifest {
            schema: "borsuk-v23-incidence-manifest-v1".to_string(),
            claim_eligible: false,
            phase: V23IncidencePhase::PostingConstruction,
            parent_receipt_sha256: Some(parent_digest),
            source_commit: "c339a546f8f9370cb2e6e9fb3b0fd4bdefa3cb05".to_string(),
            source_archive_sha256:
                "77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d".to_string(),
            index_id: "index-bcda7bb66812e162d45077e6".to_string(),
            dataset_id: "deep-image-96".to_string(),
            algorithm: V23IncidenceAlgorithm::REGISTERED,
            ordered_inputs,
        }
    }

    fn holdout_binding_manifest_fixture() -> V23IncidenceManifest {
        let parent_digest = "a1".repeat(32);
        let mut ordered_inputs = vec![
            V23IncidenceInputAuthority::PhaseObject {
                identity: object("parent-receipt", "sha256", &parent_digest),
            },
            V23IncidenceInputAuthority::PhaseObject {
                identity: object("development-result", "sha256", &"a4".repeat(32)),
            },
            V23IncidenceInputAuthority::PhaseObject {
                identity: object("page-roster", "sha256", &"a2".repeat(32)),
            },
            V23IncidenceInputAuthority::PhaseObject {
                identity: object("neighbors-parquet", "sha256", &"a3".repeat(32)),
            },
        ];
        ordered_inputs.extend(
            (0..28_282).map(|ordinal| V23IncidenceInputAuthority::PhaseObject {
                identity: object(
                    &format!("page-body-{ordinal:05}"),
                    "blake3",
                    &format!("{:064x}", ordinal + 1),
                ),
            }),
        );
        V23IncidenceManifest {
            schema: "borsuk-v23-incidence-manifest-v1".to_string(),
            claim_eligible: false,
            phase: V23IncidencePhase::HoldoutBinding,
            parent_receipt_sha256: Some(parent_digest),
            source_commit: "c339a546f8f9370cb2e6e9fb3b0fd4bdefa3cb05".to_string(),
            source_archive_sha256:
                "77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d".to_string(),
            index_id: "index-bcda7bb66812e162d45077e6".to_string(),
            dataset_id: "deep-image-96".to_string(),
            algorithm: V23IncidenceAlgorithm::REGISTERED,
            ordered_inputs,
        }
    }

    #[test]
    fn v23_incidence_authority_rejects_role_digest_length_and_phase_drift() {
        let registered = object("construction-manifest", "sha256", &"44".repeat(32));
        assert!(validate_v23_incidence_identity(&registered, &registered).is_ok());

        let mut changed = registered.clone();
        changed.role = "query-parquet".to_string();
        assert!(validate_v23_incidence_identity(&changed, &registered).is_err());

        let mut changed = registered.clone();
        changed.digest = "45".repeat(32);
        assert!(validate_v23_incidence_identity(&changed, &registered).is_err());

        let mut changed = registered.clone();
        changed.encoded_bytes += 1;
        assert!(validate_v23_incidence_identity(&changed, &registered).is_err());

        let mut changed = registered.clone();
        changed.digest_algorithm = "blake3".to_string();
        assert!(validate_v23_incidence_identity(&changed, &registered).is_err());

        let wrong_registered_algorithm =
            object("construction-manifest", "blake3", &"46".repeat(32));
        assert!(
            validate_v23_incidence_identity(
                &wrong_registered_algorithm,
                &wrong_registered_algorithm,
            )
            .is_err()
        );

        let unknown_role = object("unregistered-role", "sha256", &"47".repeat(32));
        assert!(validate_v23_incidence_identity(&unknown_role, &unknown_role).is_err());
    }

    #[test]
    fn v23_incidence_authority_receipt_binds_capability_backend_parent_and_canonical_bytes() {
        let receipt = receipt_fixture();
        let bytes = canonical_receipt(&receipt).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);

        let mut changed = receipt.clone();
        changed.claim_eligible = true;
        assert!(canonical_receipt(&changed).is_err());

        let mut changed = receipt.clone();
        changed.fma_backend = V23FmaBackend::ScalarControl;
        assert!(canonical_receipt(&changed).is_err());

        let mut changed = receipt.clone();
        changed.probes.network_canary_denied = false;
        assert!(canonical_receipt(&changed).is_err());

        let mut changed = receipt;
        changed.parent_receipt_sha256 = None;
        assert!(canonical_receipt(&changed).is_err());

        let mut changed = receipt_fixture();
        changed.run_mode = V23IncidenceReceiptRunMode::Preflight;
        changed.parent_receipt_sha256 = None;
        changed.outputs.clear();
        assert!(canonical_receipt(&changed).is_err());

        changed
            .outputs
            .push(object("incidence-tree", "blake3", &"33".repeat(32)));
        assert!(canonical_receipt(&changed).is_err());

        let mut changed = receipt_fixture();
        changed.parent_receipt_sha256 = Some("56".repeat(32));
        assert!(canonical_receipt(&changed).is_err());

        let mut changed = receipt_fixture();
        changed.run_mode = V23IncidenceReceiptRunMode::Preflight;
        changed.parent_receipt_sha256 = Some("55".repeat(32));
        changed.outputs.clear();
        assert!(canonical_receipt(&changed).is_err());

        let mut changed = receipt_fixture();
        changed.outputs[0].role = changed.ordered_mounts[0].role.clone();
        assert!(canonical_receipt(&changed).is_err());

        let mut changed = receipt_fixture();
        changed.outputs[0].uri = changed.ordered_mounts[0].uri.clone();
        assert!(canonical_receipt(&changed).is_err());

        let receipt = receipt_fixture();
        assert!(
            canonical_v23_incidence_receipt_bytes(
                &receipt,
                Some(b"wrong-parent"),
                &[("incidence-tree", b"tree-output")],
            )
            .is_err()
        );
        assert!(
            canonical_v23_incidence_receipt_bytes(
                &receipt,
                Some(b"preflight-receipt"),
                &[("incidence-tree", b"wrong-tree")],
            )
            .is_err()
        );
    }

    #[test]
    fn v23_incidence_authority_manifest_binds_constants_inputs_and_canonical_bytes() {
        let manifest = manifest_fixture();
        let bytes = canonical_v23_incidence_manifest_bytes(&manifest).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));

        let mut changed = manifest.clone();
        changed.algorithm.posting_caps = [512, 1024, 1024];
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());

        let mut changed = manifest.clone();
        changed.source_commit = "66".repeat(20);
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());

        let mut changed = manifest.clone();
        changed.index_id = "index-drift".to_string();
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());

        let mut changed = manifest.clone();
        changed
            .ordered_inputs
            .push(changed.ordered_inputs[0].clone());
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());

        let mut changed = manifest.clone();
        let V23IncidenceInputAuthority::TrainingShard { dimensions, .. } =
            &mut changed.ordered_inputs[1]
        else {
            panic!("fixture input differs");
        };
        *dimensions = 95;
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());

        let mut changed = manifest.clone();
        let V23IncidenceInputAuthority::TrainingShard {
            ordinal_end, rows, ..
        } = &mut changed.ordered_inputs[1]
        else {
            panic!("fixture input differs");
        };
        *ordinal_end -= 1;
        *rows -= 1;
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());

        let mut changed = manifest.clone();
        changed.ordered_inputs.swap(0, 1);
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());

        let mut changed = manifest;
        changed.phase = V23IncidencePhase::HoldoutEvaluation;
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());
    }

    #[test]
    fn v23_incidence_authority_phase_manifest_binds_full_ordered_input_set() {
        let manifest = posting_manifest_fixture();
        let bytes = canonical_v23_incidence_manifest_bytes(&manifest).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));

        let mut changed = manifest.clone();
        changed.ordered_inputs.pop();
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());

        let mut changed = manifest.clone();
        let V23IncidenceInputAuthority::PhaseObject { identity } = &mut changed.ordered_inputs[0]
        else {
            panic!("posting manifest parent shape differs");
        };
        identity.digest = "94".repeat(32);
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());

        let mut changed = manifest;
        changed.ordered_inputs.swap(3, 4);
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());
    }

    #[test]
    fn v23_incidence_authority_parent_receipt_is_canonical_predecessor_and_binds_outputs() {
        let mut parent = receipt_fixture();
        let parent_bytes = canonical_receipt(&parent).unwrap();
        let mut manifest = posting_manifest_fixture();
        manifest.parent_receipt_sha256 = Some(format!("{:x}", Sha256::digest(&parent_bytes)));
        let V23IncidenceInputAuthority::PhaseObject {
            identity: parent_identity,
        } = &mut manifest.ordered_inputs[0]
        else {
            panic!("posting parent fixture differs");
        };
        parent_identity.digest = manifest.parent_receipt_sha256.clone().unwrap();
        parent_identity.encoded_bytes = parent_bytes.len() as u64;
        let V23IncidenceInputAuthority::PhaseObject {
            identity: tree_identity,
        } = &mut manifest.ordered_inputs[1]
        else {
            panic!("posting tree fixture differs");
        };
        *tree_identity = parent.outputs[0].clone();
        assert!(
            validate_v23_incidence_parent_receipt(
                V23IncidencePhase::PostingConstruction,
                &manifest,
                &parent_bytes,
                &parent.executable_sha256,
            )
            .is_ok()
        );

        parent.phase = V23IncidencePhase::PostingConstruction;
        parent.final_progress_sha256 = None;
        let changed_bytes = canonical_receipt(&parent).unwrap();
        manifest.parent_receipt_sha256 = Some(format!("{:x}", Sha256::digest(&changed_bytes)));
        let V23IncidenceInputAuthority::PhaseObject { identity } = &mut manifest.ordered_inputs[0]
        else {
            panic!("posting parent fixture differs");
        };
        identity.digest = manifest.parent_receipt_sha256.clone().unwrap();
        identity.encoded_bytes = changed_bytes.len() as u64;
        assert!(
            validate_v23_incidence_parent_receipt(
                V23IncidencePhase::PostingConstruction,
                &manifest,
                &changed_bytes,
                &parent.executable_sha256,
            )
            .is_err()
        );

        parent.phase = V23IncidencePhase::TreeTraining;
        parent.final_progress_sha256 = Some(format!("{:x}", Sha256::digest(b"progress-record")));
        let changed_output = b"different-tree";
        parent.outputs[0].digest = blake3::hash(changed_output).to_hex().to_string();
        parent.outputs[0].encoded_bytes = changed_output.len() as u64;
        let changed_bytes = canonical_v23_incidence_receipt_bytes(
            &parent,
            Some(b"preflight-receipt"),
            &[("incidence-tree", changed_output.as_slice())],
        )
        .unwrap();
        manifest.parent_receipt_sha256 = Some(format!("{:x}", Sha256::digest(&changed_bytes)));
        let V23IncidenceInputAuthority::PhaseObject { identity } = &mut manifest.ordered_inputs[0]
        else {
            panic!("posting parent fixture differs");
        };
        identity.digest = manifest.parent_receipt_sha256.clone().unwrap();
        identity.encoded_bytes = changed_bytes.len() as u64;
        assert!(
            validate_v23_incidence_parent_receipt(
                V23IncidencePhase::PostingConstruction,
                &manifest,
                &changed_bytes,
                &parent.executable_sha256,
            )
            .is_err()
        );
    }

    #[test]
    fn v23_incidence_preflight_work_and_projection_are_fixed_and_bounded() {
        let tree = v23_incidence_preflight_work(V23IncidencePhase::TreeTraining);
        assert_eq!(tree.sample_vectors, 65_536);
        assert_eq!(tree.full_distance_dimensions, 35_433_480_192);
        assert_eq!(tree.full_records, 0);

        let posting = v23_incidence_preflight_work(V23IncidencePhase::PostingConstruction);
        assert_eq!(posting.sample_page_bodies, 256);
        assert_eq!(posting.sample_records, 1_048_576);
        assert_eq!(posting.full_distance_dimensions, 168_027_881_664);
        assert_eq!(posting.full_records, 55_860_333);

        let evaluation = v23_incidence_preflight_work(V23IncidencePhase::DevelopmentEvaluation);
        assert_eq!(evaluation.sample_queries, 10_000);
        assert_eq!(evaluation.full_distance_dimensions, 1_252_050_075_648);
        assert_eq!(evaluation.full_records, 52_168_753_152);
        let holdout = v23_incidence_preflight_work(V23IncidencePhase::HoldoutEvaluation);
        assert_eq!(holdout.sample_queries, evaluation.sample_queries);
        assert_eq!(holdout.full_distance_dimensions, 70_162_317_312);
        assert_eq!(holdout.full_records, 2_923_429_888);

        let measurement = V23IncidencePreflightMeasurement {
            distance_dimensions: 1_000_000,
            distance_elapsed_ns: 1_000_000,
            input_bytes: 4_000_000_000,
            input_elapsed_ns: 1_000_000,
            records: 1_048_576,
            records_elapsed_ns: 1_000_000,
        };
        let projected =
            project_v23_incidence_preflight(posting, posting_preflight_authority(), measurement)
                .unwrap();
        assert_eq!(projected.distance_dimensions_per_second, 1_000_000_000);
        assert_eq!(projected.input_bytes_per_second, 4_000_000_000_000);
        assert_eq!(projected.records_per_second, 1_048_576_000);
        assert_eq!(projected.projected_wall_ns, 210_102_692_787);
        assert!(!projected.resource_stop);

        let mut changed = measurement;
        changed.input_bytes -= 1;
        assert!(
            project_v23_incidence_preflight(posting, posting_preflight_authority(), changed,)
                .is_err()
        );

        let evaluation_measurement = V23IncidencePreflightMeasurement {
            distance_dimensions: 10_000 * 65_536 * 96,
            distance_elapsed_ns: 2_000_000_000,
            input_bytes: 5_000_000,
            input_elapsed_ns: 1_000_000,
            records: 1_000_000,
            records_elapsed_ns: 1_000_000,
        };
        assert!(
            project_v23_incidence_preflight(
                evaluation,
                evaluation_preflight_authority(V23IncidencePhase::DevelopmentEvaluation),
                evaluation_measurement,
            )
            .is_ok()
        );
        for records in [0, evaluation.sample_records + 1] {
            assert!(
                project_v23_incidence_preflight(
                    evaluation,
                    evaluation_preflight_authority(V23IncidencePhase::DevelopmentEvaluation),
                    V23IncidencePreflightMeasurement {
                        records,
                        ..evaluation_measurement
                    },
                )
                .is_err()
            );
        }
        assert!(
            project_v23_incidence_preflight(
                evaluation,
                evaluation_preflight_authority(V23IncidencePhase::DevelopmentEvaluation),
                V23IncidencePreflightMeasurement {
                    distance_dimensions: evaluation_measurement.distance_dimensions - 96,
                    ..evaluation_measurement
                },
            )
            .is_err()
        );

        let mut changed = measurement;
        changed.records -= 1;
        assert!(
            project_v23_incidence_preflight(posting, posting_preflight_authority(), changed,)
                .is_err()
        );

        let slow = V23IncidencePreflightMeasurement {
            distance_elapsed_ns: 30_000_000,
            ..measurement
        };
        let projected =
            project_v23_incidence_preflight(posting, posting_preflight_authority(), slow).unwrap();
        assert!(projected.projected_wall_ns > 5_400_000_000_000);
        assert!(projected.resource_stop);
    }

    #[test]
    fn v23_incidence_preflight_receipt_is_canonical_and_recomputes_projection() {
        let work = v23_incidence_preflight_work(V23IncidencePhase::PostingConstruction);
        let measurement = V23IncidencePreflightMeasurement {
            distance_dimensions: 1_000_000,
            distance_elapsed_ns: 1_000_000,
            input_bytes: 4_000_000_000,
            input_elapsed_ns: 1_000_000,
            records: 1_048_576,
            records_elapsed_ns: 1_000_000,
        };
        let authority = posting_preflight_authority();
        let evidence =
            project_v23_incidence_preflight(work, authority.clone(), measurement).unwrap();
        let bytes =
            canonical_v23_incidence_preflight_bytes(&evidence, &authority, Some(b"parent-receipt"))
                .unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));

        let mut changed = evidence.clone();
        changed.projected_wall_ns += 1;
        assert!(
            canonical_v23_incidence_preflight_bytes(&changed, &authority, Some(b"parent-receipt"))
                .is_err()
        );

        let mut changed = evidence.clone();
        changed.full_records -= 1;
        assert!(
            canonical_v23_incidence_preflight_bytes(&changed, &authority, Some(b"parent-receipt"))
                .is_err()
        );

        let mut changed = evidence.clone();
        changed.distance_dimensions_per_second += 1;
        assert!(
            canonical_v23_incidence_preflight_bytes(&changed, &authority, Some(b"parent-receipt"))
                .is_err()
        );

        let mut changed = evidence;
        changed.resource_stop = true;
        assert!(
            canonical_v23_incidence_preflight_bytes(&changed, &authority, Some(b"parent-receipt"))
                .is_err()
        );

        let evidence =
            project_v23_incidence_preflight(work, authority.clone(), measurement).unwrap();
        let bytes =
            canonical_v23_incidence_preflight_bytes(&evidence, &authority, Some(b"parent-receipt"))
                .unwrap();
        let parsed =
            read_v23_incidence_preflight_receipt(&bytes, &authority, Some(b"parent-receipt"))
                .unwrap();
        assert_eq!(parsed.preflight_evidence, Some(evidence));

        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value["executable_sha256"] = serde_json::Value::String("75".repeat(32));
        let mut changed = serde_json::to_vec(&canonical_json_value(value)).unwrap();
        changed.push(b'\n');
        assert!(
            read_v23_incidence_preflight_receipt(&changed, &authority, Some(b"parent-receipt"))
                .is_err()
        );

        let identity = V23IncidenceObjectIdentity {
            role: "preflight-receipt".to_string(),
            uri: "file:///authority/preflight-receipt".to_string(),
            digest_algorithm: "sha256".to_string(),
            digest: format!("{:x}", Sha256::digest(&bytes)),
            encoded_bytes: bytes.len() as u64,
            generation: "generation-0001".to_string(),
        };
        assert!(
            validate_v23_incidence_execution_preflight(
                &bytes,
                &identity,
                &authority,
                Some(b"parent-receipt"),
            )
            .is_ok()
        );
        let stopped = project_v23_incidence_preflight(
            work,
            authority.clone(),
            V23IncidencePreflightMeasurement {
                distance_elapsed_ns: 30_000_000,
                ..measurement
            },
        )
        .unwrap();
        assert!(stopped.resource_stop);
        let stopped_bytes =
            canonical_v23_incidence_preflight_bytes(&stopped, &authority, Some(b"parent-receipt"))
                .unwrap();
        let stopped_identity = V23IncidenceObjectIdentity {
            digest: format!("{:x}", Sha256::digest(&stopped_bytes)),
            encoded_bytes: stopped_bytes.len() as u64,
            ..identity
        };
        assert!(
            validate_v23_incidence_execution_preflight(
                &stopped_bytes,
                &stopped_identity,
                &authority,
                Some(b"parent-receipt"),
            )
            .is_err()
        );
    }

    #[test]
    fn v23_incidence_preflight_manifest_binding_derives_full_bytes_and_exact_subset() {
        let manifest = posting_manifest_fixture();
        let manifest_bytes = canonical_v23_incidence_manifest_bytes(&manifest).unwrap();
        let manifest_identity = V23IncidenceObjectIdentity {
            role: "phase-manifest".to_string(),
            uri: "file:///authority/phase-manifest".to_string(),
            digest_algorithm: "sha256".to_string(),
            digest: format!("{:x}", Sha256::digest(&manifest_bytes)),
            encoded_bytes: manifest_bytes.len() as u64,
            generation: "generation-0001".to_string(),
        };
        let mut input_paths = vec![V23IncidenceLocalRolePath {
            identity: manifest_identity.clone(),
            path: "/inputs/phase-manifest".into(),
        }];
        input_paths.extend(manifest.ordered_inputs[..3 + 256].iter().map(|input| {
            V23IncidenceLocalRolePath {
                identity: input.identity().clone(),
                path: format!("/inputs/{}", input.identity().role).into(),
            }
        }));
        let request = V23IncidenceLocalPhaseRequest {
            mode: V23IncidenceRunMode::Preflight(V23IncidencePhase::PostingConstruction),
            manifest_path: "/inputs/phase-manifest".into(),
            parent_receipt_path: Some("/inputs/parent-receipt".into()),
            preflight_receipt_path: None,
            input_paths,
            scratch_path: "/scratch".into(),
            output_path: "/output/preflight.json".into(),
            executable_sha256: "95".repeat(32),
        };
        request.validate().unwrap();
        let authority =
            validate_v23_incidence_request_manifest(&request, &manifest, &manifest_identity)
                .unwrap();
        let expected_bytes = manifest_identity.encoded_bytes
            + manifest
                .ordered_inputs
                .iter()
                .map(|input| input.identity().encoded_bytes)
                .sum::<u64>();
        assert_eq!(authority.full_input_bytes, expected_bytes);
        assert_eq!(authority.ordered_inputs.len(), 1 + 3 + 256);

        let mut changed = request.clone();
        changed.input_paths.last_mut().unwrap().identity.digest = "96".repeat(32);
        assert!(
            validate_v23_incidence_request_manifest(&changed, &manifest, &manifest_identity)
                .is_err()
        );

        let mut changed = request;
        changed.input_paths.push(V23IncidenceLocalRolePath {
            identity: manifest.ordered_inputs[3 + 256].identity().clone(),
            path: format!(
                "/inputs/{}",
                manifest.ordered_inputs[3 + 256].identity().role
            )
            .into(),
        });
        assert!(
            validate_v23_incidence_request_manifest(&changed, &manifest, &manifest_identity)
                .is_err()
        );
    }

    #[test]
    fn v23_incidence_preflight_holdout_evaluation_mounts_router_not_sealed_outputs() {
        let roles = phase_manifest_roles(V23IncidencePhase::HoldoutEvaluation);
        let manifest = V23IncidenceManifest {
            schema: "borsuk-v23-incidence-manifest-v1".to_string(),
            claim_eligible: false,
            phase: V23IncidencePhase::HoldoutEvaluation,
            parent_receipt_sha256: Some("a1".repeat(32)),
            source_commit: V23_INCIDENCE_SOURCE_COMMIT.to_string(),
            source_archive_sha256: V23_INCIDENCE_SOURCE_ARCHIVE_SHA256.to_string(),
            index_id: V23_INCIDENCE_INDEX_ID.to_string(),
            dataset_id: V23_INCIDENCE_DATASET_ID.to_string(),
            algorithm: V23IncidenceAlgorithm::REGISTERED,
            ordered_inputs: roles
                .iter()
                .map(|role| V23IncidenceInputAuthority::PhaseObject {
                    identity: object(
                        role,
                        if matches!(
                            role.as_str(),
                            "development-latency"
                                | "incidence-tree"
                                | "incidence-postings-one"
                                | "incidence-postings-two"
                        ) {
                            "blake3"
                        } else {
                            "sha256"
                        },
                        &"a2".repeat(32),
                    ),
                })
                .collect(),
        };
        let selected = super::preflight_registered_inputs(&manifest).unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|identity| identity.role.as_str())
                .collect::<Vec<_>>(),
            [
                "parent-receipt",
                "incidence-tree",
                "incidence-postings-one",
                "incidence-postings-two",
            ]
        );
        assert!(
            selected
                .iter()
                .all(|identity| super::phase_preflight_role_is_allowed(
                    V23IncidencePhase::HoldoutEvaluation,
                    &identity.role,
                ))
        );
    }

    #[test]
    fn v23_incidence_manifest_phase_capabilities_carry_sealed_development_evidence() {
        let holdout_binding = phase_manifest_roles(V23IncidencePhase::HoldoutBinding);
        assert!(
            holdout_binding
                .iter()
                .any(|role| role == "development-result")
        );
        assert_eq!(
            holdout_binding
                .iter()
                .filter(|role| role.as_str() == "development-result")
                .count(),
            1
        );
        let holdout = phase_manifest_roles(V23IncidencePhase::HoldoutEvaluation);
        assert!(holdout.iter().any(|role| role == "development-result"));
        assert!(holdout.iter().any(|role| role == "development-latency"));
        assert_eq!(
            holdout
                .iter()
                .filter(|role| {
                    matches!(role.as_str(), "development-result" | "development-latency")
                })
                .count(),
            2
        );
    }

    #[test]
    fn v23_incidence_local_preflight_authenticates_streams_and_capability_probes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("input.bin");
        let bytes = vec![0x5a; 2 * 1024 * 1024 + 17];
        fs::write(&path, &bytes).unwrap();
        let identity = V23IncidenceObjectIdentity {
            role: "page-body-00000".to_string(),
            uri: "file:///authority/page-body-00000".to_string(),
            digest_algorithm: "blake3".to_string(),
            digest: blake3::hash(&bytes).to_hex().to_string(),
            encoded_bytes: bytes.len() as u64,
            generation: "generation-0001".to_string(),
        };
        let measured = authenticate_v23_incidence_local_path(&path, &identity).unwrap();
        assert_eq!(measured.input_bytes, bytes.len() as u64);
        assert!(measured.input_elapsed_ns > 0);

        let mut changed = identity.clone();
        changed.encoded_bytes += 1;
        assert!(authenticate_v23_incidence_local_path(&path, &changed).is_err());

        let raw = r#"{"allowlisted_inputs_opened":true,"host_canary_denied":true,"network_canary_denied":true,"network_namespace_changed":true,"network_namespace_inode":91,"output_writable":true}"#;
        let (probes, inode) = parse_v23_incidence_sandbox_probes(raw).unwrap();
        assert!(probes.all_passed());
        assert_eq!(inode, 91);
        assert!(parse_v23_incidence_sandbox_probes(&raw.replace("true", "false")).is_err());
    }

    #[test]
    fn v23_incidence_local_preflight_measures_the_fused_tree_kernel() {
        let rows = (0..256)
            .map(|ordinal| {
                let mut row = [0.0_f32; 96];
                row[ordinal % 96] = 1.0;
                row
            })
            .collect::<Vec<_>>();
        let measured = measure_v23_incidence_tree_preflight(&rows).unwrap();
        assert_eq!(measured.distance_dimensions, 256 * 2 * 96);
        assert!(measured.distance_elapsed_ns > 0);
        assert_ne!(measured.fma_backend, V23FmaBackend::ScalarControl);
        assert!(measure_v23_incidence_tree_preflight(&[]).is_err());
    }

    fn training_preflight_parquet(child_name: &str, row_count: usize) -> Vec<u8> {
        let child = Arc::new(Field::new(child_name, DataType::Float32, false));
        let field = Field::new("emb", DataType::FixedSizeList(child.clone(), 96), false);
        let values = (0..row_count * 96)
            .map(|index| if index % 96 == 0 { 1.0 } else { 0.0 })
            .collect::<Vec<_>>();
        let vectors =
            FixedSizeListArray::try_new(child, 96, Arc::new(Float32Array::from(values)), None)
                .unwrap();
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vectors)]).unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        bytes
    }

    #[test]
    fn v23_incidence_local_preflight_reads_exact_training_physical_schema() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("training.parquet");
        fs::write(&path, training_preflight_parquet("element", 16)).unwrap();
        let rows = read_v23_incidence_training_preflight_rows(&path, 16).unwrap();
        assert_eq!(rows.len(), 16);
        assert!(rows.iter().all(|row| row[0] == 1.0));

        fs::write(&path, training_preflight_parquet("item", 16)).unwrap();
        assert!(read_v23_incidence_training_preflight_rows(&path, 16).is_err());
        assert!(read_v23_incidence_training_preflight_rows(&path, 17).is_err());
    }

    #[test]
    fn v23_incidence_local_training_stream_is_batch_bounded_and_ordinal_exact() {
        let directory = tempfile::tempdir().unwrap();
        let first = directory.path().join("first.parquet");
        let second = directory.path().join("second.parquet");
        fs::write(&first, training_preflight_parquet("element", 8)).unwrap();
        fs::write(&second, training_preflight_parquet("element", 8)).unwrap();
        let rows = v23_incidence_training_row_stream(vec![(first.clone(), 0, 8), (second, 8, 16)])
            .unwrap()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows.len(), 16);
        assert_eq!(
            rows.iter()
                .map(|row| row.source_ordinal)
                .collect::<Vec<_>>(),
            (0..16).collect::<Vec<_>>(),
        );

        assert!(
            v23_incidence_training_row_stream(vec![(first, 0, 9)])
                .unwrap()
                .collect::<crate::Result<Vec<_>>>()
                .is_err()
        );
    }

    #[test]
    fn v23_incidence_local_output_is_content_addressed_atomic_and_scratch_clean() {
        let directory = tempfile::tempdir().unwrap();
        let scratch = directory.path().join("scratch");
        let output = directory.path().join("output");
        fs::create_dir(&scratch).unwrap();
        fs::create_dir(&output).unwrap();
        let bytes = b"tree-output";
        let (identity, path) = write_v23_incidence_local_output(
            "incidence-tree",
            "blake3",
            bytes,
            &scratch,
            &output.join("receipt.json"),
        )
        .unwrap();
        assert_eq!(identity.digest, blake3::hash(bytes).to_hex().to_string());
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(scratch.read_dir().unwrap().count(), 0);
        assert!(
            write_v23_incidence_local_output(
                "incidence-tree",
                "blake3",
                bytes,
                &scratch,
                &output.join("receipt.json"),
            )
            .is_err()
        );
    }

    #[test]
    fn v23_incidence_local_tree_preflight_runs_authenticated_live_path() {
        let directory = tempfile::tempdir().unwrap();
        let training_path = directory.path().join("training.parquet");
        let training_bytes = training_preflight_parquet("element", 65_536);
        fs::write(&training_path, &training_bytes).unwrap();

        let mut manifest = manifest_fixture();
        let V23IncidenceInputAuthority::TrainingShard { identity, .. } =
            &mut manifest.ordered_inputs[1]
        else {
            panic!("training fixture differs");
        };
        identity.digest = format!("{:x}", Sha256::digest(&training_bytes));
        identity.encoded_bytes = training_bytes.len() as u64;
        let manifest_bytes = canonical_v23_incidence_manifest_bytes(&manifest).unwrap();
        let manifest_path = directory.path().join("manifest.json");
        fs::write(&manifest_path, &manifest_bytes).unwrap();
        let manifest_identity = V23IncidenceObjectIdentity {
            role: "construction-manifest".to_string(),
            uri: "file:///authority/construction-manifest".to_string(),
            digest_algorithm: "sha256".to_string(),
            digest: format!("{:x}", Sha256::digest(&manifest_bytes)),
            encoded_bytes: manifest_bytes.len() as u64,
            generation: "generation-0001".to_string(),
        };
        let request = V23IncidenceLocalPhaseRequest {
            mode: V23IncidenceRunMode::Preflight(V23IncidencePhase::TreeTraining),
            manifest_path: manifest_path.clone(),
            parent_receipt_path: None,
            preflight_receipt_path: None,
            input_paths: vec![
                V23IncidenceLocalRolePath {
                    identity: manifest_identity,
                    path: manifest_path,
                },
                V23IncidenceLocalRolePath {
                    identity: manifest.ordered_inputs[1].identity().clone(),
                    path: training_path,
                },
            ],
            scratch_path: directory.path().join("scratch"),
            output_path: directory.path().join("preflight.json"),
            executable_sha256: "95".repeat(32),
        };
        let probes = r#"{"allowlisted_inputs_opened":true,"host_canary_denied":true,"network_canary_denied":true,"network_namespace_changed":true,"network_namespace_inode":92,"output_writable":true}"#;
        let receipt = run_v23_incidence_local_phase_with_probes(request, probes).unwrap();
        let parsed: V23IncidenceReceipt = serde_json::from_slice(&receipt).unwrap();
        let evidence = parsed.preflight_evidence.unwrap();
        assert_eq!(evidence.phase, V23IncidencePhase::TreeTraining);
        assert_eq!(evidence.measurement.distance_dimensions, 65_536 * 2 * 96);
        assert_eq!(
            evidence.measurement.input_bytes,
            training_bytes.len() as u64 + manifest_bytes.len() as u64
        );
        assert_eq!(parsed.run_mode, V23IncidenceReceiptRunMode::Preflight);
    }

    #[test]
    fn v23_incidence_local_tree_execute_requires_passing_preflight_before_training() {
        let directory = tempfile::tempdir().unwrap();
        fs::create_dir(directory.path().join("scratch")).unwrap();
        fs::create_dir(directory.path().join("output")).unwrap();
        let training_path = directory.path().join("training.parquet");
        let training_bytes = training_preflight_parquet("element", 16);
        fs::write(&training_path, &training_bytes).unwrap();
        let dataset_path = directory.path().join("meta.json");
        let dataset_bytes = b"{}\n";
        fs::write(&dataset_path, dataset_bytes).unwrap();
        let mut manifest = manifest_fixture();
        let V23IncidenceInputAuthority::DatasetMeta { identity, .. } =
            &mut manifest.ordered_inputs[0]
        else {
            panic!("dataset fixture differs");
        };
        identity.digest = format!("{:x}", Sha256::digest(dataset_bytes));
        identity.encoded_bytes = dataset_bytes.len() as u64;
        let V23IncidenceInputAuthority::TrainingShard { identity, .. } =
            &mut manifest.ordered_inputs[1]
        else {
            panic!("training fixture differs");
        };
        identity.digest = format!("{:x}", Sha256::digest(&training_bytes));
        identity.encoded_bytes = training_bytes.len() as u64;
        let manifest_bytes = canonical_v23_incidence_manifest_bytes(&manifest).unwrap();
        let manifest_path = directory.path().join("manifest.json");
        fs::write(&manifest_path, &manifest_bytes).unwrap();
        let manifest_identity = V23IncidenceObjectIdentity {
            role: "construction-manifest".to_string(),
            uri: "file:///authority/construction-manifest".to_string(),
            digest_algorithm: "sha256".to_string(),
            digest: format!("{:x}", Sha256::digest(&manifest_bytes)),
            encoded_bytes: manifest_bytes.len() as u64,
            generation: "generation-0001".to_string(),
        };
        let preflight_authority = V23IncidencePreflightAuthority {
            parent_receipt_sha256: None,
            executable_sha256: "95".repeat(32),
            fma_backend: V23FmaBackend::Aarch64NeonFma,
            network_namespace_inode: 91,
            probes: V23IncidenceCapabilityProbes {
                network_namespace_changed: true,
                host_canary_denied: true,
                network_canary_denied: true,
                allowlisted_inputs_opened: true,
                output_writable: true,
            },
            full_input_bytes: manifest_bytes.len() as u64
                + dataset_bytes.len() as u64
                + training_bytes.len() as u64,
            ordered_inputs: vec![
                manifest_identity.clone(),
                manifest.ordered_inputs[1].identity().clone(),
            ],
        };
        let passing_evidence = project_v23_incidence_preflight(
            v23_incidence_preflight_work(V23IncidencePhase::TreeTraining),
            preflight_authority.clone(),
            V23IncidencePreflightMeasurement {
                distance_dimensions: 65_536 * 2 * 96,
                distance_elapsed_ns: 1,
                input_bytes: manifest_bytes.len() as u64 + training_bytes.len() as u64,
                input_elapsed_ns: 1,
                records: 0,
                records_elapsed_ns: 0,
            },
        )
        .unwrap();
        assert!(!passing_evidence.resource_stop);
        let passing_preflight_bytes =
            canonical_v23_incidence_preflight_bytes(&passing_evidence, &preflight_authority, None)
                .unwrap();
        let preflight_path = directory.path().join("preflight.json");
        let request_for = |preflight_bytes: &[u8]| {
            fs::write(&preflight_path, preflight_bytes).unwrap();
            V23IncidenceLocalPhaseRequest {
                mode: V23IncidenceRunMode::Execute(V23IncidencePhase::TreeTraining),
                manifest_path: manifest_path.clone(),
                parent_receipt_path: None,
                preflight_receipt_path: Some(preflight_path.clone()),
                input_paths: vec![
                    V23IncidenceLocalRolePath {
                        identity: manifest_identity.clone(),
                        path: manifest_path.clone(),
                    },
                    V23IncidenceLocalRolePath {
                        identity: manifest.ordered_inputs[0].identity().clone(),
                        path: dataset_path.clone(),
                    },
                    V23IncidenceLocalRolePath {
                        identity: manifest.ordered_inputs[1].identity().clone(),
                        path: training_path.clone(),
                    },
                    V23IncidenceLocalRolePath {
                        identity: V23IncidenceObjectIdentity {
                            role: "preflight-receipt".to_string(),
                            uri: "file:///authority/preflight-receipt".to_string(),
                            digest_algorithm: "sha256".to_string(),
                            digest: format!("{:x}", Sha256::digest(preflight_bytes)),
                            encoded_bytes: preflight_bytes.len() as u64,
                            generation: "generation-0001".to_string(),
                        },
                        path: preflight_path.clone(),
                    },
                ],
                scratch_path: directory.path().join("scratch"),
                output_path: directory.path().join("output/receipt.json"),
                executable_sha256: "95".repeat(32),
            }
        };
        let probes = r#"{"allowlisted_inputs_opened":true,"host_canary_denied":true,"network_canary_denied":true,"network_namespace_changed":true,"network_namespace_inode":91,"output_writable":true}"#;
        let error = run_v23_incidence_local_phase_with_probes(
            request_for(&passing_preflight_bytes),
            probes,
        )
        .unwrap_err();
        assert!(error.to_string().contains("training shard schema differs"));

        let stopped_evidence = project_v23_incidence_preflight(
            v23_incidence_preflight_work(V23IncidencePhase::TreeTraining),
            preflight_authority.clone(),
            V23IncidencePreflightMeasurement {
                distance_dimensions: 65_536 * 2 * 96,
                distance_elapsed_ns: 10_000_000_000,
                input_bytes: manifest_bytes.len() as u64 + training_bytes.len() as u64,
                input_elapsed_ns: 1_000_000,
                records: 0,
                records_elapsed_ns: 0,
            },
        )
        .unwrap();
        assert!(stopped_evidence.resource_stop);
        let stopped_preflight_bytes =
            canonical_v23_incidence_preflight_bytes(&stopped_evidence, &preflight_authority, None)
                .unwrap();
        let error = run_v23_incidence_local_phase_with_probes(
            request_for(&stopped_preflight_bytes),
            probes,
        )
        .unwrap_err();
        assert!(error.to_string().contains("did not pass"));
    }

    #[test]
    fn v23_incidence_execution_accepts_recycled_preflight_namespace_inode() {
        assert!(super::validate_v23_incidence_execution_namespace(91, 91).is_ok());
        assert!(super::validate_v23_incidence_execution_namespace(91, 92).is_ok());
        assert!(super::validate_v23_incidence_execution_namespace(91, 0).is_err());
    }

    #[test]
    fn v23_incidence_holdout_targets_allow_duplicates_across_queries() {
        let neighbors = (32..160)
            .map(|query_ordinal| (query_ordinal, (0..100).collect::<Vec<u64>>()))
            .collect::<Vec<_>>();
        let target_ids = super::v23_incidence_holdout_target_ids(&neighbors).unwrap();
        assert_eq!(target_ids.len(), 100);
        assert_eq!(target_ids.first(), Some(&0));
        assert_eq!(target_ids.last(), Some(&99));
    }

    #[test]
    fn v23_incidence_local_posting_preflight_sorts_exact_registered_sample() {
        let directory = tempfile::tempdir().unwrap();
        let measured = measure_v23_incidence_posting_sort_preflight(directory.path()).unwrap();
        assert_eq!(measured.records, 1_048_576);
        assert!(measured.records_elapsed_ns > 0);
        assert_eq!(directory.path().read_dir().unwrap().count(), 0);
    }

    fn reduced_preflight_tree_bytes() -> Vec<u8> {
        let shape = V23IncidenceTrainingShape {
            dimensions: 96,
            reservoir_rows: 32,
            depth: 5,
            lloyd_iterations: 4,
        };
        let mut zero = [f16::ZERO; 96];
        zero[0] = f16::ONE;
        let mut one = [f16::ZERO; 96];
        one[1] = f16::ONE;
        let node_count = (1_usize << shape.depth) - 1;
        let nodes = (0..node_count)
            .map(|index| {
                let level = usize::BITS as usize - (index + 1).leading_zeros() as usize - 1;
                let level_start = (1_usize << level) - 1;
                let group = index - level_start;
                let (child_zero_index, child_one_index) = if level + 1 == shape.depth {
                    (node_count + group * 2, node_count + group * 2 + 1)
                } else {
                    let start = (1_usize << (level + 1)) - 1 + group * 2;
                    (start, start + 1)
                };
                V23TreeNode {
                    child_zero: zero,
                    child_one: one,
                    child_zero_inverse_norm: 1.0,
                    child_one_inverse_norm: 1.0,
                    boundary_score_bits: 0.0_f32.to_bits(),
                    boundary_source_ordinal: 0,
                    child_zero_index: child_zero_index as u32,
                    child_one_index: child_one_index as u32,
                }
            })
            .collect();
        let leaves = (0_u32..32)
            .map(|ordinal| V23TreeLeaf {
                centroid: if ordinal.is_multiple_of(2) { zero } else { one },
                inverse_norm: 1.0,
                population: 1,
                mean_squared_residual: 0.0,
            })
            .collect();
        let farthest_seed_dimensions = 32 * 5 * 96;
        let work = V23TrainingWork {
            farthest_seed_dimensions,
            lloyd_dimensions: farthest_seed_dimensions * 4 * 2,
            repartition_dimensions: farthest_seed_dimensions * 2,
            total_distance_dimensions: farthest_seed_dimensions * 11,
        };
        encode_incidence_tree(&V23IncidenceTree {
            shape,
            reservoir_seed: reservoir_seed(
                "77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d",
            )
            .unwrap(),
            work,
            nodes,
            leaves,
        })
        .unwrap()
    }

    fn posting_preflight_page(ordinal: u32) -> (V23IncidenceObjectIdentity, Bytes) {
        let mut code = Vec::with_capacity(192);
        for dimension in 0..96 {
            code.extend_from_slice(
                &if dimension == usize::try_from(ordinal).unwrap() % 2 {
                    f16::ONE
                } else {
                    f16::ZERO
                }
                .to_bits()
                .to_le_bytes(),
            );
        }
        let page = encode_v23_page(&V23PageInput {
            generation_checksum: [7; 32],
            page_ordinal: ordinal,
            metric: VectorMetric::Cosine,
            dimensions: 96,
            family: V23QuantizerFamily::F16Flat,
            code_width: 192,
            primary_rows: vec![V23PageRow {
                canonical_record_id: ordinal.to_string().into_bytes().into(),
                code: code.into(),
            }],
            replicated_rows: Vec::new(),
        })
        .unwrap();
        let digest = blake3::hash(&page).to_hex().to_string();
        (
            V23IncidenceObjectIdentity {
                role: format!("page-body-{ordinal:05}"),
                uri: format!("file:///authority/pages/{digest}"),
                digest_algorithm: "blake3".to_string(),
                digest,
                encoded_bytes: page.len() as u64,
                generation: "generation-0001".to_string(),
            },
            page,
        )
    }

    #[test]
    fn v23_incidence_local_posting_preflight_decodes_256_pages_and_assigns_both_arms() {
        let tree = reduced_preflight_tree_bytes();
        let pages = (0..256).map(posting_preflight_page).collect::<Vec<_>>();
        let measured = measure_v23_incidence_posting_pages_preflight(&tree, &pages).unwrap();
        assert_eq!(measured.distance_dimensions, 256 * (5 * 5 - 2) * 96);
        assert!(measured.distance_elapsed_ns > 0);
        assert_ne!(measured.fma_backend, V23FmaBackend::ScalarControl);

        let mut changed = pages;
        changed[7].0.role = "page-body-00008".to_string();
        assert!(measure_v23_incidence_posting_pages_preflight(&tree, &changed).is_err());
    }

    #[test]
    fn v23_incidence_local_posting_file_stream_decodes_once_for_both_arms() {
        let directory = tempfile::tempdir().unwrap();
        let tree = decode_incidence_tree(&reduced_preflight_tree_bytes()).unwrap();
        let pages = (0..4)
            .map(posting_preflight_page)
            .map(|(identity, bytes)| {
                let path = directory.path().join(&identity.role);
                fs::write(&path, bytes).unwrap();
                V23IncidenceLocalRolePath { identity, path }
            })
            .collect::<Vec<_>>();
        let records = v23_incidence_page_posting_stream(&tree, pages)
            .collect::<crate::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(records.len(), 4);
        assert_eq!(
            records
                .iter()
                .map(|records| records.one.page)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
        assert!(records.iter().all(|records| {
            records.two[0].page == records.one.page
                && records.two[1].page == records.one.page
                && records.one.reserved == 0
                && records.two.iter().all(|record| record.reserved == 0)
        }));
    }

    #[test]
    fn v23_incidence_local_holdout_execute_requires_every_sealed_input() {
        let directory = tempfile::tempdir().unwrap();
        let request = V23IncidenceLocalPhaseRequest {
            mode: V23IncidenceRunMode::Execute(V23IncidencePhase::HoldoutEvaluation),
            manifest_path: directory.path().join("manifest.json"),
            parent_receipt_path: Some(directory.path().join("parent.json")),
            preflight_receipt_path: Some(directory.path().join("preflight.json")),
            input_paths: Vec::new(),
            scratch_path: directory.path().join("scratch"),
            output_path: directory.path().join("output/receipt.json"),
            executable_sha256: "95".repeat(32),
        };
        let error = run_v23_incidence_holdout_evaluation(
            &request,
            &receipt_fixture(),
            b"preflight\n",
            r#"{"allowlisted_inputs_opened":true,"host_canary_denied":true,"network_canary_denied":true,"network_namespace_changed":true,"network_namespace_inode":92,"output_writable":true}"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("development-result is absent"));
    }

    #[test]
    fn v23_incidence_local_posting_preflight_runs_authenticated_live_path() {
        let directory = tempfile::tempdir().unwrap();
        let scratch = directory.path().join("scratch");
        fs::create_dir(&scratch).unwrap();
        let tree = reduced_preflight_tree_bytes();
        let mut parent_receipt = receipt_fixture();
        parent_receipt.executable_sha256 = "95".repeat(32);
        parent_receipt.outputs[0].digest = blake3::hash(&tree).to_hex().to_string();
        parent_receipt.outputs[0].encoded_bytes = tree.len() as u64;
        let parent = canonical_v23_incidence_receipt_bytes(
            &parent_receipt,
            Some(b"preflight-receipt"),
            &[("incidence-tree", tree.as_slice())],
        )
        .unwrap();
        let roster = b"{}\n".to_vec();
        let pages = (0..256).map(posting_preflight_page).collect::<Vec<_>>();

        let mut manifest = posting_manifest_fixture();
        manifest.parent_receipt_sha256 = Some(format!("{:x}", Sha256::digest(&parent)));
        let replacements = [
            (
                "parent-receipt",
                "sha256",
                format!("{:x}", Sha256::digest(&parent)),
                parent.len() as u64,
            ),
            (
                "incidence-tree",
                "blake3",
                blake3::hash(&tree).to_hex().to_string(),
                tree.len() as u64,
            ),
            (
                "page-roster",
                "sha256",
                format!("{:x}", Sha256::digest(&roster)),
                roster.len() as u64,
            ),
        ];
        for (input, (role, algorithm, digest, encoded_bytes)) in
            manifest.ordered_inputs[..3].iter_mut().zip(replacements)
        {
            let V23IncidenceInputAuthority::PhaseObject { identity } = input else {
                panic!("posting fixture differs");
            };
            identity.role = role.to_string();
            identity.digest_algorithm = algorithm.to_string();
            identity.digest = digest;
            identity.encoded_bytes = encoded_bytes;
        }
        for (input, (identity, _)) in manifest.ordered_inputs[3..3 + 256].iter_mut().zip(&pages) {
            let V23IncidenceInputAuthority::PhaseObject {
                identity: registered,
            } = input
            else {
                panic!("page fixture differs");
            };
            *registered = identity.clone();
        }
        let manifest_bytes = canonical_v23_incidence_manifest_bytes(&manifest).unwrap();
        let manifest_identity = V23IncidenceObjectIdentity {
            role: "phase-manifest".to_string(),
            uri: "file:///authority/phase-manifest".to_string(),
            digest_algorithm: "sha256".to_string(),
            digest: format!("{:x}", Sha256::digest(&manifest_bytes)),
            encoded_bytes: manifest_bytes.len() as u64,
            generation: "generation-0001".to_string(),
        };
        let manifest_path = directory.path().join("phase-manifest");
        fs::write(&manifest_path, &manifest_bytes).unwrap();
        let mut input_paths = vec![V23IncidenceLocalRolePath {
            identity: manifest_identity,
            path: manifest_path.clone(),
        }];
        for (identity, bytes) in [
            (
                manifest.ordered_inputs[0].identity().clone(),
                parent.as_slice(),
            ),
            (
                manifest.ordered_inputs[1].identity().clone(),
                tree.as_slice(),
            ),
            (
                manifest.ordered_inputs[2].identity().clone(),
                roster.as_slice(),
            ),
        ] {
            let path = directory.path().join(&identity.role);
            fs::write(&path, bytes).unwrap();
            input_paths.push(V23IncidenceLocalRolePath { identity, path });
        }
        for (identity, bytes) in &pages {
            let path = directory.path().join(&identity.role);
            fs::write(&path, bytes).unwrap();
            input_paths.push(V23IncidenceLocalRolePath {
                identity: identity.clone(),
                path,
            });
        }
        let mounted_bytes = input_paths
            .iter()
            .map(|input| input.identity.encoded_bytes)
            .sum::<u64>();
        let request = V23IncidenceLocalPhaseRequest {
            mode: V23IncidenceRunMode::Preflight(V23IncidencePhase::PostingConstruction),
            manifest_path,
            parent_receipt_path: Some(directory.path().join("parent-receipt")),
            preflight_receipt_path: None,
            input_paths,
            scratch_path: scratch,
            output_path: directory.path().join("preflight.json"),
            executable_sha256: "95".repeat(32),
        };
        let probes = r#"{"allowlisted_inputs_opened":true,"host_canary_denied":true,"network_canary_denied":true,"network_namespace_changed":true,"network_namespace_inode":91,"output_writable":true}"#;
        let receipt = run_v23_incidence_local_phase_with_probes(request, probes).unwrap();
        let parsed: V23IncidenceReceipt = serde_json::from_slice(&receipt).unwrap();
        let evidence = parsed.preflight_evidence.unwrap();
        assert_eq!(evidence.phase, V23IncidencePhase::PostingConstruction);
        assert_eq!(evidence.measurement.input_bytes, mounted_bytes);
        assert_eq!(evidence.measurement.records, 1_048_576);
        assert_eq!(
            evidence.measurement.distance_dimensions,
            256 * (5 * 5 - 2) * 96
        );
    }

    #[test]
    fn v23_incidence_local_holdout_binding_preflight_decodes_pages_without_neighbor_access() {
        let directory = tempfile::tempdir().unwrap();
        let scratch = directory.path().join("scratch");
        fs::create_dir(&scratch).unwrap();
        let development = b"development-result\n";
        let development_latency = b"development-latency\n";
        let mut parent_receipt = receipt_fixture();
        parent_receipt.phase = V23IncidencePhase::DevelopmentEvaluation;
        parent_receipt.final_progress_sha256 = None;
        parent_receipt.executable_sha256 = "95".repeat(32);
        parent_receipt.outputs = vec![
            V23IncidenceObjectIdentity {
                encoded_bytes: development.len() as u64,
                ..object(
                    "development-result",
                    "sha256",
                    &format!("{:x}", Sha256::digest(development)),
                )
            },
            V23IncidenceObjectIdentity {
                encoded_bytes: development_latency.len() as u64,
                ..object(
                    "development-latency",
                    "blake3",
                    blake3::hash(development_latency).to_hex().as_ref(),
                )
            },
        ];
        let parent = canonical_v23_incidence_receipt_bytes(
            &parent_receipt,
            Some(b"preflight-receipt"),
            &[
                ("development-result", development.as_slice()),
                ("development-latency", development_latency.as_slice()),
            ],
        )
        .unwrap();
        let roster = b"{}\n".to_vec();
        let pages = (0..256).map(posting_preflight_page).collect::<Vec<_>>();
        let mut manifest = holdout_binding_manifest_fixture();
        manifest.parent_receipt_sha256 = Some(format!("{:x}", Sha256::digest(&parent)));
        for (index, bytes, role) in [
            (0, parent.as_slice(), "parent-receipt"),
            (1, development.as_slice(), "development-result"),
            (2, roster.as_slice(), "page-roster"),
        ] {
            let V23IncidenceInputAuthority::PhaseObject { identity } =
                &mut manifest.ordered_inputs[index]
            else {
                panic!("holdout fixture differs");
            };
            identity.role = role.to_string();
            identity.digest = format!("{:x}", Sha256::digest(bytes));
            identity.encoded_bytes = bytes.len() as u64;
        }
        for (input, (identity, _)) in manifest.ordered_inputs[4..4 + 256].iter_mut().zip(&pages) {
            let V23IncidenceInputAuthority::PhaseObject {
                identity: registered,
            } = input
            else {
                panic!("page fixture differs");
            };
            *registered = identity.clone();
        }
        let manifest_bytes = canonical_v23_incidence_manifest_bytes(&manifest).unwrap();
        let manifest_identity = V23IncidenceObjectIdentity {
            role: "phase-manifest".to_string(),
            uri: "file:///authority/holdout-manifest".to_string(),
            digest_algorithm: "sha256".to_string(),
            digest: format!("{:x}", Sha256::digest(&manifest_bytes)),
            encoded_bytes: manifest_bytes.len() as u64,
            generation: "generation-0001".to_string(),
        };
        let manifest_path = directory.path().join("phase-manifest");
        fs::write(&manifest_path, &manifest_bytes).unwrap();
        let mut input_paths = vec![V23IncidenceLocalRolePath {
            identity: manifest_identity,
            path: manifest_path.clone(),
        }];
        for (identity, bytes) in [
            (
                manifest.ordered_inputs[0].identity().clone(),
                parent.as_slice(),
            ),
            (
                manifest.ordered_inputs[2].identity().clone(),
                roster.as_slice(),
            ),
        ] {
            let path = directory.path().join(&identity.role);
            fs::write(&path, bytes).unwrap();
            input_paths.push(V23IncidenceLocalRolePath { identity, path });
        }
        for (identity, bytes) in &pages {
            let path = directory.path().join(&identity.role);
            fs::write(&path, bytes).unwrap();
            input_paths.push(V23IncidenceLocalRolePath {
                identity: identity.clone(),
                path,
            });
        }
        assert!(
            input_paths
                .iter()
                .all(|input| input.identity.role != "neighbors-parquet")
        );
        let mounted_bytes = input_paths
            .iter()
            .map(|input| input.identity.encoded_bytes)
            .sum::<u64>();
        let request = V23IncidenceLocalPhaseRequest {
            mode: V23IncidenceRunMode::Preflight(V23IncidencePhase::HoldoutBinding),
            manifest_path,
            parent_receipt_path: Some(directory.path().join("parent-receipt")),
            preflight_receipt_path: None,
            input_paths,
            scratch_path: scratch,
            output_path: directory.path().join("preflight.json"),
            executable_sha256: "95".repeat(32),
        };
        let probes = r#"{"allowlisted_inputs_opened":true,"host_canary_denied":true,"network_canary_denied":true,"network_namespace_changed":true,"network_namespace_inode":91,"output_writable":true}"#;
        let receipt = run_v23_incidence_local_phase_with_probes(request, probes).unwrap();
        let parsed: V23IncidenceReceipt = serde_json::from_slice(&receipt).unwrap();
        let evidence = parsed.preflight_evidence.unwrap();
        assert_eq!(evidence.phase, V23IncidencePhase::HoldoutBinding);
        assert_eq!(evidence.measurement.input_bytes, mounted_bytes);
        assert_eq!(evidence.measurement.distance_dimensions, 0);
        assert_eq!(evidence.measurement.records, 0);
    }

    fn test_campaign_truth(
        first: u32,
        count: u32,
    ) -> Vec<crate::v23_incidence_eval::V23IncidenceQueryTruth> {
        (first..first + count)
            .map(
                |query_ordinal| crate::v23_incidence_eval::V23IncidenceQueryTruth {
                    query_ordinal,
                    ground_truth_page_assignments: (0..10).map(|index| vec![index % 8]).collect(),
                    oracle_pages: (0..8).collect(),
                },
            )
            .collect()
    }

    fn test_campaign_queries(first: u32, count: u32) -> Vec<[f32; 96]> {
        (first..first + count)
            .map(|query_ordinal| {
                std::array::from_fn(|dimension| {
                    let signed = (((u64::from(query_ordinal) + 8_192).wrapping_mul(131)
                        + dimension as u64 * 17)
                        % 257) as i32
                        - 128;
                    signed as f32 / 129.0
                })
            })
            .collect()
    }

    fn write_test_campaign_file(path: &Path, bytes: &[u8]) -> crate::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|source| crate::BorsukError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        file.write_all(bytes)
            .map_err(|source| crate::BorsukError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        file.sync_all().map_err(|source| crate::BorsukError::Io {
            path: path.to_path_buf(),
            source,
        })
    }

    fn write_test_campaign_receipt(
        root: &Path,
        phase: V23IncidencePhase,
        outputs: &[(&str, &str, &str)],
    ) -> crate::Result<()> {
        let (parent_name, parent_bytes) = match phase {
            V23IncidencePhase::TreeTraining => (None, b"test-root-preflight\n".to_vec()),
            V23IncidencePhase::PostingConstruction => (
                Some("tree-training-receipt.json"),
                fs::read(root.join("tree-training-receipt.json")).map_err(|source| {
                    crate::BorsukError::Io {
                        path: root.join("tree-training-receipt.json"),
                        source,
                    }
                })?,
            ),
            V23IncidencePhase::DevelopmentEvaluation => (
                Some("posting-construction-receipt.json"),
                fs::read(root.join("posting-construction-receipt.json")).map_err(|source| {
                    crate::BorsukError::Io {
                        path: root.join("posting-construction-receipt.json"),
                        source,
                    }
                })?,
            ),
            V23IncidencePhase::HoldoutBinding => (
                Some("development-evaluation-receipt.json"),
                fs::read(root.join("development-evaluation-receipt.json")).map_err(|source| {
                    crate::BorsukError::Io {
                        path: root.join("development-evaluation-receipt.json"),
                        source,
                    }
                })?,
            ),
            V23IncidencePhase::HoldoutEvaluation => (
                Some("holdout-binding-receipt.json"),
                fs::read(root.join("holdout-binding-receipt.json")).map_err(|source| {
                    crate::BorsukError::Io {
                        path: root.join("holdout-binding-receipt.json"),
                        source,
                    }
                })?,
            ),
        };
        let mut output_bytes = Vec::with_capacity(outputs.len());
        let mut output_identities = Vec::with_capacity(outputs.len());
        for (role, algorithm, name) in outputs {
            let bytes = fs::read(root.join(name)).map_err(|source| crate::BorsukError::Io {
                path: root.join(name),
                source,
            })?;
            let digest = if *algorithm == "sha256" {
                format!("{:x}", Sha256::digest(&bytes))
            } else {
                blake3::hash(&bytes).to_hex().to_string()
            };
            output_identities.push(V23IncidenceObjectIdentity {
                role: (*role).to_string(),
                uri: format!("file:///test-output/{name}"),
                digest_algorithm: (*algorithm).to_string(),
                digest,
                encoded_bytes: bytes.len() as u64,
                generation: "generation-test".to_string(),
            });
            output_bytes.push(((*role).to_string(), bytes));
        }
        let mount_role = if phase == V23IncidencePhase::TreeTraining {
            "construction-manifest"
        } else {
            "phase-manifest"
        };
        let phase_name = match phase {
            V23IncidencePhase::TreeTraining => "tree-training",
            V23IncidencePhase::PostingConstruction => "posting-construction",
            V23IncidencePhase::DevelopmentEvaluation => "development-evaluation",
            V23IncidencePhase::HoldoutBinding => "holdout-binding",
            V23IncidencePhase::HoldoutEvaluation => "holdout-evaluation",
        };
        let mount_digest = format!("{:x}", Sha256::digest(phase_name.as_bytes()));
        let receipt = V23IncidenceReceipt {
            schema: V23_INCIDENCE_RECEIPT_SCHEMA.to_string(),
            claim_eligible: false,
            phase,
            run_mode: V23IncidenceReceiptRunMode::Execute,
            parent_receipt_sha256: Some(format!("{:x}", Sha256::digest(&parent_bytes))),
            executable_sha256: "95".repeat(32),
            fma_backend: V23FmaBackend::Aarch64NeonFma,
            network_namespace_inode: 91,
            ordered_mounts: vec![V23IncidenceObjectIdentity {
                role: mount_role.to_string(),
                uri: format!("file:///test-input/{phase_name}/manifest"),
                digest_algorithm: "sha256".to_string(),
                digest: mount_digest,
                encoded_bytes: 1,
                generation: "generation-test".to_string(),
            }],
            probes: V23IncidenceCapabilityProbes {
                network_namespace_changed: true,
                host_canary_denied: true,
                network_canary_denied: true,
                allowlisted_inputs_opened: true,
                output_writable: true,
            },
            preflight_evidence: None,
            final_progress_sha256: (phase == V23IncidencePhase::TreeTraining)
                .then(|| format!("{:x}", Sha256::digest(b"test-tree-progress"))),
            outputs: output_identities,
            stop: None,
        };
        let output_refs = output_bytes
            .iter()
            .map(|(role, bytes)| (role.as_str(), bytes.as_slice()))
            .collect::<Vec<_>>();
        let bytes =
            canonical_v23_incidence_receipt_bytes(&receipt, Some(&parent_bytes), &output_refs)?;
        let name = format!("{phase_name}-receipt.json");
        let _ = parent_name;
        write_test_campaign_file(&root.join(name), &bytes)
    }

    fn run_v23_incidence_test_child_phase(root: &Path, phase: &str) -> crate::Result<()> {
        const EXECUTABLE: &str = "9595959595959595959595959595959595959595959595959595959595959595";
        match phase {
            "tree-training" => {
                let threads = std::env::var("RAYON_NUM_THREADS")
                    .ok()
                    .and_then(|value| value.parse::<usize>().ok())
                    .ok_or_else(|| {
                        crate::BorsukError::InvalidStorage(
                            "V23 incidence test thread count differs".to_string(),
                        )
                    })?;
                let rows = (0..4_096_u64)
                    .map(|source_ordinal| {
                        let mut vector = [0.0_f32; 96];
                        for (dimension, value) in vector.iter_mut().enumerate() {
                            let signed = ((source_ordinal.wrapping_mul(131)
                                + dimension as u64 * 17)
                                % 257) as i32
                                - 128;
                            *value = signed as f32 / 129.0;
                        }
                        V23TrainingRow {
                            source_ordinal,
                            vector,
                        }
                    })
                    .collect::<Vec<_>>();
                let tree = train_incidence_tree_test_shape(
                    &rows,
                    V23IncidenceTrainingShape {
                        dimensions: 96,
                        reservoir_rows: 4_096,
                        depth: 5,
                        lloyd_iterations: 4,
                    },
                    threads,
                    257,
                )?;
                let bytes = encode_incidence_tree(&tree)?;
                decode_incidence_tree(&bytes)?;
                write_test_campaign_file(&root.join("incidence-tree.bin"), &bytes)?;
                write_test_campaign_receipt(
                    root,
                    V23IncidencePhase::TreeTraining,
                    &[("incidence-tree", "blake3", "incidence-tree.bin")],
                )
            }
            "posting-construction" => {
                let tree_bytes = fs::read(root.join("incidence-tree.bin")).map_err(|source| {
                    crate::BorsukError::Io {
                        path: root.join("incidence-tree.bin"),
                        source,
                    }
                })?;
                let tree = decode_incidence_tree(&tree_bytes)?;
                let build = |arm, name: &str| -> crate::Result<Vec<u8>> {
                    let scratch = root.join(format!("{name}-scratch"));
                    fs::create_dir(&scratch).map_err(|source| crate::BorsukError::Io {
                        path: scratch.clone(),
                        source,
                    })?;
                    let mut records = Vec::new();
                    for source_ordinal in 0..4_096_u64 {
                        let mut vector = [0.0_f32; 96];
                        for (dimension, value) in vector.iter_mut().enumerate() {
                            let signed = ((source_ordinal.wrapping_mul(131)
                                + dimension as u64 * 17)
                                % 257) as i32
                                - 128;
                            *value = signed as f32 / 129.0;
                        }
                        let page = source_ordinal as u32 % 8;
                        let leaves = match arm {
                            PostingAssignmentArm::OneLeaf => {
                                vec![assign_one_leaf(&tree, &vector, source_ordinal)?]
                            }
                            PostingAssignmentArm::TwoBeamLeaves => {
                                assign_two_beam_leaves(&tree, &vector, source_ordinal)?
                                    .0
                                    .to_vec()
                            }
                        };
                        records.extend(leaves.into_iter().map(|leaf| {
                            Ok(V23PostingRecord {
                                leaf,
                                page,
                                reserved: 0,
                            })
                        }));
                    }
                    let plane = build_posting_plane(records, arm, &scratch, 257, 2_048)?;
                    let bytes = encode_posting_plane(&plane)?;
                    decode_posting_plane(&bytes)?;
                    fs::remove_dir(&scratch).map_err(|source| crate::BorsukError::Io {
                        path: scratch,
                        source,
                    })?;
                    Ok(bytes)
                };
                let one = build(PostingAssignmentArm::OneLeaf, "posting-one")?;
                let two = build(PostingAssignmentArm::TwoBeamLeaves, "posting-two")?;
                write_test_campaign_file(&root.join("postings-one.bin"), &one)?;
                write_test_campaign_file(&root.join("postings-two.bin"), &two)?;
                write_test_campaign_receipt(
                    root,
                    V23IncidencePhase::PostingConstruction,
                    &[
                        ("incidence-postings-one", "blake3", "postings-one.bin"),
                        ("incidence-postings-two", "blake3", "postings-two.bin"),
                    ],
                )
            }
            "development-evaluation" => {
                let tree_bytes = fs::read(root.join("incidence-tree.bin")).map_err(|source| {
                    crate::BorsukError::Io {
                        path: root.join("incidence-tree.bin"),
                        source,
                    }
                })?;
                let tree = decode_incidence_tree(&tree_bytes)?;
                let one = fs::read(root.join("postings-one.bin")).map_err(|source| {
                    crate::BorsukError::Io {
                        path: root.join("postings-one.bin"),
                        source,
                    }
                })?;
                let two = fs::read(root.join("postings-two.bin")).map_err(|source| {
                    crate::BorsukError::Io {
                        path: root.join("postings-two.bin"),
                        source,
                    }
                })?;
                let one_plane = decode_posting_plane(&one)?;
                let two_plane = decode_posting_plane(&two)?;
                let latency = crate::v23_incidence_eval::encode_v23_incidence_latency_samples(
                    &vec![1_000_000; 10_000],
                )?;
                let latencies = vec![latency; 18];
                let queries = test_campaign_queries(0, 32);
                let truth = test_campaign_truth(0, 32);
                let development = V23IncidenceCell::registered_ladder()
                    .into_iter()
                    .enumerate()
                    .map(|(index, cell)| {
                        let plane = match cell.arm {
                            PostingAssignmentArm::OneLeaf => &one_plane,
                            PostingAssignmentArm::TwoBeamLeaves => &two_plane,
                        };
                        crate::v23_incidence_eval::evaluate_v23_incidence_cell_test_shape(
                            &tree,
                            plane,
                            cell,
                            &queries,
                            &truth,
                            16,
                            &latencies[index],
                        )
                    })
                    .collect::<crate::Result<Vec<_>>>()?;
                let authority = V23IncidenceDevelopmentAuthority {
                    source_commit: V23_INCIDENCE_SOURCE_COMMIT.to_string(),
                    source_archive_sha256: V23_INCIDENCE_SOURCE_ARCHIVE_SHA256.to_string(),
                    index_id: V23_INCIDENCE_INDEX_ID.to_string(),
                    dataset_id: V23_INCIDENCE_DATASET_ID.to_string(),
                    query_cohort_sha256: "31".repeat(32),
                    tree_blake3: blake3::hash(&tree_bytes).to_hex().to_string(),
                    posting_one_blake3: blake3::hash(&one).to_hex().to_string(),
                    posting_two_blake3: blake3::hash(&two).to_hex().to_string(),
                    executable_sha256: EXECUTABLE.to_string(),
                };
                let artifact = V23IncidenceDevelopmentArtifact {
                    schema: "borsuk-v23-incidence-development-v1".to_string(),
                    claim_eligible: false,
                    authority: authority.clone(),
                    development,
                    development_truth: truth.clone(),
                    sealed_cell: Some(V23IncidenceCell::registered_ladder()[0]),
                };
                let bytes = canonical_v23_incidence_development_artifact_bytes(
                    &artifact, &authority, &latencies, &truth,
                )?;
                let latency_bytes = encode_v23_incidence_development_latency_bundle(&latencies)?;
                write_test_campaign_file(&root.join("development-result.json"), &bytes)?;
                write_test_campaign_file(&root.join("development-latency.bin"), &latency_bytes)?;
                write_test_campaign_receipt(
                    root,
                    V23IncidencePhase::DevelopmentEvaluation,
                    &[
                        ("development-result", "sha256", "development-result.json"),
                        ("development-latency", "blake3", "development-latency.bin"),
                    ],
                )
            }
            "holdout-binding" => {
                let development =
                    fs::read(root.join("development-result.json")).map_err(|source| {
                        crate::BorsukError::Io {
                            path: root.join("development-result.json"),
                            source,
                        }
                    })?;
                let parsed: V23IncidenceDevelopmentArtifact = serde_json::from_slice(&development)
                    .map_err(|error| {
                        crate::BorsukError::InvalidStorage(format!(
                            "V23 incidence test development JSON differs: {error}"
                        ))
                    })?;
                let sealed_cell = parsed.sealed_cell.ok_or_else(|| {
                    crate::BorsukError::InvalidStorage(
                        "V23 incidence test development cell is absent".to_string(),
                    )
                })?;
                let authority = V23IncidenceHoldoutTruthAuthority {
                    development_result_sha256: format!("{:x}", Sha256::digest(&development)),
                    neighbors_sha256: "32".repeat(32),
                    page_roster_sha256: "33".repeat(32),
                };
                let truth = test_campaign_truth(32, 128);
                let artifact = V23IncidenceHoldoutTruthArtifact {
                    schema: "borsuk-v23-incidence-holdout-truth-v1".to_string(),
                    claim_eligible: false,
                    authority: authority.clone(),
                    sealed_cell,
                    layout: recompute_v23_incidence_layout_quality(&truth)?,
                    truth,
                };
                let bytes = canonical_v23_incidence_holdout_truth_bytes(
                    &artifact,
                    &authority,
                    sealed_cell,
                )?;
                write_test_campaign_file(&root.join("holdout-truth.json"), &bytes)?;
                write_test_campaign_receipt(
                    root,
                    V23IncidencePhase::HoldoutBinding,
                    &[("holdout-truth", "sha256", "holdout-truth.json")],
                )
            }
            "holdout-evaluation" => {
                let tree_bytes = fs::read(root.join("incidence-tree.bin")).map_err(|source| {
                    crate::BorsukError::Io {
                        path: root.join("incidence-tree.bin"),
                        source,
                    }
                })?;
                let tree = decode_incidence_tree(&tree_bytes)?;
                let one = fs::read(root.join("postings-one.bin")).map_err(|source| {
                    crate::BorsukError::Io {
                        path: root.join("postings-one.bin"),
                        source,
                    }
                })?;
                let two = fs::read(root.join("postings-two.bin")).map_err(|source| {
                    crate::BorsukError::Io {
                        path: root.join("postings-two.bin"),
                        source,
                    }
                })?;
                let development_bytes =
                    fs::read(root.join("development-result.json")).map_err(|source| {
                        crate::BorsukError::Io {
                            path: root.join("development-result.json"),
                            source,
                        }
                    })?;
                let development: V23IncidenceDevelopmentArtifact =
                    serde_json::from_slice(&development_bytes).map_err(|error| {
                        crate::BorsukError::InvalidStorage(format!(
                            "V23 incidence test development JSON differs: {error}"
                        ))
                    })?;
                let holdout_bytes =
                    fs::read(root.join("holdout-truth.json")).map_err(|source| {
                        crate::BorsukError::Io {
                            path: root.join("holdout-truth.json"),
                            source,
                        }
                    })?;
                let holdout_truth: V23IncidenceHoldoutTruthArtifact =
                    serde_json::from_slice(&holdout_bytes).map_err(|error| {
                        crate::BorsukError::InvalidStorage(format!(
                            "V23 incidence test holdout JSON differs: {error}"
                        ))
                    })?;
                let latency = crate::v23_incidence_eval::encode_v23_incidence_latency_samples(
                    &vec![1_000_000; 10_000],
                )?;
                let plane = match holdout_truth.sealed_cell.arm {
                    PostingAssignmentArm::OneLeaf => decode_posting_plane(&one)?,
                    PostingAssignmentArm::TwoBeamLeaves => decode_posting_plane(&two)?,
                };
                let queries = test_campaign_queries(32, 128);
                let evaluated = crate::v23_incidence_eval::evaluate_v23_incidence_cell_test_shape(
                    &tree,
                    &plane,
                    holdout_truth.sealed_cell,
                    &queries,
                    &holdout_truth.truth,
                    16,
                    &latency,
                )?;
                let holdout = V23IncidenceHoldoutResult {
                    cell: evaluated.cell,
                    quality: evaluated.quality,
                    projected_serving_bytes: evaluated.projected_serving_bytes,
                    maximum_posting_visits: evaluated.maximum_posting_visits,
                    maximum_touched_pages: evaluated.maximum_touched_pages,
                    p99_ns: evaluated.p99_ns,
                    determinism_passed: evaluated.determinism_passed,
                    latency_blake3: evaluated.latency_blake3,
                    latency_bytes: evaluated.latency_bytes,
                    selections: evaluated.selections,
                };
                let campaign = V23IncidenceCampaignInput {
                    authority_passed: true,
                    resource_passed: true,
                    determinism_passed: true,
                    development: development.development.clone(),
                    holdout_layout: holdout_truth.layout,
                    holdout: Some(holdout),
                };
                let result = V23IncidenceCampaignResult {
                    schema: "borsuk-v23-incidence-result-v1".to_string(),
                    claim_eligible: false,
                    source_commit: V23_INCIDENCE_SOURCE_COMMIT.to_string(),
                    source_archive_sha256: V23_INCIDENCE_SOURCE_ARCHIVE_SHA256.to_string(),
                    index_id: V23_INCIDENCE_INDEX_ID.to_string(),
                    dataset_id: V23_INCIDENCE_DATASET_ID.to_string(),
                    query_cohort_sha256: "31".repeat(32),
                    tree_blake3: blake3::hash(&tree_bytes).to_hex().to_string(),
                    posting_one_blake3: blake3::hash(&one).to_hex().to_string(),
                    posting_two_blake3: blake3::hash(&two).to_hex().to_string(),
                    executable_sha256: EXECUTABLE.to_string(),
                    sealed_cell: development.sealed_cell,
                    classification: classify_v23_incidence_campaign(&campaign),
                    campaign,
                    page_body_reads: 0,
                };
                let development_latency =
                    fs::read(root.join("development-latency.bin")).map_err(|source| {
                        crate::BorsukError::Io {
                            path: root.join("development-latency.bin"),
                            source,
                        }
                    })?;
                let development_latencies =
                    decode_v23_incidence_development_latency_bundle(&development_latency)?;
                let mut latency_refs = development_latencies
                    .iter()
                    .map(Vec::as_slice)
                    .collect::<Vec<_>>();
                latency_refs.push(latency.as_slice());
                let bytes = canonical_v23_incidence_result_bytes(
                    &result,
                    &latency_refs,
                    &development.development_truth,
                    &holdout_truth.truth,
                )?;
                write_test_campaign_file(&root.join("campaign-result.json"), &bytes)?;
                write_test_campaign_file(&root.join("holdout-latency.bin"), &latency)?;
                write_test_campaign_receipt(
                    root,
                    V23IncidencePhase::HoldoutEvaluation,
                    &[
                        ("campaign-result", "sha256", "campaign-result.json"),
                        ("holdout-latency", "blake3", "holdout-latency.bin"),
                    ],
                )
            }
            _ => Err(crate::BorsukError::InvalidStorage(
                "V23 incidence test child phase differs".to_string(),
            )),
        }
    }

    #[test]
    fn v23_incidence_campaign_end_to_end_child() {
        let Ok(phase) = std::env::var("BORSUK_V23_INCIDENCE_TEST_CHILD_PHASE") else {
            return;
        };
        let root = std::env::var("BORSUK_V23_INCIDENCE_TEST_ROOT").unwrap();
        run_v23_incidence_test_child_phase(Path::new(&root), &phase).unwrap();
    }

    #[test]
    fn v23_incidence_campaign_end_to_end_is_process_isolated_deterministic_and_single_use() {
        let executable = std::env::current_exe().unwrap();
        let child = "v23_incidence::tests::v23_incidence_campaign_end_to_end_child";
        let phases = [
            "tree-training",
            "posting-construction",
            "development-evaluation",
            "holdout-binding",
            "holdout-evaluation",
        ];
        let directory = tempfile::tempdir().unwrap();
        let mut canonical_result = None;
        for threads in [1, 2, 8] {
            let root = directory.path().join(format!("threads-{threads}"));
            fs::create_dir(&root).unwrap();
            for phase in phases {
                let output = Command::new(&executable)
                    .args([child, "--exact", "--nocapture"])
                    .env("BORSUK_V23_INCIDENCE_TEST_CHILD_PHASE", phase)
                    .env("BORSUK_V23_INCIDENCE_TEST_ROOT", &root)
                    .env("RAYON_NUM_THREADS", threads.to_string())
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "{phase} child failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            let trained_tree =
                decode_incidence_tree(&fs::read(root.join("incidence-tree.bin")).unwrap()).unwrap();
            assert_eq!(trained_tree.shape.reservoir_rows, 4_096);
            assert_eq!(trained_tree.shape.depth, 5);
            assert_eq!(trained_tree.leaves.len(), 32);
            for (name, arm) in [
                ("postings-one.bin", PostingAssignmentArm::OneLeaf),
                ("postings-two.bin", PostingAssignmentArm::TwoBeamLeaves),
            ] {
                let plane = decode_posting_plane(&fs::read(root.join(name)).unwrap()).unwrap();
                assert_eq!(plane.arm, arm);
                assert_eq!(plane.leaves.len(), 65_536);
                assert!(plane.source_records > 0);
                assert!(plane.leaves.iter().any(|leaf| leaf.total_mass > 0));
            }
            let mut parent_bytes = b"test-root-preflight\n".to_vec();
            for (phase_name, expected_phase) in [
                ("tree-training", V23IncidencePhase::TreeTraining),
                (
                    "posting-construction",
                    V23IncidencePhase::PostingConstruction,
                ),
                (
                    "development-evaluation",
                    V23IncidencePhase::DevelopmentEvaluation,
                ),
                ("holdout-binding", V23IncidencePhase::HoldoutBinding),
                ("holdout-evaluation", V23IncidencePhase::HoldoutEvaluation),
            ] {
                let receipt_bytes =
                    fs::read(root.join(format!("{phase_name}-receipt.json"))).unwrap();
                let receipt: V23IncidenceReceipt = serde_json::from_slice(&receipt_bytes).unwrap();
                assert_eq!(receipt.phase, expected_phase);
                assert_eq!(
                    receipt.parent_receipt_sha256.as_deref(),
                    Some(format!("{:x}", Sha256::digest(&parent_bytes)).as_str())
                );
                assert!(!receipt.claim_eligible);
                assert!(receipt.stop.is_none());
                parent_bytes = receipt_bytes;
            }
            let result_bytes = fs::read(root.join("campaign-result.json")).unwrap();
            let result: crate::v23_incidence_eval::V23IncidenceCampaignResult =
                serde_json::from_slice(&result_bytes).unwrap();
            assert!(!result.claim_eligible);
            assert_eq!(result.page_body_reads, 0);
            assert!(result.sealed_cell.is_some());
            assert!(result.campaign.development.iter().all(|cell| {
                cell.maximum_posting_visits <= u32::from(cell.cell.probes) * 8
                    && cell.maximum_touched_pages == 8
            }));
            assert!(
                result
                    .campaign
                    .development
                    .iter()
                    .flat_map(|cell| &cell.selections)
                    .all(|selection| selection.page_ordinals.len() == 8)
            );
            let holdout = result.campaign.holdout.as_ref().unwrap();
            assert!(
                holdout.maximum_posting_visits <= u32::from(holdout.cell.probes) * 8
                    && holdout.maximum_touched_pages == 8
            );
            assert_eq!(
                holdout
                    .selections
                    .iter()
                    .map(|selection| selection.page_ordinals.len())
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from([8])
            );
            if let Some(expected) = &canonical_result {
                assert_eq!(&result_bytes, expected);
            } else {
                canonical_result = Some(result_bytes.clone());
            }
            let before = result_bytes;
            let output = Command::new(&executable)
                .args([child, "--exact", "--nocapture"])
                .env(
                    "BORSUK_V23_INCIDENCE_TEST_CHILD_PHASE",
                    "holdout-evaluation",
                )
                .env("BORSUK_V23_INCIDENCE_TEST_ROOT", &root)
                .env("RAYON_NUM_THREADS", threads.to_string())
                .output()
                .unwrap();
            assert!(!output.status.success());
            assert_eq!(fs::read(root.join("campaign-result.json")).unwrap(), before);
        }
    }
}
