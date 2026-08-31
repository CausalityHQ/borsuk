use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use arrow_array::{Array, FixedSizeListArray, Float32Array};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{
    Deserialize, Serialize,
    de::{DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result,
    centroid_hnsw::{CatalogRouter, CatalogRoutingStrategy},
    global_pq_sidecar::{F16FlatScanQuantizer, GlobalScanQuantizer, GlobalScanQuantizerState},
    logical_cell_catalog::LogicalCellCatalog,
    metric::VectorMetric,
    rotated_product_quantizer::{ProductQuantizerConfig, ProductRotation, RotatedProductQuantizer},
    segment_cache::ByteAdmissionGate,
    storage::Storage,
    turboquant::{FastTurboQuantMseScanQuantizer, FastTurboQuantProdScanQuantizer},
    v22_feasibility::{V22_MAX_EXACT_PREFIX_ROWS, V22StageLQueryPrefix, V22StageLSpill},
};

#[allow(dead_code, reason = "consumed by the planned D2 page-codec slice")]
pub(crate) const V23_PAGE_MAX_ENCODED_BYTES: u64 = 245_760;
#[allow(dead_code, reason = "consumed by the planned D2 and D3 slices")]
pub(crate) const V23_WAVE_MAX_PAGES: usize = 8;
pub(crate) const V23_WAVE_MAX_BYTES: u64 = 1_966_080;
#[allow(dead_code, reason = "consumed by the planned D2 RAM projection")]
pub(crate) const V23_PROCESS_MAX_BYTES: u64 = 3 * 1024 * 1024 * 1024;
pub(crate) const V23_DIAGNOSTIC_QUERIES: usize = 32;
const V23_PAGE_HEADER_BYTES: u64 = 96;
const V23_PAGE_MAGIC: &[u8; 4] = b"BVP2";
const V23_PAGE_VERSION: u8 = 2;
const V23_PROJECTED_ROWS: u64 = 100_000_000;
const V23_PROJECTED_SELECTOR_COARSE_CELLS: u64 = 4_096;
const V23_PROJECTED_ROOT_HEADER_BYTES: u64 = 96;
// Struct storage, two owned content-address strings, allocator rounding, and
// Vec capacity. This deliberately exceeds the current Rust ABI footprint.
const V23_PROJECTED_ROOT_FIXED_BYTES_PER_PAGE: u64 = 320;
// Production HNSW caps each tower at 17 layers, with 32 base and 16 upper
// neighbours. 4 KiB/page exceeds Vec headers, maximum adjacency capacity, and
// allocator rounding for that bounded topology.
const V23_PROJECTED_ROUTER_BYTES_PER_PAGE: u64 = 4_096;
const V23_PROJECTED_FIXED_RUNTIME_BYTES: u64 = 512 * 1024 * 1024;
const V23_D2_EVALUATED_ARMS: u64 = 2;
const V23_D2_LIGHTWEIGHT_SAMPLE_SLACK_BYTES: u64 = 4_096;
const V23_D1_PROJECTED_PAGE_ROWS: u64 = 2_048;
const V23_SCALAR_SIMD_MAX_DISTANCE_DELTA_PPM: u64 = 10;
#[allow(dead_code, reason = "consumed by the planned D3 benchmark slice")]
pub(crate) const V23_D3_WAVES: usize = 1_000;
const V23_D1_CPU_MAX_NS: u64 = 15_000_000;
const V23_SELECTOR_CODE_WIDTHS: [u16; 2] = [8, 12];
const V23_SELECTOR_DIMENSIONS: u32 = 96;
const V23_SELECTOR_ROUTING_CELLS: usize = 320;
const V23_SELECTOR_RANKED_ROWS: usize = 4_096;
const V23_SELECTOR_HEADER_BYTES: usize = 96;
const V23_SELECTOR_MAGIC: &[u8; 4] = b"BVS3";
const V23_SELECTOR_VERSION: u8 = 3;
const V23_SELECTOR_MAXIMUM_ASSIGNMENTS_PER_ROW: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Production SIMD quantizer family evaluated by V23.
pub enum V23QuantizerFamily {
    /// Seeded SRHT product quantization with one byte per subspace.
    SrhtPq,
    /// Data-oblivious Fast-TurboQuant MSE scan codec.
    FastTurboQuantMse,
    /// Two-stage production Fast-TurboQuant scan codec.
    FastTurboQuantProd,
    /// Near-exact IEEE-754 binary16 coordinates for bounded page-local scans.
    F16Flat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Canonical identity of one D1 quantizer arm.
pub struct V23D1ArmKey {
    /// Production quantizer family.
    pub family: V23QuantizerFamily,
    /// Fixed encoded bytes carried by every row.
    pub code_width_bytes: u16,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Ordered approximate top-ten result retained as scientific evidence.
pub struct V23RankedResult {
    /// Authenticated raw record IDs in rank order.
    pub ids: Vec<Vec<u8>>,
    /// Approximate distances paired with `ids`.
    pub distances: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// D1 code-fidelity evidence for one frozen query.
pub struct V23D1QuerySample {
    /// Zero-based position in the frozen query authority.
    pub query_index: u32,
    /// Exact ground-truth top-ten record IDs.
    pub ground_truth_ids: Vec<Vec<u8>>,
    /// Code-ranked result over the exact top-2,048 oracle pool.
    pub oracle: V23RankedResult,
    /// Scalar-kernel result over the same exact top-2,048 oracle pool.
    pub scalar_oracle: V23RankedResult,
    /// Code-ranked result over the complete registered routed pool.
    pub routed: V23RankedResult,
    /// Exact oracle-pool row count.
    pub oracle_candidate_rows: u32,
    /// Complete routed-pool row count.
    pub routed_candidate_rows: u64,
    /// Exact capacity-derived code rows scanned by the independent CPU timing.
    pub wave_candidate_rows: u64,
    /// Recomputed ground-truth hits in `oracle`.
    pub oracle_hits: u8,
    /// Recomputed ground-truth hits in `routed`.
    pub routed_hits: u8,
    /// Query preparation plus one maximum production page wave SIMD scan.
    pub cpu_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Aggregate D1 evidence for one quantizer family and width.
pub struct V23D1Arm {
    /// Canonical arm identity.
    pub key: V23D1ArmKey,
    /// BLAKE3 of the complete serialized quantizer state.
    pub quantizer_checksum: String,
    /// Canonical, self-contained production quantizer state used by D2 and D3.
    pub quantizer_state: serde_json::Value,
    /// Query-major scientific evidence.
    pub query_samples: Vec<V23D1QuerySample>,
    /// Oracle-pool recall in parts per million.
    pub oracle_recall_ppm: u64,
    /// Routed-pool recall in parts per million.
    pub routed_recall_ppm: u64,
    /// Whether real-corpus scalar and SIMD oracle rankings have identical IDs.
    pub scalar_simd_ids_equal: bool,
    /// Maximum normalized scalar/SIMD distance delta in parts per million.
    pub scalar_simd_max_distance_delta_ppm: u64,
    /// Nearest-rank p99 CPU time across frozen queries.
    pub cpu_p99_ns: u64,
    /// Conservative encoded-byte projection for one maximum page wave.
    pub wave_projected_bytes: u64,
    /// Exact result of every D1 scientific gate.
    pub passed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Complete claim-ineligible V23 D1 report.
pub struct V23D1Report {
    /// Exact evidence schema name.
    pub schema: String,
    /// Authenticated source V20 cell-card root checksum.
    pub v20_root_checksum: String,
    /// Authenticated source V20 codebook checksum.
    pub v20_codebook_checksum: String,
    /// BLAKE3 of the ordered quantizer-training sample ordinals.
    pub sample_ordinals_checksum: String,
    /// BLAKE3 of exact ordered source-query ordinals and raw `f32` bits.
    pub query_vectors_checksum: String,
    /// Strictly increasing frozen source-query ordinals.
    pub query_ordinals: Vec<u64>,
    /// Live rows covered by the immutable source generation.
    pub rows: u64,
    /// Exact dense-vector dimensionality used by every arm.
    pub dimensions: u32,
    /// Complete source routing-cell count.
    pub routing_cell_count: usize,
    /// Maximum authenticated raw record-ID width in the corpus.
    pub maximum_record_id_bytes: u16,
    /// Canonically ordered quantizer arms.
    pub arms: Vec<V23D1Arm>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Authenticated immutable V23 diagnostic posting-page reference.
pub struct V23PageRef {
    /// Authenticated immutable source-generation checksum.
    pub generation_checksum: [u8; 32],
    /// Contiguous zero-based page ordinal.
    pub page_ordinal: u32,
    /// Exact distance metric encoded in the page header.
    #[serde(
        serialize_with = "serialize_v23_page_metric",
        deserialize_with = "deserialize_v23_page_metric"
    )]
    pub metric: VectorMetric,
    /// Exact dense-vector dimensionality.
    pub dimensions: u32,
    /// Exact production quantizer family.
    pub family: V23QuantizerFamily,
    /// Fixed encoded bytes carried by each row.
    pub code_width: u16,
    /// Content-addressed object path.
    pub path: String,
    /// BLAKE3 of the complete encoded page.
    pub checksum: String,
    /// Complete encoded object length.
    pub encoded_bytes: u64,
    /// Unique authoritative rows owned by the page.
    pub primary_rows: u32,
    /// Boundary rows replicated into the page.
    pub replicated_rows: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Authenticated immutable V23 packed page-selector reference.
pub struct V23SelectorRef {
    /// Authenticated immutable source-generation checksum.
    pub generation_checksum: [u8; 32],
    /// Exact distance metric encoded in the selector header.
    #[serde(
        serialize_with = "serialize_v23_page_metric",
        deserialize_with = "deserialize_v23_page_metric"
    )]
    pub metric: VectorMetric,
    /// Exact dense-vector dimensionality.
    pub dimensions: u32,
    /// Complete authenticated coarse-centroid count.
    pub coarse_cells: u32,
    /// Complete immutable posting-page count.
    pub page_count: u32,
    /// Maximum page assignments carried by one unique row.
    pub maximum_assignments_per_row: u8,
    /// Fixed SRHT-PQ bytes carried by every unique selector row.
    pub code_width: u16,
    /// Complete packed unique-row count.
    pub row_count: u64,
    /// Content-addressed object path.
    pub path: String,
    /// BLAKE3 of the complete encoded selector.
    pub checksum: String,
    /// Complete encoded object length.
    pub encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23SelectorRow {
    coarse_cell: u32,
    primary_page: u32,
    replica_page: Option<u32>,
    source_ordinal: u64,
    code: Box<[u8]>,
}

impl V23SelectorRow {
    pub(crate) fn new(
        coarse_cell: u32,
        primary_page: u32,
        replica_page: Option<u32>,
        source_ordinal: u64,
        code: &[u8],
    ) -> Self {
        Self {
            coarse_cell,
            primary_page,
            replica_page,
            source_ordinal,
            code: code.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23SelectorInput {
    pub(crate) generation_checksum: [u8; 32],
    pub(crate) metric: VectorMetric,
    pub(crate) dimensions: u32,
    pub(crate) page_count: u32,
    pub(crate) code_width: u16,
    pub(crate) maximum_assignments_per_row: u8,
    pub(crate) coarse_centroids: Vec<Vec<f32>>,
    pub(crate) rows: Vec<V23SelectorRow>,
}

#[derive(Debug, Clone)]
pub(crate) struct V23DecodedSelector {
    bytes: Bytes,
    centroids: Box<[f32]>,
    offsets: Box<[u32]>,
    primary_pages_start: usize,
    replica_pages_start: usize,
    codes_start: usize,
    dimensions: usize,
    code_width: usize,
    row_count: usize,
}

impl V23DecodedSelector {
    pub(crate) fn coarse_centroid(&self, cell: usize) -> Option<&[f32]> {
        let start = cell.checked_mul(self.dimensions)?;
        self.centroids
            .get(start..start.checked_add(self.dimensions)?)
    }

    pub(crate) fn cell_range(&self, cell: usize) -> Option<std::ops::Range<usize>> {
        let start = usize::try_from(*self.offsets.get(cell)?).ok()?;
        let end = usize::try_from(*self.offsets.get(cell + 1)?).ok()?;
        Some(start..end)
    }

    pub(crate) fn row_pages(&self, row: usize) -> Option<(u32, Option<u32>)> {
        if row >= self.row_count {
            return None;
        }
        let primary = read_v23_u32(&self.bytes, self.primary_pages_start + row * 4)?;
        let replica = read_v23_u32(&self.bytes, self.replica_pages_start + row * 4)?;
        Some((primary, (replica != u32::MAX).then_some(replica)))
    }

    #[cfg(test)]
    pub(crate) fn row_code(&self, row: usize) -> Option<&[u8]> {
        if row >= self.row_count {
            return None;
        }
        let start = self
            .codes_start
            .checked_add(row.checked_mul(self.code_width)?)?;
        self.bytes.get(start..start.checked_add(self.code_width)?)
    }

    fn row_codes(&self, rows: std::ops::Range<usize>) -> Option<&[u8]> {
        if rows.start > rows.end || rows.end > self.row_count {
            return None;
        }
        let start = self
            .codes_start
            .checked_add(rows.start.checked_mul(self.code_width)?)?;
        let end = self
            .codes_start
            .checked_add(rows.end.checked_mul(self.code_width)?)?;
        self.bytes.get(start..end)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23PageSelection {
    pub(crate) page_ordinals: Vec<u32>,
    pub(crate) candidate_rows: u64,
    pub(crate) routed_cells: u16,
    pub(crate) ranked_rows: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct V23GlobalAdcAuthority<'a> {
    pub(crate) d1_selector_arm: &'a V23D1Arm,
    pub(crate) d2_selector: &'a V23SelectorRef,
    pub(crate) pages: &'a [V23PageRef],
    pub(crate) selector_bytes: Bytes,
}

#[derive(Debug, Clone)]
pub(crate) struct V23GlobalAdcRequest<'a> {
    pub(crate) authority: V23GlobalAdcAuthority<'a>,
    pub(crate) queries: &'a [Vec<f32>],
    pub(crate) ground_truth_page_assignments: &'a [Vec<Vec<u32>>],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[doc(hidden)]
pub struct V23GlobalAdcObjectIdentity {
    pub role: String,
    pub uri: String,
    pub digest_algorithm: String,
    pub digest: String,
    pub encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[doc(hidden)]
pub struct V23GlobalAdcEvidenceIdentity {
    pub source_commit: String,
    pub source_archive_sha256: String,
    pub index_id: String,
    pub d1_report: V23GlobalAdcObjectIdentity,
    pub d2_terminal: V23GlobalAdcObjectIdentity,
    pub d2_result: V23GlobalAdcObjectIdentity,
    pub d2_report: V23GlobalAdcObjectIdentity,
    pub roster: V23GlobalAdcObjectIdentity,
    pub query: V23GlobalAdcObjectIdentity,
    pub selector: V23GlobalAdcObjectIdentity,
}

#[derive(Debug, Clone)]
pub(crate) struct V23GlobalAdcArtifactRequest<'a> {
    pub(crate) d1_report: &'a V23D1Report,
    pub(crate) d2_report: &'a V23D2Report,
    pub(crate) pages: &'a [V23PageRef],
    pub(crate) query_ordinals: &'a [u64],
    pub(crate) queries: &'a [Vec<f32>],
    pub(crate) ground_truth_page_assignments: &'a [Vec<Vec<u32>>],
    pub(crate) selector_bytes: Bytes,
    pub(crate) registered_identity: &'a V23GlobalAdcEvidenceIdentity,
    pub(crate) observed_identity: &'a V23GlobalAdcEvidenceIdentity,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct V23GlobalAdcArtifactResult {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) evidence: V23GlobalAdcEvidenceIdentity,
    pub(crate) diagnostic: V23GlobalAdcResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V23GlobalAdcLocalArtifactPaths {
    pub d1_report: PathBuf,
    pub d2_terminal: PathBuf,
    pub d2_result: PathBuf,
    pub d2_report: PathBuf,
    pub roster: PathBuf,
    pub query: PathBuf,
    pub selector: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V23GlobalAdcLocalRunRequest {
    pub paths: V23GlobalAdcLocalArtifactPaths,
    pub registered_identity: V23GlobalAdcEvidenceIdentity,
    pub execute_global_adc: bool,
}

pub(crate) struct V23GlobalAdcLoadedLocalArtifacts {
    d1_report: V23D1Report,
    d2_report: V23D2Report,
    pages: Vec<V23PageRef>,
    queries: Vec<Vec<f32>>,
    selector_bytes: Bytes,
    evidence: V23GlobalAdcEvidenceIdentity,
}

impl V23GlobalAdcLoadedLocalArtifacts {
    fn width_12_arm(&self) -> Result<&V23D2Arm> {
        self.d2_report
            .arms
            .iter()
            .find(|arm| {
                arm.selector_key
                    == (V23D1ArmKey {
                        family: V23QuantizerFamily::SrhtPq,
                        code_width_bytes: 12,
                    })
            })
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "V23 global ADC local width-12 arm is absent".to_string(),
                )
            })
    }

    fn validate(&self) -> Result<()> {
        let d2_arm = self.width_12_arm()?;
        let ground_truth_page_assignments = d2_arm
            .query_samples
            .iter()
            .map(|sample| sample.ground_truth_page_assignments.clone())
            .collect::<Vec<_>>();
        validate_v23_global_adc_artifact_request(V23GlobalAdcArtifactRequest {
            d1_report: &self.d1_report,
            d2_report: &self.d2_report,
            pages: &self.pages,
            query_ordinals: &self.d2_report.query_ordinals,
            queries: &self.queries,
            ground_truth_page_assignments: &ground_truth_page_assignments,
            selector_bytes: self.selector_bytes.clone(),
            registered_identity: &self.evidence,
            observed_identity: &self.evidence,
        })
    }

    pub(crate) fn run(&self) -> Result<V23GlobalAdcArtifactResult> {
        self.validate()?;
        let d2_arm = self.width_12_arm()?;
        let ground_truth_page_assignments = d2_arm
            .query_samples
            .iter()
            .map(|sample| sample.ground_truth_page_assignments.clone())
            .collect::<Vec<_>>();
        let d1_selector_arm = self
            .d1_report
            .arms
            .iter()
            .find(|arm| arm.key == d2_arm.selector_key)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "V23 global ADC local D1 selector arm is absent".to_string(),
                )
            })?;
        let diagnostic = diagnose_v23_global_adc(V23GlobalAdcRequest {
            authority: V23GlobalAdcAuthority {
                d1_selector_arm,
                d2_selector: &d2_arm.selector,
                pages: &self.pages,
                selector_bytes: self.selector_bytes.clone(),
            },
            queries: &self.queries,
            ground_truth_page_assignments: &ground_truth_page_assignments,
        })?;
        let result = V23GlobalAdcArtifactResult {
            schema: "borsuk-v23-global-adc-diagnostic-v1".to_string(),
            claim_eligible: false,
            evidence: self.evidence.clone(),
            diagnostic,
        };
        canonical_v23_global_adc_artifact_result_bytes(&result, &self.evidence)?;
        Ok(result)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum V23GlobalAdcCausalClass {
    #[serde(rename = "tested-reducers-rejected")]
    TestedReducers,
    #[serde(rename = "faithful-reducer-rejected")]
    FaithfulReducer,
    #[serde(rename = "router-rejected")]
    Router,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23GlobalAdcGates {
    pub(crate) aggregate_recall_ppm: u64,
    pub(crate) minimum_query_recall_ppm: u64,
    pub(crate) oracle_attainment_ppm: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct V23GlobalAdcQuerySample {
    pub(crate) query_index: u32,
    pub(crate) page_ordinals: Vec<u32>,
    pub(crate) gt_page_hits: u8,
    pub(crate) oracle_gt_page_hits: u8,
    pub(crate) recall_ppm: u64,
    pub(crate) minimum_distance: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct V23GlobalAdcReducerResult {
    pub(crate) reducer: String,
    pub(crate) query_samples: Vec<V23GlobalAdcQuerySample>,
    pub(crate) aggregate_recall_ppm: u64,
    pub(crate) minimum_query_recall_ppm: u64,
    pub(crate) oracle_attainment_ppm: u64,
    pub(crate) passed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct V23GlobalAdcResult {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) selector_checksum: String,
    pub(crate) selector_code_width: u16,
    pub(crate) selector_cells_scanned: u32,
    pub(crate) selector_rows_scanned: u64,
    pub(crate) selection_width: u8,
    pub(crate) page_body_reads: u64,
    pub(crate) scalar_simd_max_distance_delta_ppm: u64,
    pub(crate) scalar_simd_pages_equal: bool,
    pub(crate) gates: V23GlobalAdcGates,
    pub(crate) faithful: V23GlobalAdcReducerResult,
    pub(crate) per_page_min: V23GlobalAdcReducerResult,
    pub(crate) causal_classification: V23GlobalAdcCausalClass,
}

#[derive(Debug, Clone, Copy)]
struct V23GlobalAdcRankedRow {
    distance: f32,
    row: usize,
}

impl PartialEq for V23GlobalAdcRankedRow {
    fn eq(&self, other: &Self) -> bool {
        self.distance.to_bits() == other.distance.to_bits() && self.row == other.row
    }
}

impl Eq for V23GlobalAdcRankedRow {}

impl PartialOrd for V23GlobalAdcRankedRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for V23GlobalAdcRankedRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.row.cmp(&other.row))
    }
}

pub(crate) struct V23PageSelector {
    quantizer: GlobalScanQuantizer,
    decoded: V23DecodedSelector,
    metric: VectorMetric,
    page_count: usize,
}

impl V23PageSelector {
    pub(crate) fn from_encoded(
        selector_ref: &V23SelectorRef,
        bytes: Bytes,
        quantizer: GlobalScanQuantizer,
    ) -> Result<Self> {
        if !V23_SELECTOR_CODE_WIDTHS.contains(&selector_ref.code_width)
            || quantizer.code_bytes_per_vector() != usize::from(selector_ref.code_width)
        {
            return Err(BorsukError::InvalidStorage(
                "V23 selector quantizer authority differs".to_string(),
            ));
        }
        quantizer.prepare_contiguous_query(&vec![0.0; selector_ref.dimensions as usize])?;
        let decoded = decode_v23_selector(bytes, selector_ref)?;
        Ok(Self {
            quantizer,
            decoded,
            metric: selector_ref.metric.clone(),
            page_count: usize::try_from(selector_ref.page_count).map_err(|_| {
                BorsukError::InvalidStorage("V23 selector page count exceeds usize".to_string())
            })?,
        })
    }

    pub(crate) fn select(&self, query: &[f32], maximum_pages: usize) -> Result<V23PageSelection> {
        if query.len() != self.decoded.dimensions
            || query.iter().any(|value| !value.is_finite())
            || maximum_pages == 0
            || maximum_pages > V23_WAVE_MAX_PAGES
            || maximum_pages > self.page_count
        {
            return Err(BorsukError::InvalidSearchOptions(
                "V23 selector query authority differs".to_string(),
            ));
        }
        let prepared_query = if self.metric == VectorMetric::Cosine {
            crate::metric::unit_l2_normalized(query)
        } else {
            query.to_vec()
        };
        let mut cells = (0..self.decoded.offsets.len() - 1)
            .map(|cell| {
                Ok((
                    self.metric.distance_unchecked(
                        &prepared_query,
                        self.decoded.coarse_centroid(cell).ok_or_else(|| {
                            BorsukError::InvalidStorage(
                                "V23 selector centroid is absent".to_string(),
                            )
                        })?,
                    )?,
                    cell,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let compare_cells = |left: &(f32, usize), right: &(f32, usize)| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        };
        if cells.len() > V23_SELECTOR_ROUTING_CELLS {
            cells.select_nth_unstable_by(V23_SELECTOR_ROUTING_CELLS, compare_cells);
            cells.truncate(V23_SELECTOR_ROUTING_CELLS);
        }
        cells.sort_unstable_by(compare_cells);
        let prepared = self.quantizer.prepare_contiguous_query(&prepared_query)?;
        let mut ranked = Vec::<(f32, usize)>::new();
        for (_, cell) in &cells {
            let range = self.decoded.cell_range(*cell).ok_or_else(|| {
                BorsukError::InvalidStorage("V23 selector cell range is absent".to_string())
            })?;
            let distances = self.quantizer.score_prepared_contiguous_codes(
                &prepared,
                self.decoded.row_codes(range.clone()).ok_or_else(|| {
                    BorsukError::InvalidStorage("V23 selector cell codes are absent".to_string())
                })?,
            )?;
            if distances.len() != range.len() {
                return Err(BorsukError::InvalidStorage(
                    "V23 selector cell score cardinality differs".to_string(),
                ));
            }
            ranked.extend(distances.into_iter().zip(range));
        }
        let candidate_rows = u64::try_from(ranked.len()).map_err(|_| {
            BorsukError::InvalidStorage("V23 selector candidate rows exceed u64".to_string())
        })?;
        let compare_rows = |left: &(f32, usize), right: &(f32, usize)| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        };
        if ranked.len() > V23_SELECTOR_RANKED_ROWS {
            ranked.select_nth_unstable_by(V23_SELECTOR_RANKED_ROWS, compare_rows);
            ranked.truncate(V23_SELECTOR_RANKED_ROWS);
        }
        ranked.sort_unstable_by(compare_rows);
        let page_ordinals = v23_reciprocal_rank_max_cover(&self.decoded, &ranked, maximum_pages)?;
        Ok(V23PageSelection {
            page_ordinals,
            candidate_rows,
            routed_cells: u16::try_from(cells.len()).map_err(|_| {
                BorsukError::InvalidStorage("V23 selector routed cells exceed u16".to_string())
            })?,
            ranked_rows: u32::try_from(ranked.len()).map_err(|_| {
                BorsukError::InvalidStorage("V23 selector ranked rows exceed u32".to_string())
            })?,
        })
    }
}

fn v23_reciprocal_rank_max_cover(
    decoded: &V23DecodedSelector,
    ranked: &[(f32, usize)],
    maximum_pages: usize,
) -> Result<Vec<u32>> {
    let row_pages = ranked
        .iter()
        .map(|(_, row)| {
            decoded.row_pages(*row).ok_or_else(|| {
                BorsukError::InvalidStorage("V23 selector row pages are absent".to_string())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    v23_reciprocal_rank_page_cover(&row_pages, maximum_pages)
}

pub(crate) fn v23_reciprocal_rank_page_cover(
    row_pages: &[(u32, Option<u32>)],
    maximum_pages: usize,
) -> Result<Vec<u32>> {
    // Deterministic weighted maximum coverage over both physical labels.
    // Reciprocal rank weights keep the nearest scientific rows dominant
    // while allowing one page to cover several strong rows. Selected rows
    // are removed from subsequent rounds, so every page adds evidence.
    if row_pages.is_empty()
        || maximum_pages == 0
        || maximum_pages > V23_WAVE_MAX_PAGES
        || row_pages
            .iter()
            .any(|(primary, replica)| replica == &Some(*primary))
    {
        return Err(BorsukError::InvalidStorage(
            "V23 page-cover authority differs".to_string(),
        ));
    }
    let mut uncovered = vec![true; row_pages.len()];
    let mut page_ordinals = Vec::with_capacity(maximum_pages);
    while page_ordinals.len() < maximum_pages {
        let mut scores = BTreeMap::<u32, u64>::new();
        for (rank, (primary, replica)) in row_pages.iter().enumerate() {
            if !uncovered[rank] {
                continue;
            }
            let weight = 1_000_000_000_u64 / u64::try_from(rank + 1).unwrap();
            let primary_score = scores.entry(*primary).or_default();
            *primary_score = primary_score.saturating_add(weight);
            if let Some(replica) = replica {
                let replica_score = scores.entry(*replica).or_default();
                *replica_score = replica_score.saturating_add(weight);
            }
        }
        let Some((page, score)) = scores
            .into_iter()
            .filter(|(page, _)| !page_ordinals.contains(page))
            .max_by(|left, right| left.1.cmp(&right.1).then_with(|| right.0.cmp(&left.0)))
        else {
            break;
        };
        if score == 0 {
            break;
        }
        page_ordinals.push(page);
        for (rank, (primary, replica)) in row_pages.iter().enumerate() {
            if *primary == page || *replica == Some(page) {
                uncovered[rank] = false;
            }
        }
    }
    page_ordinals.sort_unstable();
    Ok(page_ordinals)
}

const V23_GLOBAL_ADC_BLOCK_ROWS: usize = 65_536;
const V23_GLOBAL_ADC_SELECTION_WIDTH: usize = 8;

fn v23_global_adc_retain(
    heap: &mut BinaryHeap<V23GlobalAdcRankedRow>,
    candidate: V23GlobalAdcRankedRow,
) {
    if heap.len() < V23_SELECTOR_RANKED_ROWS {
        heap.push(candidate);
    } else if heap.peek().is_some_and(|worst| candidate < *worst) {
        heap.pop();
        heap.push(candidate);
    }
}

fn v23_global_adc_observe_page_minima(
    decoded: &V23DecodedSelector,
    minima: &mut [f32],
    row: usize,
    distance: f32,
) -> Result<()> {
    let (primary, replica) = decoded.row_pages(row).ok_or_else(|| {
        BorsukError::InvalidStorage("V23 global ADC row pages are absent".to_string())
    })?;
    for page in [Some(primary), replica].into_iter().flatten() {
        let value = minima.get_mut(page as usize).ok_or_else(|| {
            BorsukError::InvalidStorage("V23 global ADC page ordinal differs".to_string())
        })?;
        if distance.total_cmp(value).is_lt() {
            *value = distance;
        }
    }
    Ok(())
}

fn v23_global_adc_reduce(
    decoded: &V23DecodedSelector,
    ranked: BinaryHeap<V23GlobalAdcRankedRow>,
    minima: Vec<f32>,
) -> Result<(Vec<u32>, Vec<u32>)> {
    let mut ranked = ranked
        .into_vec()
        .into_iter()
        .map(|candidate| (candidate.distance, candidate.row))
        .collect::<Vec<_>>();
    ranked.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    let faithful = v23_reciprocal_rank_max_cover(decoded, &ranked, V23_GLOBAL_ADC_SELECTION_WIDTH)?;
    let mut per_page_min = minima
        .into_iter()
        .enumerate()
        .filter(|(_, distance)| distance.is_finite())
        .map(|(page, distance)| (distance, page as u32))
        .collect::<Vec<_>>();
    per_page_min.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    per_page_min.truncate(V23_GLOBAL_ADC_SELECTION_WIDTH);
    let mut per_page_min = per_page_min
        .into_iter()
        .map(|(_, page)| page)
        .collect::<Vec<_>>();
    per_page_min.sort_unstable();
    if faithful.len() != V23_GLOBAL_ADC_SELECTION_WIDTH
        || per_page_min.len() != V23_GLOBAL_ADC_SELECTION_WIDTH
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC cannot select eight pages".to_string(),
        ));
    }
    Ok((faithful, per_page_min))
}

fn v23_global_adc_select(
    quantizer: &GlobalScanQuantizer,
    decoded: &V23DecodedSelector,
    metric: &VectorMetric,
    page_count: usize,
    query: &[f32],
) -> Result<(Vec<u32>, Vec<u32>, f32, u64)> {
    let query = if metric == &VectorMetric::Cosine {
        crate::metric::unit_l2_normalized(query)
    } else {
        query.to_vec()
    };
    let prepared = quantizer.prepare_contiguous_query(&query)?;
    let mut simd_ranked = BinaryHeap::with_capacity(V23_SELECTOR_RANKED_ROWS + 1);
    let mut scalar_ranked = BinaryHeap::with_capacity(V23_SELECTOR_RANKED_ROWS + 1);
    let mut simd_minima = vec![f32::INFINITY; page_count];
    let mut scalar_minima = vec![f32::INFINITY; page_count];
    let mut minimum_distance = f32::INFINITY;
    let mut maximum_delta_ppm = 0_u64;
    for cell in 0..decoded.offsets.len() - 1 {
        let range = decoded.cell_range(cell).ok_or_else(|| {
            BorsukError::InvalidStorage("V23 global ADC cell range is absent".to_string())
        })?;
        for start in (range.start..range.end).step_by(V23_GLOBAL_ADC_BLOCK_ROWS) {
            let end = range
                .end
                .min(start.saturating_add(V23_GLOBAL_ADC_BLOCK_ROWS));
            let codes = decoded.row_codes(start..end).ok_or_else(|| {
                BorsukError::InvalidStorage("V23 global ADC codes are absent".to_string())
            })?;
            let simd = quantizer.score_prepared_contiguous_codes(&prepared, codes)?;
            let scalar = quantizer.score_codes(&query, codes.chunks_exact(decoded.code_width))?;
            if simd.len() != end - start || scalar.len() != simd.len() {
                return Err(BorsukError::InvalidStorage(
                    "V23 global ADC score cardinality differs".to_string(),
                ));
            }
            for (offset, (simd_distance, scalar_distance)) in
                simd.into_iter().zip(scalar).enumerate()
            {
                if !simd_distance.is_finite() || !scalar_distance.is_finite() {
                    return Err(BorsukError::InvalidStorage(
                        "V23 global ADC score is non-finite".to_string(),
                    ));
                }
                let normalized = f64::from((simd_distance - scalar_distance).abs())
                    / f64::from(scalar_distance.abs().max(1.0));
                maximum_delta_ppm = maximum_delta_ppm
                    .max((normalized * 1_000_000.0).ceil().min(u64::MAX as f64) as u64);
                minimum_distance = minimum_distance.min(simd_distance);
                let row = start + offset;
                v23_global_adc_retain(
                    &mut simd_ranked,
                    V23GlobalAdcRankedRow {
                        distance: simd_distance,
                        row,
                    },
                );
                v23_global_adc_retain(
                    &mut scalar_ranked,
                    V23GlobalAdcRankedRow {
                        distance: scalar_distance,
                        row,
                    },
                );
                v23_global_adc_observe_page_minima(decoded, &mut simd_minima, row, simd_distance)?;
                v23_global_adc_observe_page_minima(
                    decoded,
                    &mut scalar_minima,
                    row,
                    scalar_distance,
                )?;
            }
        }
    }
    if !minimum_distance.is_finite() {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC selector is empty".to_string(),
        ));
    }
    let simd = v23_global_adc_reduce(decoded, simd_ranked, simd_minima)?;
    let scalar = v23_global_adc_reduce(decoded, scalar_ranked, scalar_minima)?;
    if simd != scalar {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC scalar and SIMD page selections differ".to_string(),
        ));
    }
    Ok((simd.0, simd.1, minimum_distance, maximum_delta_ppm))
}

fn validate_v23_global_adc_authority(
    authority: &V23GlobalAdcAuthority<'_>,
) -> Result<(GlobalScanQuantizer, V23DecodedSelector)> {
    let selector = authority.d2_selector;
    if authority.d1_selector_arm.key
        != (V23D1ArmKey {
            family: V23QuantizerFamily::SrhtPq,
            code_width_bytes: 12,
        })
        || selector.code_width != 12
        || selector.coarse_cells != 4_096
        || selector.page_count as usize != authority.pages.len()
        || authority.pages.len() < V23_GLOBAL_ADC_SELECTION_WIDTH
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC authority differs".to_string(),
        ));
    }
    let quantizer = restore_v23_diagnostic_quantizer(authority.d1_selector_arm)?;
    if quantizer.dimensions() != selector.dimensions as usize {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC quantizer dimensions differ".to_string(),
        ));
    }
    let decoded = decode_v23_selector(authority.selector_bytes.clone(), selector)?;
    let mut primary_rows = vec![0_u32; authority.pages.len()];
    let mut replica_rows = vec![0_u32; authority.pages.len()];
    for row in 0..decoded.row_count {
        let (primary, replica) = decoded.row_pages(row).ok_or_else(|| {
            BorsukError::InvalidStorage("V23 global ADC row page is absent".to_string())
        })?;
        primary_rows[primary as usize] = primary_rows[primary as usize].saturating_add(1);
        if let Some(replica) = replica {
            replica_rows[replica as usize] = replica_rows[replica as usize].saturating_add(1);
        }
    }
    for (index, page) in authority.pages.iter().enumerate() {
        if page.page_ordinal as usize != index
            || page.generation_checksum != selector.generation_checksum
            || page.metric != selector.metric
            || page.dimensions != selector.dimensions
            || page.family != V23QuantizerFamily::F16Flat
            || page.code_width != page.dimensions.saturating_mul(2) as u16
            || !valid_checksum(&page.checksum)
            || page.path != format!("pages/{}", page.checksum)
            || page.encoded_bytes == 0
            || page.encoded_bytes > V23_PAGE_MAX_ENCODED_BYTES
            || page.primary_rows != primary_rows[index]
            || page.replicated_rows != replica_rows[index]
        {
            return Err(BorsukError::InvalidStorage(
                "V23 global ADC page authority differs".to_string(),
            ));
        }
    }
    Ok((quantizer, decoded))
}

fn v23_global_adc_valid_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_v23_global_adc_evidence_identity(
    observed: &V23GlobalAdcEvidenceIdentity,
    registered: &V23GlobalAdcEvidenceIdentity,
) -> Result<()> {
    if observed != registered
        || !v23_global_adc_valid_hex(&observed.source_commit, 40)
        || !v23_global_adc_valid_hex(&observed.source_archive_sha256, 64)
        || observed.index_id.is_empty()
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC registered evidence identity differs".to_string(),
        ));
    }
    let objects = [
        (&observed.d1_report, "d1-report", "sha256"),
        (&observed.d2_terminal, "d2-terminal", "sha256"),
        (&observed.d2_result, "d2-result", "sha256"),
        (&observed.d2_report, "d2-report", "sha256"),
        (&observed.roster, "page-roster", "sha256"),
        (&observed.query, "query-parquet", "sha256"),
        (&observed.selector, "selector", "blake3"),
    ];
    if objects.iter().any(|(object, role, digest_algorithm)| {
        object.role != *role
            || object.uri.is_empty()
            || object.digest_algorithm != *digest_algorithm
            || !v23_global_adc_valid_hex(&object.digest, 64)
            || object.encoded_bytes == 0
    }) {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC object identity differs".to_string(),
        ));
    }
    Ok(())
}

fn v23_global_adc_read_local_role(
    path: &Path,
    identity: &V23GlobalAdcObjectIdentity,
) -> Result<Vec<u8>> {
    let bytes = fs::read(path).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 global ADC {} cannot be read: {error}",
            identity.role
        ))
    })?;
    let digest = if identity.digest_algorithm == "blake3" {
        blake3::hash(&bytes).to_hex().to_string()
    } else {
        format!("{:x}", Sha256::digest(&bytes))
    };
    if bytes.len() as u64 != identity.encoded_bytes || digest != identity.digest {
        return Err(BorsukError::InvalidStorage(format!(
            "V23 global ADC {} local bytes differ",
            identity.role
        )));
    }
    Ok(bytes)
}

struct V23JsonOrderSeed<'a> {
    expected_keys: Option<&'a [&'a str]>,
}

impl<'de> DeserializeSeed<'de> for V23JsonOrderSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(V23JsonOrderVisitor {
            expected_keys: self.expected_keys,
        })
    }
}

struct V23JsonOrderVisitor<'a> {
    expected_keys: Option<&'a [&'a str]>,
}

impl<'de> Visitor<'de> for V23JsonOrderVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("compact JSON with the registered object-key order")
    }

    fn visit_bool<E>(self, _value: bool) -> std::result::Result<Self::Value, E> {
        Ok(())
    }

    fn visit_i64<E>(self, _value: i64) -> std::result::Result<Self::Value, E> {
        Ok(())
    }

    fn visit_u64<E>(self, _value: u64) -> std::result::Result<Self::Value, E> {
        Ok(())
    }

    fn visit_f64<E>(self, _value: f64) -> std::result::Result<Self::Value, E> {
        Ok(())
    }

    fn visit_str<E>(self, _value: &str) -> std::result::Result<Self::Value, E> {
        Ok(())
    }

    fn visit_string<E>(self, _value: String) -> std::result::Result<Self::Value, E> {
        Ok(())
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(())
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence
            .next_element_seed(V23JsonOrderSeed {
                expected_keys: None,
            })?
            .is_some()
        {}
        Ok(())
    }

    fn visit_map<A>(self, mut object: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut previous: Option<String> = None;
        let mut index = 0;
        while let Some(key) = object.next_key::<String>()? {
            if let Some(expected) = self.expected_keys {
                if expected.get(index).copied() != Some(key.as_str()) {
                    return Err(serde::de::Error::custom(
                        "top-level object key order differs",
                    ));
                }
            } else if previous.as_ref().is_some_and(|prior| prior >= &key) {
                return Err(serde::de::Error::custom(
                    "recursive object key order differs",
                ));
            }
            object.next_value_seed(V23JsonOrderSeed {
                expected_keys: None,
            })?;
            previous = Some(key);
            index += 1;
        }
        if self
            .expected_keys
            .is_some_and(|expected| index != expected.len())
        {
            return Err(serde::de::Error::custom(
                "top-level object key count differs",
            ));
        }
        Ok(())
    }
}

fn v23_global_adc_validate_json_bytes(
    bytes: &[u8],
    role: &str,
    expected_top_level_order: Option<&[&str]>,
) -> Result<()> {
    if bytes.last() != Some(&b'\n') || bytes[..bytes.len().saturating_sub(1)].last() == Some(&b'\n')
    {
        return Err(BorsukError::InvalidStorage(format!(
            "V23 global ADC {role} newline convention differs"
        )));
    }
    let body = &bytes[..bytes.len() - 1];
    let mut in_string = false;
    let mut escaped = false;
    for byte in body.iter().copied() {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
        } else if byte == b'"' {
            in_string = true;
        } else if matches!(byte, b' ' | b'\t' | b'\r' | b'\n') {
            return Err(BorsukError::InvalidStorage(format!(
                "V23 global ADC {role} compact JSON convention differs"
            )));
        }
    }
    if in_string || escaped {
        return Err(BorsukError::InvalidStorage(format!(
            "V23 global ADC {role} JSON string differs"
        )));
    }
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    V23JsonOrderSeed {
        expected_keys: expected_top_level_order,
    }
    .deserialize(&mut deserializer)
    .map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 global ADC {role} JSON ordering differs: {error}"
        ))
    })?;
    deserializer.end().map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 global ADC {role} trailing JSON differs: {error}"
        ))
    })
}

fn v23_global_adc_parse_json(bytes: &[u8], role: &str) -> Result<serde_json::Value> {
    serde_json::from_slice(bytes).map_err(|error| {
        BorsukError::InvalidStorage(format!("V23 global ADC {role} JSON differs: {error}"))
    })
}

fn v23_global_adc_exact_json_keys(
    value: &serde_json::Value,
    expected: &[&str],
    role: &str,
) -> Result<()> {
    let keys = value
        .as_object()
        .map(|object| object.keys().map(String::as_str).collect::<BTreeSet<_>>());
    if keys != Some(expected.iter().copied().collect()) {
        return Err(BorsukError::InvalidStorage(format!(
            "V23 global ADC {role} schema differs"
        )));
    }
    Ok(())
}

fn v23_global_adc_json_string<'a>(
    value: &'a serde_json::Value,
    key: &str,
    role: &str,
) -> Result<&'a str> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| BorsukError::InvalidStorage(format!("V23 global ADC {role} {key} differs")))
}

fn v23_global_adc_json_bool(value: &serde_json::Value, key: &str, role: &str) -> Result<bool> {
    value
        .get(key)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| BorsukError::InvalidStorage(format!("V23 global ADC {role} {key} differs")))
}

fn v23_global_adc_json_u64(value: &serde_json::Value, key: &str, role: &str) -> Result<u64> {
    value
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| BorsukError::InvalidStorage(format!("V23 global ADC {role} {key} differs")))
}

pub(crate) fn read_v23_query_vectors(
    bytes: &[u8],
    query_ordinals: &[u64],
    expected_queries: usize,
) -> Result<Vec<Vec<f32>>> {
    const V23_GLOBAL_ADC_QUERY_ROWS: u64 = 10_000;
    if query_ordinals.len() != expected_queries
        || expected_queries == 0
        || query_ordinals.windows(2).any(|pair| pair[0] >= pair[1])
        || query_ordinals
            .last()
            .is_none_or(|ordinal| *ordinal >= V23_GLOBAL_ADC_QUERY_ROWS)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC query ordinals differ".to_string(),
        ));
    }
    let expected_schema = Schema::new(vec![Field::new(
        "emb",
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
        ),
        false,
    )]);
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    if builder.schema().as_ref() != &expected_schema
        || builder.metadata().file_metadata().num_rows() != V23_GLOBAL_ADC_QUERY_ROWS as i64
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC query Parquet schema differs".to_string(),
        ));
    }
    let mut queries = vec![None; expected_queries];
    let mut physical_row = 0_u64;
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 1 || batch.column(0).null_count() != 0 {
            return Err(BorsukError::InvalidStorage(
                "V23 global ADC query Parquet columns differ".to_string(),
            ));
        }
        let lists = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V23 global ADC query Parquet list differs".to_string())
            })?;
        for row in 0..batch.num_rows() {
            if lists.is_null(row) {
                return Err(BorsukError::InvalidStorage(
                    "V23 global ADC query vector is null".to_string(),
                ));
            }
            let values = lists.value(row);
            let values = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("V23 global ADC query values differ".to_string())
                })?;
            if values.len() != 96
                || values.null_count() != 0
                || values.values().iter().any(|value| !value.is_finite())
                || values.values().iter().all(|value| *value == 0.0)
            {
                return Err(BorsukError::InvalidStorage(
                    "V23 global ADC query vector authority differs".to_string(),
                ));
            }
            if let Ok(query_index) = query_ordinals.binary_search(&physical_row) {
                queries[query_index] = Some(values.values().to_vec());
            }
            physical_row += 1;
        }
    }
    if physical_row != V23_GLOBAL_ADC_QUERY_ROWS {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC query row count differs".to_string(),
        ));
    }
    queries
        .into_iter()
        .map(|query| {
            query.ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "V23 global ADC registered query ordinal is absent".to_string(),
                )
            })
        })
        .collect()
}

pub(crate) fn load_v23_global_adc_local_artifacts(
    paths: &V23GlobalAdcLocalArtifactPaths,
    observed_identity: &V23GlobalAdcEvidenceIdentity,
    registered_identity: &V23GlobalAdcEvidenceIdentity,
) -> Result<V23GlobalAdcLoadedLocalArtifacts> {
    validate_v23_global_adc_evidence_identity(observed_identity, registered_identity)?;
    let d1_bytes = v23_global_adc_read_local_role(&paths.d1_report, &observed_identity.d1_report)?;
    let terminal_bytes =
        v23_global_adc_read_local_role(&paths.d2_terminal, &observed_identity.d2_terminal)?;
    let result_bytes =
        v23_global_adc_read_local_role(&paths.d2_result, &observed_identity.d2_result)?;
    let d2_bytes = v23_global_adc_read_local_role(&paths.d2_report, &observed_identity.d2_report)?;
    let roster_bytes = v23_global_adc_read_local_role(&paths.roster, &observed_identity.roster)?;
    let query_bytes = v23_global_adc_read_local_role(&paths.query, &observed_identity.query)?;
    let selector_bytes =
        v23_global_adc_read_local_role(&paths.selector, &observed_identity.selector)?;

    const TERMINAL_KEYS: &[&str] = &[
        "schema_version",
        "status",
        "role",
        "attempt",
        "attempt_id",
        "instance_id",
        "source_archive_sha256",
        "manifest_sha256",
        "protocol_sha256",
        "binary_sha256",
        "purchase_option",
        "runtime_profile",
        "arm_index",
        "max_active_searches",
        "max_waiting_searches",
        "leaf_read_width",
        "max_inflight_leaf_reads",
        "max_parallel_decode_rank_tasks",
        "cpu_threads",
        "io_threads",
        "s3_get_concurrency",
        "ram_budget_bytes",
        "disk_cache_max_bytes",
        "exact_read_max_physical_amplification",
        "execution_contract_sha256",
        "artifact_upload_reconciliations",
        "claim_eligible",
        "v23_stage",
        "v23_passed",
        "v23_result_sha256",
        "v23_page_prefix",
        "v23_d2_report_sha256",
        "v23_pages_sha256",
        "v23_summary_sha256",
        "v23_d1_receipt_sha256",
        "v23_d1_report_sha256",
        "v23_prerequisite_binary_sha256",
        "base_build_terminal_sha256",
        "base_manifest_sha256",
        "base_protocol_sha256",
        "base_source_archive_sha256",
        "base_index_receipt_sha256",
        "base_object_roster_sha256",
        "base_inventory_sha256",
        "base_index_id",
        "base_index_uri",
        "diagnostic_source_archive_sha256",
        "memory_max_bytes",
        "memory_swap_max_bytes",
        "memory_peak_bytes",
    ];
    v23_global_adc_validate_json_bytes(&d1_bytes, "D1 report", None)?;
    v23_global_adc_validate_json_bytes(&d2_bytes, "D2 report", None)?;
    v23_global_adc_validate_json_bytes(&result_bytes, "D2 result", None)?;
    v23_global_adc_validate_json_bytes(&roster_bytes, "page roster", None)?;
    v23_global_adc_validate_json_bytes(&terminal_bytes, "D2 terminal", Some(TERMINAL_KEYS))?;

    let d1_value = v23_global_adc_parse_json(&d1_bytes, "D1 report")?;
    v23_global_adc_exact_json_keys(
        &d1_value,
        &[
            "claim_eligible",
            "dataset_id",
            "document_kind",
            "index_id",
            "report",
            "schema",
            "source_archive_sha256",
            "stage",
        ],
        "D1 report",
    )?;
    let d1_report: V23D1Report =
        serde_json::from_value(d1_value.get("report").cloned().ok_or_else(|| {
            BorsukError::InvalidStorage("V23 global ADC D1 report is absent".to_string())
        })?)
        .map_err(|error| {
            BorsukError::InvalidStorage(format!("V23 global ADC D1 report differs: {error}"))
        })?;
    let d2_value = v23_global_adc_parse_json(&d2_bytes, "D2 report")?;
    v23_global_adc_exact_json_keys(
        &d2_value,
        &[
            "claim_eligible",
            "d1_report_sha256",
            "dataset_id",
            "document_kind",
            "index_id",
            "page_uri",
            "report",
            "schema",
            "source_archive_sha256",
            "stage",
        ],
        "D2 report",
    )?;
    let d2_report: V23D2Report =
        serde_json::from_value(d2_value.get("report").cloned().ok_or_else(|| {
            BorsukError::InvalidStorage("V23 global ADC D2 report is absent".to_string())
        })?)
        .map_err(|error| {
            BorsukError::InvalidStorage(format!("V23 global ADC D2 report differs: {error}"))
        })?;
    let terminal = v23_global_adc_parse_json(&terminal_bytes, "D2 terminal")?;
    v23_global_adc_exact_json_keys(&terminal, TERMINAL_KEYS, "D2 terminal")?;
    let result = v23_global_adc_parse_json(&result_bytes, "D2 result")?;
    v23_global_adc_exact_json_keys(
        &result,
        &[
            "arms",
            "artifact_sha256",
            "attempt_id",
            "cell_id",
            "claim_eligible",
            "d1_report_sha256",
            "dataset_id",
            "dataset_materialization_sha256",
            "diagnostic_cell_id",
            "document_kind",
            "elapsed_ns",
            "index_id",
            "instance_identity",
            "pages",
            "pages_sha256",
            "passed",
            "passing_arm_indexes",
            "publishable",
            "queries",
            "resources",
            "rows",
            "runtime_attestation",
            "schema",
            "source_archive_sha256",
            "stage",
        ],
        "D2 result",
    )?;
    let roster = v23_global_adc_parse_json(&roster_bytes, "page roster")?;
    v23_global_adc_exact_json_keys(
        &roster,
        &[
            "claim_eligible",
            "d1_report_sha256",
            "dataset_id",
            "document_kind",
            "index_id",
            "page_uri",
            "pages",
            "schema",
            "source_archive_sha256",
            "stage",
        ],
        "page roster",
    )?;
    if v23_global_adc_json_u64(&terminal, "schema_version", "D2 terminal")? != 5
        || terminal.get("status").and_then(serde_json::Value::as_str) != Some("complete")
        || terminal.get("role").and_then(serde_json::Value::as_str) != Some("runtime")
        || v23_global_adc_json_bool(&terminal, "claim_eligible", "D2 terminal")?
        || result.get("schema").and_then(serde_json::Value::as_str) != Some("borsuk-v23-summary-v1")
        || result
            .get("document_kind")
            .and_then(serde_json::Value::as_str)
            != Some("publication-v3-v23-d2-summary")
        || result
            .get("claim_eligible")
            .and_then(serde_json::Value::as_bool)
            != Some(false)
        || roster.get("schema").and_then(serde_json::Value::as_str) != Some("borsuk-v23-pages-v1")
        || d1_value.get("schema").and_then(serde_json::Value::as_str)
            != Some("borsuk-v23-d1-artifact-v1")
        || d2_value.get("schema").and_then(serde_json::Value::as_str)
            != Some("borsuk-v23-d2-artifact-v1")
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC role schema differs".to_string(),
        ));
    }
    for (document, role, expected_kind, expected_stage) in [
        (&d1_value, "D1 report", "publication-v3-v23-d1-report", "d1"),
        (&d2_value, "D2 report", "publication-v3-v23-d2-report", "d2"),
        (
            &roster,
            "page roster",
            "publication-v3-v23-page-roster",
            "d2",
        ),
    ] {
        if v23_global_adc_json_bool(document, "claim_eligible", role)?
            || v23_global_adc_json_string(document, "document_kind", role)? != expected_kind
            || v23_global_adc_json_string(document, "stage", role)? != expected_stage
            || v23_global_adc_json_string(document, "index_id", role)? != observed_identity.index_id
            || v23_global_adc_json_string(document, "source_archive_sha256", role)?
                != observed_identity.source_archive_sha256
        {
            return Err(BorsukError::InvalidStorage(format!(
                "V23 global ADC {role} outer authority differs"
            )));
        }
    }
    let dataset_id = v23_global_adc_json_string(&d1_value, "dataset_id", "D1 report")?;
    let page_uri = v23_global_adc_json_string(&d2_value, "page_uri", "D2 report")?;
    if dataset_id.is_empty()
        || v23_global_adc_json_string(&d2_value, "dataset_id", "D2 report")? != dataset_id
        || v23_global_adc_json_string(&roster, "dataset_id", "page roster")? != dataset_id
        || page_uri.is_empty()
        || v23_global_adc_json_string(&roster, "page_uri", "page roster")? != page_uri
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC outer role binding differs".to_string(),
        ));
    }
    for key in [
        "status",
        "role",
        "attempt_id",
        "instance_id",
        "source_archive_sha256",
        "manifest_sha256",
        "protocol_sha256",
        "binary_sha256",
        "purchase_option",
        "runtime_profile",
        "execution_contract_sha256",
        "v23_stage",
        "v23_result_sha256",
        "v23_page_prefix",
        "v23_d2_report_sha256",
        "v23_pages_sha256",
        "v23_summary_sha256",
        "v23_d1_receipt_sha256",
        "v23_d1_report_sha256",
        "v23_prerequisite_binary_sha256",
        "base_build_terminal_sha256",
        "base_manifest_sha256",
        "base_protocol_sha256",
        "base_source_archive_sha256",
        "base_index_receipt_sha256",
        "base_object_roster_sha256",
        "base_inventory_sha256",
        "base_index_id",
        "base_index_uri",
        "diagnostic_source_archive_sha256",
    ] {
        v23_global_adc_json_string(&terminal, key, "D2 terminal")?;
    }
    for key in [
        "schema_version",
        "attempt",
        "arm_index",
        "max_active_searches",
        "max_waiting_searches",
        "leaf_read_width",
        "max_inflight_leaf_reads",
        "max_parallel_decode_rank_tasks",
        "cpu_threads",
        "io_threads",
        "s3_get_concurrency",
        "ram_budget_bytes",
        "disk_cache_max_bytes",
        "exact_read_max_physical_amplification",
        "artifact_upload_reconciliations",
        "memory_max_bytes",
        "memory_swap_max_bytes",
        "memory_peak_bytes",
    ] {
        v23_global_adc_json_u64(&terminal, key, "D2 terminal")?;
    }
    v23_global_adc_json_bool(&terminal, "v23_passed", "D2 terminal")?;

    for key in [
        "artifact_sha256",
        "attempt_id",
        "cell_id",
        "d1_report_sha256",
        "dataset_id",
        "dataset_materialization_sha256",
        "diagnostic_cell_id",
        "document_kind",
        "index_id",
        "instance_identity",
        "pages_sha256",
        "schema",
        "source_archive_sha256",
        "stage",
    ] {
        v23_global_adc_json_string(&result, key, "D2 result")?;
    }
    for key in ["arms", "elapsed_ns", "pages", "queries", "rows"] {
        v23_global_adc_json_u64(&result, key, "D2 result")?;
    }
    for key in ["claim_eligible", "passed", "publishable"] {
        v23_global_adc_json_bool(&result, key, "D2 result")?;
    }
    if result
        .get("passing_arm_indexes")
        .and_then(serde_json::Value::as_array)
        .is_none_or(|indexes| indexes.iter().any(|index| index.as_u64().is_none()))
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC D2 result passing arm indexes differ".to_string(),
        ));
    }
    let resources = result.get("resources").ok_or_else(|| {
        BorsukError::InvalidStorage("V23 global ADC D2 result resources are absent".to_string())
    })?;
    v23_global_adc_exact_json_keys(
        resources,
        &[
            "cpu_ns",
            "disk_read_bytes",
            "disk_write_bytes",
            "peak_rss_bytes",
        ],
        "D2 result resources",
    )?;
    for key in [
        "cpu_ns",
        "disk_read_bytes",
        "disk_write_bytes",
        "peak_rss_bytes",
    ] {
        v23_global_adc_json_u64(resources, key, "D2 result resources")?;
    }
    let attestation = result.get("runtime_attestation").ok_or_else(|| {
        BorsukError::InvalidStorage("V23 global ADC runtime attestation is absent".to_string())
    })?;
    v23_global_adc_exact_json_keys(
        attestation,
        &[
            "architecture",
            "attempt_id",
            "cache_capacity_bytes",
            "cache_device",
            "cache_filesystem_bytes",
            "cache_is_mount",
            "cell_id",
            "effective_disk_cache_max_bytes",
            "instance_id",
            "instance_type",
            "memory_max_bytes",
            "memory_peak_bytes",
            "oom_events",
            "oom_kill_events",
            "purchase_option",
            "root_device",
            "schema_version",
            "source_revision",
            "swap_current_bytes",
            "swap_max_bytes",
            "swap_peak_bytes",
            "vcpus",
        ],
        "runtime attestation",
    )?;
    for key in [
        "architecture",
        "attempt_id",
        "cache_device",
        "cell_id",
        "instance_id",
        "instance_type",
        "purchase_option",
        "root_device",
        "source_revision",
    ] {
        v23_global_adc_json_string(attestation, key, "runtime attestation")?;
    }
    for key in [
        "cache_capacity_bytes",
        "cache_filesystem_bytes",
        "effective_disk_cache_max_bytes",
        "memory_max_bytes",
        "memory_peak_bytes",
        "oom_events",
        "oom_kill_events",
        "schema_version",
        "swap_current_bytes",
        "swap_max_bytes",
        "swap_peak_bytes",
        "vcpus",
    ] {
        v23_global_adc_json_u64(attestation, key, "runtime attestation")?;
    }
    v23_global_adc_json_bool(attestation, "cache_is_mount", "runtime attestation")?;

    let attempt_id = v23_global_adc_json_string(&terminal, "attempt_id", "D2 terminal")?;
    let instance_id = v23_global_adc_json_string(&terminal, "instance_id", "D2 terminal")?;
    if v23_global_adc_json_string(&result, "attempt_id", "D2 result")? != attempt_id
        || v23_global_adc_json_string(attestation, "attempt_id", "runtime attestation")?
            != attempt_id
        || v23_global_adc_json_string(&result, "instance_identity", "D2 result")? != instance_id
        || v23_global_adc_json_string(attestation, "instance_id", "runtime attestation")?
            != instance_id
        || v23_global_adc_json_string(attestation, "source_revision", "runtime attestation")?
            != observed_identity.source_commit
        || v23_global_adc_json_string(&result, "source_archive_sha256", "D2 result")?
            != observed_identity.source_archive_sha256
        || v23_global_adc_json_string(&result, "index_id", "D2 result")?
            != observed_identity.index_id
        || v23_global_adc_json_string(&result, "dataset_id", "D2 result")? != dataset_id
        || v23_global_adc_json_string(&terminal, "base_index_id", "D2 terminal")?
            != observed_identity.index_id
        || v23_global_adc_json_string(&terminal, "diagnostic_source_archive_sha256", "D2 terminal")?
            != observed_identity.source_archive_sha256
        || v23_global_adc_json_string(&terminal, "v23_page_prefix", "D2 terminal")? != page_uri
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC runtime cross-object binding differs".to_string(),
        ));
    }
    if v23_global_adc_json_string(&terminal, "source_archive_sha256", "D2 terminal")?
        != observed_identity.source_archive_sha256
        || v23_global_adc_json_string(&terminal, "v23_result_sha256", "D2 terminal")?
            != observed_identity.d2_result.digest
        || v23_global_adc_json_string(&terminal, "v23_d2_report_sha256", "D2 terminal")?
            != observed_identity.d2_report.digest
        || v23_global_adc_json_string(&terminal, "v23_pages_sha256", "D2 terminal")?
            != observed_identity.roster.digest
        || v23_global_adc_json_string(&terminal, "v23_d1_report_sha256", "D2 terminal")?
            != observed_identity.d1_report.digest
        || v23_global_adc_json_string(&result, "artifact_sha256", "D2 result")?
            != observed_identity.d2_report.digest
        || v23_global_adc_json_string(&result, "pages_sha256", "D2 result")?
            != observed_identity.roster.digest
        || v23_global_adc_json_string(&result, "d1_report_sha256", "D2 result")?
            != observed_identity.d1_report.digest
        || v23_global_adc_json_string(&d2_value, "d1_report_sha256", "D2 report")?
            != observed_identity.d1_report.digest
        || v23_global_adc_json_string(&roster, "d1_report_sha256", "page roster")?
            != observed_identity.d1_report.digest
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC receipt binding differs".to_string(),
        ));
    }
    let pages: Vec<V23PageRef> =
        serde_json::from_value(roster.get("pages").cloned().ok_or_else(|| {
            BorsukError::InvalidStorage("V23 global ADC roster pages are absent".to_string())
        })?)
        .map_err(|error| {
            BorsukError::InvalidStorage(format!("V23 global ADC roster pages differ: {error}"))
        })?;
    let passing_arm_indexes = d2_report
        .arms
        .iter()
        .enumerate()
        .filter_map(|(index, arm)| arm.passed.then_some(index as u64))
        .collect::<Vec<_>>();
    let result_passing_arm_indexes = result
        .get("passing_arm_indexes")
        .and_then(serde_json::Value::as_array)
        .unwrap()
        .iter()
        .map(|index| index.as_u64().unwrap())
        .collect::<Vec<_>>();
    let result_passed = v23_global_adc_json_bool(&result, "passed", "D2 result")?;
    let diagnostic_cell_id =
        v23_global_adc_json_string(&result, "diagnostic_cell_id", "D2 result")?;
    if v23_global_adc_json_u64(&result, "arms", "D2 result")? != d2_report.arms.len() as u64
        || v23_global_adc_json_u64(&result, "pages", "D2 result")? != pages.len() as u64
        || v23_global_adc_json_u64(&result, "queries", "D2 result")?
            != V23_DIAGNOSTIC_QUERIES as u64
        || v23_global_adc_json_u64(&result, "rows", "D2 result")? != d2_report.rows
        || result_passing_arm_indexes != passing_arm_indexes
        || result_passed != !passing_arm_indexes.is_empty()
        || v23_global_adc_json_bool(&terminal, "v23_passed", "D2 terminal")? != result_passed
        || v23_global_adc_json_string(attestation, "cell_id", "runtime attestation")?
            != diagnostic_cell_id
        || attempt_id != format!("runtime-v23-d2-{diagnostic_cell_id}-arm-0000-a0001")
        || !v23_global_adc_valid_hex(
            v23_global_adc_json_string(&result, "dataset_materialization_sha256", "D2 result")?,
            64,
        )
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC D2 result derivation differs".to_string(),
        ));
    }
    let queries = read_v23_query_vectors(
        &query_bytes,
        &d1_report.query_ordinals,
        V23_DIAGNOSTIC_QUERIES,
    )?;
    let loaded = V23GlobalAdcLoadedLocalArtifacts {
        d1_report,
        d2_report,
        pages,
        queries,
        selector_bytes: Bytes::from(selector_bytes),
        evidence: observed_identity.clone(),
    };
    loaded.validate()?;
    Ok(loaded)
}

#[doc(hidden)]
pub fn run_v23_global_adc_local_request(request: V23GlobalAdcLocalRunRequest) -> Result<Vec<u8>> {
    if !request.execute_global_adc {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC execution was not explicitly authorized".to_string(),
        ));
    }
    let loaded = load_v23_global_adc_local_artifacts(
        &request.paths,
        &request.registered_identity,
        &request.registered_identity,
    )?;
    let result = loaded.run()?;
    canonical_v23_global_adc_artifact_result_bytes(&result, &request.registered_identity)
}

pub(crate) fn validate_v23_global_adc_artifact_request(
    request: V23GlobalAdcArtifactRequest<'_>,
) -> Result<()> {
    validate_v23_global_adc_evidence_identity(
        request.observed_identity,
        request.registered_identity,
    )?;
    validate_d1_report(request.d1_report)?;
    validate_d2_report(request.d2_report)?;
    if request.d2_report.d1_report_checksum != v23_d1_report_checksum(request.d1_report)?
        || request.query_ordinals != request.d1_report.query_ordinals
        || request.query_ordinals != request.d2_report.query_ordinals
        || v23_query_vectors_checksum(request.query_ordinals, request.queries)?
            != request.d1_report.query_vectors_checksum
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC report and query authority differs".to_string(),
        ));
    }
    let selector_key = V23D1ArmKey {
        family: V23QuantizerFamily::SrhtPq,
        code_width_bytes: 12,
    };
    let d1_selector_arm = request
        .d1_report
        .arms
        .iter()
        .find(|arm| arm.key == selector_key)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 global ADC width-12 D1 arm is absent".to_string())
        })?;
    let d2_arm = request
        .d2_report
        .arms
        .iter()
        .find(|arm| arm.selector_key == selector_key)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 global ADC width-12 D2 arm is absent".to_string())
        })?;
    if !request
        .d1_report
        .arms
        .iter()
        .any(|arm| arm.key == d2_arm.d1_key)
        || request.pages != d2_arm.pages
        || request.ground_truth_page_assignments.len() != V23_DIAGNOSTIC_QUERIES
        || d2_arm.query_samples.len() != V23_DIAGNOSTIC_QUERIES
        || request
            .ground_truth_page_assignments
            .iter()
            .zip(&d2_arm.query_samples)
            .any(|(ground_truth, sample)| ground_truth != &sample.ground_truth_page_assignments)
        || request.observed_identity.selector.digest != d2_arm.selector.checksum
        || request.observed_identity.selector.encoded_bytes != d2_arm.selector.encoded_bytes
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC cross-object authority differs".to_string(),
        ));
    }
    validate_v23_global_adc_authority(&V23GlobalAdcAuthority {
        d1_selector_arm,
        d2_selector: &d2_arm.selector,
        pages: request.pages,
        selector_bytes: request.selector_bytes,
    })?;
    Ok(())
}

pub(crate) fn classify_v23_global_adc(
    faithful_passed: bool,
    per_page_min_passed: bool,
) -> Result<V23GlobalAdcCausalClass> {
    match (faithful_passed, per_page_min_passed) {
        (false, false) => Ok(V23GlobalAdcCausalClass::TestedReducers),
        (false, true) => Ok(V23GlobalAdcCausalClass::FaithfulReducer),
        (true, _) => Ok(V23GlobalAdcCausalClass::Router),
    }
}

fn v23_global_adc_reducer_result(
    reducer: &str,
    samples: Vec<V23GlobalAdcQuerySample>,
) -> V23GlobalAdcReducerResult {
    let total_hits = samples
        .iter()
        .map(|sample| u64::from(sample.gt_page_hits))
        .sum::<u64>();
    let total_oracle_hits = samples
        .iter()
        .map(|sample| u64::from(sample.oracle_gt_page_hits))
        .sum::<u64>();
    let denominator = (samples.len() as u64).saturating_mul(10);
    let aggregate_recall_ppm = total_hits.saturating_mul(1_000_000) / denominator.max(1);
    let minimum_query_recall_ppm = samples
        .iter()
        .map(|sample| sample.recall_ppm)
        .min()
        .unwrap_or(0);
    let oracle_attainment_ppm = total_hits.saturating_mul(1_000_000) / total_oracle_hits.max(1);
    let passed = aggregate_recall_ppm >= 975_000
        && minimum_query_recall_ppm >= 800_000
        && oracle_attainment_ppm >= 995_000;
    V23GlobalAdcReducerResult {
        reducer: reducer.to_string(),
        query_samples: samples,
        aggregate_recall_ppm,
        minimum_query_recall_ppm,
        oracle_attainment_ppm,
        passed,
    }
}

pub(crate) fn diagnose_v23_global_adc(
    request: V23GlobalAdcRequest<'_>,
) -> Result<V23GlobalAdcResult> {
    let (quantizer, decoded) = validate_v23_global_adc_authority(&request.authority)?;
    if request.queries.len() != V23_DIAGNOSTIC_QUERIES
        || request.ground_truth_page_assignments.len() != V23_DIAGNOSTIC_QUERIES
        || request.queries.iter().any(|query| {
            query.len() != decoded.dimensions || query.iter().any(|value| !value.is_finite())
        })
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC query authority differs".to_string(),
        ));
    }
    let mut faithful_samples = Vec::with_capacity(V23_DIAGNOSTIC_QUERIES);
    let mut per_page_min_samples = Vec::with_capacity(V23_DIAGNOSTIC_QUERIES);
    let mut scalar_simd_max_distance_delta_ppm = 0_u64;
    for (query_index, (query, assignments)) in request
        .queries
        .iter()
        .zip(request.ground_truth_page_assignments)
        .enumerate()
    {
        if assignments.len() != 10
            || assignments.iter().any(|pages| {
                pages.is_empty()
                    || pages.windows(2).any(|pair| pair[0] >= pair[1])
                    || pages
                        .iter()
                        .any(|page| *page as usize >= request.authority.pages.len())
            })
        {
            return Err(BorsukError::InvalidStorage(
                "V23 global ADC ground truth authority differs".to_string(),
            ));
        }
        let oracle = best_v23_page_coverage(assignments, V23_GLOBAL_ADC_SELECTION_WIDTH)?;
        let (faithful_pages, per_page_min_pages, minimum_distance, delta_ppm) =
            v23_global_adc_select(
                &quantizer,
                &decoded,
                &request.authority.d2_selector.metric,
                request.authority.pages.len(),
                query,
            )?;
        scalar_simd_max_distance_delta_ppm = scalar_simd_max_distance_delta_ppm.max(delta_ppm);
        for (pages, destination) in [
            (faithful_pages, &mut faithful_samples),
            (per_page_min_pages, &mut per_page_min_samples),
        ] {
            let hits = assignments
                .iter()
                .filter(|assigned| {
                    assigned
                        .iter()
                        .any(|page| pages.binary_search(page).is_ok())
                })
                .count();
            destination.push(V23GlobalAdcQuerySample {
                query_index: query_index as u32,
                page_ordinals: pages,
                gt_page_hits: hits as u8,
                oracle_gt_page_hits: oracle.hits as u8,
                recall_ppm: (hits as u64).saturating_mul(100_000),
                minimum_distance,
            });
        }
    }
    let faithful =
        v23_global_adc_reducer_result("global-top4096-reciprocal-rank-max-cover", faithful_samples);
    let per_page_min = v23_global_adc_reducer_result("per-page-minimum-adc", per_page_min_samples);
    let causal_classification = classify_v23_global_adc(faithful.passed, per_page_min.passed)?;
    Ok(V23GlobalAdcResult {
        schema: "borsuk-v23-global-adc-v1".to_string(),
        claim_eligible: false,
        selector_checksum: request.authority.d2_selector.checksum.clone(),
        selector_code_width: request.authority.d2_selector.code_width,
        selector_cells_scanned: request.authority.d2_selector.coarse_cells,
        selector_rows_scanned: request.authority.d2_selector.row_count,
        selection_width: V23_GLOBAL_ADC_SELECTION_WIDTH as u8,
        page_body_reads: 0,
        scalar_simd_max_distance_delta_ppm,
        scalar_simd_pages_equal: true,
        gates: V23GlobalAdcGates {
            aggregate_recall_ppm: 975_000,
            minimum_query_recall_ppm: 800_000,
            oracle_attainment_ppm: 995_000,
        },
        faithful,
        per_page_min,
        causal_classification,
    })
}

pub(crate) fn canonical_v23_global_adc_result_bytes(
    result: &V23GlobalAdcResult,
) -> Result<Vec<u8>> {
    if result.claim_eligible
        || result.schema != "borsuk-v23-global-adc-v1"
        || result.selection_width as usize != V23_GLOBAL_ADC_SELECTION_WIDTH
        || result.page_body_reads != 0
        || !result.scalar_simd_pages_equal
        || result.gates.aggregate_recall_ppm != 975_000
        || result.gates.minimum_query_recall_ppm != 800_000
        || result.gates.oracle_attainment_ppm != 995_000
        || classify_v23_global_adc(result.faithful.passed, result.per_page_min.passed)?
            != result.causal_classification
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC result authority differs".to_string(),
        ));
    }
    let value = serde_json::to_value(result).map_err(|error| {
        BorsukError::InvalidStorage(format!("V23 global ADC result cannot be encoded: {error}"))
    })?;
    let mut bytes = serde_json::to_vec(&v23_canonical_json_value(value)).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 global ADC result cannot be serialized: {error}"
        ))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn validate_v23_global_adc_artifact_reducer(
    reducer: &V23GlobalAdcReducerResult,
    expected_name: &str,
) -> Result<()> {
    if reducer.reducer != expected_name
        || reducer.query_samples.len() != V23_DIAGNOSTIC_QUERIES
        || reducer
            .query_samples
            .iter()
            .enumerate()
            .any(|(query_index, sample)| {
                usize::try_from(sample.query_index).ok() != Some(query_index)
                    || sample.page_ordinals.len() != V23_GLOBAL_ADC_SELECTION_WIDTH
                    || sample
                        .page_ordinals
                        .windows(2)
                        .any(|pair| pair[0] >= pair[1])
                    || sample.gt_page_hits > 10
                    || sample.oracle_gt_page_hits > 10
                    || sample.gt_page_hits > sample.oracle_gt_page_hits
                    || sample.recall_ppm != u64::from(sample.gt_page_hits) * 100_000
                    || !sample.minimum_distance.is_finite()
            })
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC reducer samples differ".to_string(),
        ));
    }
    let recomputed = v23_global_adc_reducer_result(expected_name, reducer.query_samples.clone());
    if &recomputed != reducer {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC reducer aggregate differs".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn canonical_v23_global_adc_artifact_result_bytes(
    result: &V23GlobalAdcArtifactResult,
    expected_identity: &V23GlobalAdcEvidenceIdentity,
) -> Result<Vec<u8>> {
    validate_v23_global_adc_evidence_identity(&result.evidence, expected_identity)?;
    let selector_fixed_bytes = (V23_SELECTOR_HEADER_BYTES as u64)
        .checked_add(4_096_u64 * 96 * 4)
        .and_then(|bytes| bytes.checked_add(4_097_u64 * 4));
    let selector_rows_from_length = selector_fixed_bytes
        .and_then(|fixed| result.evidence.selector.encoded_bytes.checked_sub(fixed))
        .filter(|row_bytes| row_bytes % 20 == 0)
        .map(|row_bytes| row_bytes / 20)
        .filter(|rows| *rows > 0);
    if result.schema != "borsuk-v23-global-adc-diagnostic-v1"
        || result.claim_eligible
        || result.diagnostic.schema != "borsuk-v23-global-adc-v1"
        || !valid_checksum(&result.diagnostic.selector_checksum)
        || result.diagnostic.selector_checksum != result.evidence.selector.digest
        || result.diagnostic.selector_code_width != 12
        || result.diagnostic.selector_cells_scanned != 4_096
        || selector_rows_from_length != Some(result.diagnostic.selector_rows_scanned)
        || result.diagnostic.selection_width as usize != V23_GLOBAL_ADC_SELECTION_WIDTH
        || result.diagnostic.page_body_reads != 0
        || !result.diagnostic.scalar_simd_pages_equal
        || result.diagnostic.scalar_simd_max_distance_delta_ppm
            > V23_SCALAR_SIMD_MAX_DISTANCE_DELTA_PPM
        || result.diagnostic.gates.aggregate_recall_ppm != 975_000
        || result.diagnostic.gates.minimum_query_recall_ppm != 800_000
        || result.diagnostic.gates.oracle_attainment_ppm != 995_000
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC artifact result authority differs".to_string(),
        ));
    }
    validate_v23_global_adc_artifact_reducer(
        &result.diagnostic.faithful,
        "global-top4096-reciprocal-rank-max-cover",
    )?;
    validate_v23_global_adc_artifact_reducer(
        &result.diagnostic.per_page_min,
        "per-page-minimum-adc",
    )?;
    if classify_v23_global_adc(
        result.diagnostic.faithful.passed,
        result.diagnostic.per_page_min.passed,
    )? != result.diagnostic.causal_classification
    {
        return Err(BorsukError::InvalidStorage(
            "V23 global ADC causal classification differs".to_string(),
        ));
    }
    canonical_v23_global_adc_result_bytes(&result.diagnostic)?;
    let value = serde_json::to_value(result).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 global ADC artifact result cannot be encoded: {error}"
        ))
    })?;
    let mut bytes = serde_json::to_vec(&v23_canonical_json_value(value)).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 global ADC artifact result cannot be serialized: {error}"
        ))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn serialize_v23_page_metric<S>(
    metric: &VectorMetric,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if !matches!(
        metric,
        VectorMetric::Euclidean | VectorMetric::SquaredEuclidean | VectorMetric::Cosine
    ) {
        return Err(serde::ser::Error::custom(
            "V23 page metric is not supported",
        ));
    }
    serializer.serialize_str(&metric.to_string())
}

fn deserialize_v23_page_metric<'de, D>(
    deserializer: D,
) -> std::result::Result<VectorMetric, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    let metric = value
        .parse::<VectorMetric>()
        .map_err(|_| serde::de::Error::custom("V23 page metric is invalid"))?;
    if value != metric.to_string()
        || !matches!(
            metric,
            VectorMetric::Euclidean | VectorMetric::SquaredEuclidean | VectorMetric::Cosine
        )
    {
        return Err(serde::de::Error::custom("V23 page metric is invalid"));
    }
    Ok(metric)
}

pub(crate) type V23PageSink<'a> = dyn FnMut(&V23PageRef, &Bytes) -> Result<()> + 'a;
pub(crate) type V23SelectorSink<'a> = dyn FnMut(&V23SelectorRef, &Bytes) -> Result<()> + 'a;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23PageRow {
    pub(crate) canonical_record_id: Box<[u8]>,
    pub(crate) code: Box<[u8]>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23PageInput {
    pub(crate) generation_checksum: [u8; 32],
    pub(crate) page_ordinal: u32,
    pub(crate) metric: VectorMetric,
    pub(crate) dimensions: u32,
    pub(crate) family: V23QuantizerFamily,
    pub(crate) code_width: u16,
    pub(crate) primary_rows: Vec<V23PageRow>,
    pub(crate) replicated_rows: Vec<V23PageRow>,
}

#[derive(Debug, Clone)]
pub(crate) struct V23DecodedPage {
    bytes: Bytes,
    offsets: Box<[u32]>,
    page_ordinal: u32,
    id_start: usize,
    code_start: usize,
    primary_rows: usize,
    replicated_rows: usize,
    code_width: usize,
}

impl V23DecodedPage {
    pub(crate) fn page_ordinal(&self) -> u32 {
        self.page_ordinal
    }

    pub(crate) fn primary_rows(&self) -> usize {
        self.primary_rows
    }

    pub(crate) fn replicated_rows(&self) -> usize {
        self.replicated_rows
    }

    pub(crate) fn record_id(&self, index: usize) -> Option<&[u8]> {
        let start = usize::try_from(*self.offsets.get(index)?).ok()?;
        let end = usize::try_from(*self.offsets.get(index + 1)?).ok()?;
        self.bytes.get(self.id_start + start..self.id_start + end)
    }

    pub(crate) fn code(&self, index: usize) -> Option<&[u8]> {
        if index >= self.primary_rows + self.replicated_rows {
            return None;
        }
        let start = self
            .code_start
            .checked_add(index.checked_mul(self.code_width)?)?;
        self.bytes.get(start..start.checked_add(self.code_width)?)
    }
}

fn v23_metric_tag(metric: &VectorMetric) -> Option<u8> {
    match metric {
        VectorMetric::Euclidean => Some(1),
        VectorMetric::SquaredEuclidean => Some(2),
        VectorMetric::Cosine => Some(3),
        VectorMetric::InnerProduct => Some(4),
        _ => None,
    }
}

fn v23_family_tag(family: V23QuantizerFamily) -> u8 {
    match family {
        V23QuantizerFamily::SrhtPq => 1,
        V23QuantizerFamily::FastTurboQuantMse => 2,
        V23QuantizerFamily::FastTurboQuantProd => 3,
        V23QuantizerFamily::F16Flat => 4,
    }
}

fn read_v23_u16(bytes: &[u8], start: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        bytes.get(start..start + 2)?.try_into().ok()?,
    ))
}

fn read_v23_u32(bytes: &[u8], start: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        bytes.get(start..start + 4)?.try_into().ok()?,
    ))
}

fn read_v23_u64(bytes: &[u8], start: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        bytes.get(start..start + 8)?.try_into().ok()?,
    ))
}

pub(crate) fn encode_v23_selector(input: &V23SelectorInput) -> Result<Bytes> {
    let metric_tag = v23_metric_tag(&input.metric)
        .filter(|tag| *tag <= 3)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 selector metric is not supported".to_string())
        })?;
    let dimensions = usize::try_from(input.dimensions).map_err(|_| {
        BorsukError::InvalidStorage("V23 selector dimensions exceed usize".to_string())
    })?;
    let coarse_cells = input.coarse_centroids.len();
    let row_count = input.rows.len();
    if input.generation_checksum == [0; 32]
        || input.dimensions != V23_SELECTOR_DIMENSIONS
        || input.page_count == 0
        || input.maximum_assignments_per_row != V23_SELECTOR_MAXIMUM_ASSIGNMENTS_PER_ROW
        || !V23_SELECTOR_CODE_WIDTHS.contains(&input.code_width)
        || coarse_cells == 0
        || input.coarse_centroids.iter().any(|centroid| {
            centroid.len() != dimensions || centroid.iter().any(|value| !value.is_finite())
        })
        || row_count == 0
        || input.rows.iter().any(|row| {
            usize::try_from(row.coarse_cell).map_or(true, |cell| cell >= coarse_cells)
                || row.primary_page >= input.page_count
                || row
                    .replica_page
                    .is_some_and(|page| page >= input.page_count || page == row.primary_page)
                || row.code.len() != usize::from(input.code_width)
        })
        || input.rows.windows(2).any(|pair| {
            (pair[0].coarse_cell, pair[0].source_ordinal)
                >= (pair[1].coarse_cell, pair[1].source_ordinal)
        })
        || input
            .rows
            .iter()
            .map(|row| row.source_ordinal)
            .collect::<BTreeSet<_>>()
            .len()
            != row_count
    {
        return Err(BorsukError::InvalidStorage(
            "V23 selector input authority differs".to_string(),
        ));
    }
    let coarse_cells_u32 = u32::try_from(coarse_cells)
        .map_err(|_| BorsukError::InvalidStorage("V23 selector cells exceed u32".to_string()))?;
    let row_count_u64 = u64::try_from(row_count)
        .map_err(|_| BorsukError::InvalidStorage("V23 selector rows exceed u64".to_string()))?;
    let centroid_bytes = coarse_cells
        .checked_mul(dimensions)
        .and_then(|values| values.checked_mul(4))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 selector centroids overflow".to_string())
        })?;
    let offset_bytes = coarse_cells
        .checked_add(1)
        .and_then(|values| values.checked_mul(4))
        .ok_or_else(|| BorsukError::InvalidStorage("V23 selector offsets overflow".to_string()))?;
    let page_bytes = row_count
        .checked_mul(4)
        .ok_or_else(|| BorsukError::InvalidStorage("V23 selector pages overflow".to_string()))?;
    let code_bytes = row_count
        .checked_mul(usize::from(input.code_width))
        .ok_or_else(|| BorsukError::InvalidStorage("V23 selector codes overflow".to_string()))?;
    let total_bytes = V23_SELECTOR_HEADER_BYTES
        .checked_add(centroid_bytes)
        .and_then(|bytes| bytes.checked_add(offset_bytes))
        .and_then(|bytes| bytes.checked_add(page_bytes))
        .and_then(|bytes| bytes.checked_add(page_bytes))
        .and_then(|bytes| bytes.checked_add(code_bytes))
        .ok_or_else(|| BorsukError::InvalidStorage("V23 selector bytes overflow".to_string()))?;
    let mut encoded = Vec::with_capacity(total_bytes);
    encoded.resize(V23_SELECTOR_HEADER_BYTES, 0);
    encoded[0..4].copy_from_slice(V23_SELECTOR_MAGIC);
    encoded[4] = V23_SELECTOR_VERSION;
    encoded[5] = metric_tag;
    encoded[6] = v23_family_tag(V23QuantizerFamily::SrhtPq);
    encoded[7] = input.maximum_assignments_per_row;
    encoded[8..12].copy_from_slice(&input.dimensions.to_le_bytes());
    encoded[12..16].copy_from_slice(&coarse_cells_u32.to_le_bytes());
    encoded[16..20].copy_from_slice(&input.page_count.to_le_bytes());
    encoded[20..22].copy_from_slice(&input.code_width.to_le_bytes());
    encoded[24..32].copy_from_slice(&row_count_u64.to_le_bytes());
    encoded[32..64].copy_from_slice(&input.generation_checksum);
    encoded[64..68].copy_from_slice(
        &u32::try_from(centroid_bytes)
            .map_err(|_| {
                BorsukError::InvalidStorage("V23 selector centroid bytes exceed u32".to_string())
            })?
            .to_le_bytes(),
    );
    encoded[68..72].copy_from_slice(
        &u32::try_from(offset_bytes)
            .map_err(|_| {
                BorsukError::InvalidStorage("V23 selector offset bytes exceed u32".to_string())
            })?
            .to_le_bytes(),
    );
    encoded[72..76].copy_from_slice(
        &u32::try_from(page_bytes)
            .map_err(|_| {
                BorsukError::InvalidStorage("V23 selector page bytes exceed u32".to_string())
            })?
            .to_le_bytes(),
    );
    encoded[76..80].copy_from_slice(
        &u32::try_from(page_bytes)
            .map_err(|_| {
                BorsukError::InvalidStorage("V23 selector replica bytes exceed u32".to_string())
            })?
            .to_le_bytes(),
    );
    debug_assert_eq!(encoded.capacity(), total_bytes);
    for centroid in &input.coarse_centroids {
        for value in centroid {
            encoded.extend_from_slice(&value.to_bits().to_le_bytes());
        }
    }
    let mut next_row = 0_usize;
    for cell in 0..coarse_cells {
        encoded.extend_from_slice(
            &u32::try_from(next_row)
                .map_err(|_| {
                    BorsukError::InvalidStorage("V23 selector offset exceeds u32".to_string())
                })?
                .to_le_bytes(),
        );
        while next_row < row_count
            && usize::try_from(input.rows[next_row].coarse_cell).ok() == Some(cell)
        {
            next_row += 1;
        }
    }
    encoded.extend_from_slice(
        &u32::try_from(row_count)
            .map_err(|_| {
                BorsukError::InvalidStorage("V23 selector final offset exceeds u32".to_string())
            })?
            .to_le_bytes(),
    );
    for row in &input.rows {
        encoded.extend_from_slice(&row.primary_page.to_le_bytes());
    }
    for row in &input.rows {
        encoded.extend_from_slice(&row.replica_page.unwrap_or(u32::MAX).to_le_bytes());
    }
    for row in &input.rows {
        encoded.extend_from_slice(&row.code);
    }
    if encoded.len() != total_bytes {
        return Err(BorsukError::InvalidStorage(
            "V23 selector encoded length differs".to_string(),
        ));
    }
    Ok(Bytes::from(encoded))
}

pub(crate) fn decode_v23_selector(
    bytes: Bytes,
    selector_ref: &V23SelectorRef,
) -> Result<V23DecodedSelector> {
    let expected_path = format!("selectors/{}", selector_ref.checksum);
    if bytes.len() as u64 != selector_ref.encoded_bytes
        || bytes.len() < V23_SELECTOR_HEADER_BYTES
        || selector_ref.generation_checksum == [0; 32]
        || selector_ref.dimensions != V23_SELECTOR_DIMENSIONS
        || selector_ref.coarse_cells == 0
        || selector_ref.page_count == 0
        || selector_ref.maximum_assignments_per_row != V23_SELECTOR_MAXIMUM_ASSIGNMENTS_PER_ROW
        || !V23_SELECTOR_CODE_WIDTHS.contains(&selector_ref.code_width)
        || selector_ref.row_count == 0
        || !valid_checksum(&selector_ref.checksum)
        || selector_ref.path != expected_path
    {
        return Err(BorsukError::InvalidStorage(
            "V23 selector envelope authority differs".to_string(),
        ));
    }
    let actual_checksum = blake3::hash(&bytes).to_hex().to_string();
    if actual_checksum != selector_ref.checksum {
        return Err(BorsukError::ChecksumMismatch {
            path: selector_ref.path.clone(),
            expected: selector_ref.checksum.clone(),
            actual: actual_checksum,
        });
    }
    if bytes.get(0..4) != Some(V23_SELECTOR_MAGIC.as_slice())
        || bytes[4] != V23_SELECTOR_VERSION
        || v23_metric_tag(&selector_ref.metric) != Some(bytes[5])
        || bytes[6] != v23_family_tag(V23QuantizerFamily::SrhtPq)
        || bytes[7] != selector_ref.maximum_assignments_per_row
        || read_v23_u32(&bytes, 8) != Some(selector_ref.dimensions)
        || read_v23_u32(&bytes, 12) != Some(selector_ref.coarse_cells)
        || read_v23_u32(&bytes, 16) != Some(selector_ref.page_count)
        || read_v23_u16(&bytes, 20) != Some(selector_ref.code_width)
        || read_v23_u16(&bytes, 22) != Some(0)
        || read_v23_u64(&bytes, 24) != Some(selector_ref.row_count)
        || bytes.get(32..64) != Some(selector_ref.generation_checksum.as_slice())
        || bytes[80..V23_SELECTOR_HEADER_BYTES]
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 selector header authority differs".to_string(),
        ));
    }
    let dimensions = usize::try_from(selector_ref.dimensions).map_err(|_| {
        BorsukError::InvalidStorage("V23 selector dimensions exceed usize".to_string())
    })?;
    let coarse_cells = usize::try_from(selector_ref.coarse_cells)
        .map_err(|_| BorsukError::InvalidStorage("V23 selector cells exceed usize".to_string()))?;
    let row_count = usize::try_from(selector_ref.row_count)
        .map_err(|_| BorsukError::InvalidStorage("V23 selector rows exceed usize".to_string()))?;
    let centroid_bytes = coarse_cells
        .checked_mul(dimensions)
        .and_then(|n| n.checked_mul(4));
    let offset_bytes = coarse_cells.checked_add(1).and_then(|n| n.checked_mul(4));
    let page_bytes = row_count.checked_mul(4);
    if centroid_bytes.and_then(|n| u32::try_from(n).ok()) != read_v23_u32(&bytes, 64)
        || offset_bytes.and_then(|n| u32::try_from(n).ok()) != read_v23_u32(&bytes, 68)
        || page_bytes.and_then(|n| u32::try_from(n).ok()) != read_v23_u32(&bytes, 72)
        || page_bytes.and_then(|n| u32::try_from(n).ok()) != read_v23_u32(&bytes, 76)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 selector section lengths differ".to_string(),
        ));
    }
    let centroid_bytes = centroid_bytes.unwrap();
    let offset_bytes = offset_bytes.unwrap();
    let page_bytes = page_bytes.unwrap();
    let centroids_start = V23_SELECTOR_HEADER_BYTES;
    let offsets_start = centroids_start + centroid_bytes;
    let primary_pages_start = offsets_start + offset_bytes;
    let replica_pages_start = primary_pages_start + page_bytes;
    let codes_start = replica_pages_start + page_bytes;
    let expected_bytes =
        codes_start.checked_add(row_count.saturating_mul(usize::from(selector_ref.code_width)));
    if expected_bytes != Some(bytes.len()) {
        return Err(BorsukError::InvalidStorage(
            "V23 selector total length differs".to_string(),
        ));
    }
    let mut centroids = Vec::with_capacity(coarse_cells.saturating_mul(dimensions));
    for index in 0..coarse_cells.saturating_mul(dimensions) {
        let bits = read_v23_u32(&bytes, centroids_start + index * 4).ok_or_else(|| {
            BorsukError::InvalidStorage("V23 selector centroid is absent".to_string())
        })?;
        let value = f32::from_bits(bits);
        if !value.is_finite() {
            return Err(BorsukError::InvalidStorage(
                "V23 selector centroid is non-finite".to_string(),
            ));
        }
        centroids.push(value);
    }
    let mut offsets = Vec::with_capacity(coarse_cells + 1);
    for cell in 0..=coarse_cells {
        offsets.push(
            read_v23_u32(&bytes, offsets_start + cell * 4).ok_or_else(|| {
                BorsukError::InvalidStorage("V23 selector offset is absent".to_string())
            })?,
        );
    }
    if offsets.first() != Some(&0)
        || usize::try_from(*offsets.last().unwrap()).ok() != Some(row_count)
        || offsets.windows(2).any(|pair| pair[0] > pair[1])
    {
        return Err(BorsukError::InvalidStorage(
            "V23 selector offsets differ".to_string(),
        ));
    }
    for cell in 0..coarse_cells {
        let start = usize::try_from(offsets[cell]).unwrap();
        let end = usize::try_from(offsets[cell + 1]).unwrap();
        for row in start..end {
            let primary = read_v23_u32(&bytes, primary_pages_start + row * 4).ok_or_else(|| {
                BorsukError::InvalidStorage("V23 selector primary page is absent".to_string())
            })?;
            let replica = read_v23_u32(&bytes, replica_pages_start + row * 4).ok_or_else(|| {
                BorsukError::InvalidStorage("V23 selector replica page is absent".to_string())
            })?;
            if primary >= selector_ref.page_count
                || (replica != u32::MAX
                    && (replica >= selector_ref.page_count || replica == primary))
            {
                return Err(BorsukError::InvalidStorage(
                    "V23 selector row page authority differs".to_string(),
                ));
            }
        }
    }
    Ok(V23DecodedSelector {
        bytes,
        centroids: centroids.into_boxed_slice(),
        offsets: offsets.into_boxed_slice(),
        primary_pages_start,
        replica_pages_start,
        codes_start,
        dimensions,
        code_width: usize::from(selector_ref.code_width),
        row_count,
    })
}

pub(crate) fn encode_v23_page(input: &V23PageInput) -> Result<Bytes> {
    let metric_tag = v23_metric_tag(&input.metric).ok_or_else(|| {
        BorsukError::InvalidStorage("V23 page metric is not supported".to_string())
    })?;
    let primary_rows = u32::try_from(input.primary_rows.len()).map_err(|_| {
        BorsukError::InvalidStorage("V23 primary row count exceeds u32".to_string())
    })?;
    let replicated_rows = u32::try_from(input.replicated_rows.len()).map_err(|_| {
        BorsukError::InvalidStorage("V23 replica row count exceeds u32".to_string())
    })?;
    let rows = input
        .primary_rows
        .iter()
        .chain(input.replicated_rows.iter())
        .collect::<Vec<_>>();
    let primary_ordered = input
        .primary_rows
        .windows(2)
        .all(|pair| pair[0].canonical_record_id < pair[1].canonical_record_id);
    let replicas_ordered = input
        .replicated_rows
        .windows(2)
        .all(|pair| pair[0].canonical_record_id < pair[1].canonical_record_id);
    let unique_ids = rows
        .iter()
        .map(|row| row.canonical_record_id.as_ref())
        .collect::<BTreeSet<_>>()
        .len()
        == rows.len();
    if input.generation_checksum == [0; 32]
        || input.dimensions == 0
        || !valid_page_code_width(input.family, input.code_width, input.dimensions)
        || rows.is_empty()
        || rows.iter().any(|row| {
            row.canonical_record_id.is_empty() || row.code.len() != usize::from(input.code_width)
        })
        || !primary_ordered
        || !replicas_ordered
        || !unique_ids
    {
        return Err(BorsukError::InvalidStorage(
            "V23 page input authority differs".to_string(),
        ));
    }
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    offsets.push(0_u32);
    let mut ids = Vec::new();
    let mut codes = Vec::with_capacity(rows.len().saturating_mul(usize::from(input.code_width)));
    for row in rows {
        ids.extend_from_slice(&row.canonical_record_id);
        offsets.push(u32::try_from(ids.len()).map_err(|_| {
            BorsukError::InvalidStorage("V23 page ID section exceeds u32".to_string())
        })?);
        codes.extend_from_slice(&row.code);
    }
    let offset_bytes = offsets
        .len()
        .checked_mul(4)
        .ok_or_else(|| BorsukError::InvalidStorage("V23 page offset bytes overflow".to_string()))?;
    let id_section_bytes = offset_bytes
        .checked_add(ids.len())
        .ok_or_else(|| BorsukError::InvalidStorage("V23 page ID bytes overflow".to_string()))?;
    let total_bytes = usize::try_from(V23_PAGE_HEADER_BYTES)
        .unwrap()
        .checked_add(id_section_bytes)
        .and_then(|bytes| bytes.checked_add(codes.len()))
        .ok_or_else(|| BorsukError::InvalidStorage("V23 page bytes overflow".to_string()))?;
    if total_bytes as u64 > V23_PAGE_MAX_ENCODED_BYTES {
        return Err(BorsukError::InvalidStorage(
            "V23 page exceeds encoded-byte cap".to_string(),
        ));
    }
    let mut encoded = vec![0_u8; usize::try_from(V23_PAGE_HEADER_BYTES).unwrap()];
    encoded[0..4].copy_from_slice(V23_PAGE_MAGIC);
    encoded[4] = V23_PAGE_VERSION;
    encoded[5] = metric_tag;
    encoded[6] = v23_family_tag(input.family);
    encoded[7] = 0;
    encoded[8..12].copy_from_slice(&input.dimensions.to_le_bytes());
    encoded[12..16].copy_from_slice(&input.page_ordinal.to_le_bytes());
    encoded[16..20].copy_from_slice(&primary_rows.to_le_bytes());
    encoded[20..24].copy_from_slice(&replicated_rows.to_le_bytes());
    encoded[24..28].copy_from_slice(
        &u32::try_from(id_section_bytes)
            .map_err(|_| BorsukError::InvalidStorage("V23 ID bytes exceed u32".to_string()))?
            .to_le_bytes(),
    );
    encoded[28..32].copy_from_slice(
        &u32::try_from(codes.len())
            .map_err(|_| BorsukError::InvalidStorage("V23 code bytes exceed u32".to_string()))?
            .to_le_bytes(),
    );
    encoded[32..64].copy_from_slice(&input.generation_checksum);
    encoded[64..66].copy_from_slice(&input.code_width.to_le_bytes());
    for offset in offsets {
        encoded.extend_from_slice(&offset.to_le_bytes());
    }
    encoded.extend_from_slice(&ids);
    encoded.extend_from_slice(&codes);
    Ok(Bytes::from(encoded))
}

pub(crate) fn decode_v23_page(bytes: Bytes, page_ref: &V23PageRef) -> Result<V23DecodedPage> {
    let header_bytes = usize::try_from(V23_PAGE_HEADER_BYTES).unwrap();
    let expected_path = format!("pages/{}", page_ref.checksum);
    if bytes.len() as u64 != page_ref.encoded_bytes
        || bytes.len() < header_bytes
        || bytes.len() as u64 > V23_PAGE_MAX_ENCODED_BYTES
        || page_ref.generation_checksum == [0; 32]
        || page_ref.dimensions == 0
        || !valid_page_code_width(page_ref.family, page_ref.code_width, page_ref.dimensions)
        || !valid_checksum(&page_ref.checksum)
        || page_ref.path != expected_path
    {
        return Err(BorsukError::InvalidStorage(
            "V23 page envelope authority differs".to_string(),
        ));
    }
    let actual_checksum = blake3::hash(&bytes).to_hex().to_string();
    if actual_checksum != page_ref.checksum {
        return Err(BorsukError::ChecksumMismatch {
            path: page_ref.path.clone(),
            expected: page_ref.checksum.clone(),
            actual: actual_checksum,
        });
    }
    if bytes.get(0..4) != Some(V23_PAGE_MAGIC.as_slice())
        || bytes[4] != V23_PAGE_VERSION
        || v23_metric_tag(&page_ref.metric) != Some(bytes[5])
        || v23_family_tag(page_ref.family) != bytes[6]
        || bytes[7] != 0
        || read_v23_u16(&bytes, 64) != Some(page_ref.code_width)
        || read_v23_u32(&bytes, 8) != Some(page_ref.dimensions)
        || read_v23_u32(&bytes, 12) != Some(page_ref.page_ordinal)
        || bytes.get(32..64) != Some(page_ref.generation_checksum.as_slice())
        || bytes[66..96].iter().any(|byte| *byte != 0)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 page header authority differs".to_string(),
        ));
    }
    let primary_rows =
        usize::try_from(read_v23_u32(&bytes, 16).ok_or_else(|| {
            BorsukError::InvalidStorage("V23 primary rows are absent".to_string())
        })?)
        .map_err(|_| BorsukError::InvalidStorage("V23 primary rows exceed usize".to_string()))?;
    let replicated_rows =
        usize::try_from(read_v23_u32(&bytes, 20).ok_or_else(|| {
            BorsukError::InvalidStorage("V23 replica rows are absent".to_string())
        })?)
        .map_err(|_| BorsukError::InvalidStorage("V23 replica rows exceed usize".to_string()))?;
    let row_count = primary_rows
        .checked_add(replicated_rows)
        .ok_or_else(|| BorsukError::InvalidStorage("V23 page row count overflows".to_string()))?;
    let id_section_bytes = usize::try_from(read_v23_u32(&bytes, 24).ok_or_else(|| {
        BorsukError::InvalidStorage("V23 ID section length is absent".to_string())
    })?)
    .map_err(|_| BorsukError::InvalidStorage("V23 ID section exceeds usize".to_string()))?;
    let code_section_bytes = usize::try_from(read_v23_u32(&bytes, 28).ok_or_else(|| {
        BorsukError::InvalidStorage("V23 code section length is absent".to_string())
    })?)
    .map_err(|_| BorsukError::InvalidStorage("V23 code section exceeds usize".to_string()))?;
    let offset_bytes = row_count
        .checked_add(1)
        .and_then(|count| count.checked_mul(4))
        .ok_or_else(|| BorsukError::InvalidStorage("V23 page offsets overflow".to_string()))?;
    let id_start = header_bytes
        .checked_add(offset_bytes)
        .ok_or_else(|| BorsukError::InvalidStorage("V23 page ID start overflows".to_string()))?;
    let code_start = header_bytes
        .checked_add(id_section_bytes)
        .ok_or_else(|| BorsukError::InvalidStorage("V23 page code start overflows".to_string()))?;
    if primary_rows == 0
        || primary_rows != usize::try_from(page_ref.primary_rows).unwrap_or(usize::MAX)
        || replicated_rows != usize::try_from(page_ref.replicated_rows).unwrap_or(usize::MAX)
        || id_section_bytes < offset_bytes
        || code_section_bytes != row_count.saturating_mul(usize::from(page_ref.code_width))
        || code_start.checked_add(code_section_bytes) != Some(bytes.len())
    {
        return Err(BorsukError::InvalidStorage(
            "V23 page section authority differs".to_string(),
        ));
    }
    let mut offsets = Vec::with_capacity(row_count + 1);
    for index in 0..=row_count {
        offsets.push(
            read_v23_u32(&bytes, header_bytes + index * 4).ok_or_else(|| {
                BorsukError::InvalidStorage("V23 page offset is absent".to_string())
            })?,
        );
    }
    let id_bytes = id_section_bytes - offset_bytes;
    if offsets.first() != Some(&0)
        || usize::try_from(*offsets.last().unwrap()).ok() != Some(id_bytes)
        || offsets.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(BorsukError::InvalidStorage(
            "V23 page offsets differ".to_string(),
        ));
    }
    let ids = offsets
        .windows(2)
        .map(|pair| {
            let start = usize::try_from(pair[0]).ok()?;
            let end = usize::try_from(pair[1]).ok()?;
            bytes.get(id_start + start..id_start + end)
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| BorsukError::InvalidStorage("V23 page ID slice differs".to_string()))?;
    if ids[..primary_rows]
        .windows(2)
        .any(|pair| pair[0] >= pair[1])
        || ids[primary_rows..]
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || ids.iter().copied().collect::<BTreeSet<_>>().len() != ids.len()
    {
        return Err(BorsukError::InvalidStorage(
            "V23 page ID authority differs".to_string(),
        ));
    }
    if page_ref.family == V23QuantizerFamily::F16Flat
        && bytes[code_start..]
            .as_chunks::<2>()
            .0
            .iter()
            .any(|bits| !half::f16::from_bits(u16::from_le_bytes([bits[0], bits[1]])).is_finite())
    {
        return Err(BorsukError::InvalidStorage(
            "V23 f16 page code is non-finite".to_string(),
        ));
    }
    Ok(V23DecodedPage {
        bytes,
        offsets: offsets.into_boxed_slice(),
        page_ordinal: page_ref.page_ordinal,
        id_start,
        code_start,
        primary_rows,
        replicated_rows,
        code_width: usize::from(page_ref.code_width),
    })
}

fn stream_v23_materialized_pages(
    pages: &[V23PageRef],
    page_bytes: &[Bytes],
    sink: &mut V23PageSink<'_>,
) -> Result<()> {
    if pages.len() != page_bytes.len() {
        return Err(BorsukError::InvalidStorage(
            "V23 materialized page bodies differ from references".to_string(),
        ));
    }
    for (expected_ordinal, (page, bytes)) in pages.iter().zip(page_bytes).enumerate() {
        let checksum = blake3::hash(bytes).to_hex().to_string();
        if page.page_ordinal as usize != expected_ordinal
            || page.encoded_bytes != bytes.len() as u64
            || page.checksum != checksum
            || page.path != format!("pages/{checksum}")
        {
            return Err(BorsukError::InvalidStorage(
                "V23 materialized page authority differs".to_string(),
            ));
        }
        sink(page, bytes)?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// D2 page-routing and code-ranking evidence for one frozen query.
pub struct V23D2QuerySample {
    /// Zero-based position in the frozen query authority.
    pub query_index: u32,
    /// Sorted immutable pages fixed before simulated I/O.
    pub page_ordinals: Vec<u32>,
    /// Exact best-achievable page set for the ten ground-truth rows under the
    /// immutable physical assignment. Diagnostic evidence only; never used by
    /// the serving selector.
    pub oracle_page_ordinals: Vec<u32>,
    /// Exact page assignments for each ordered ground-truth row, sufficient
    /// for independent oracle and selected-containment recomputation.
    pub ground_truth_page_assignments: Vec<Vec<u32>>,
    /// Sum of complete selected page lengths.
    pub encoded_bytes: u64,
    /// Rows scanned before replica deduplication.
    pub candidate_rows: u64,
    /// Rows scored from the resident selector sidecar before physical pages
    /// are chosen.
    pub selector_candidate_rows: u64,
    /// Coarse cells actually returned by the resident selector router.
    pub selector_routed_cells: u16,
    /// Mini-code-ranked rows admitted to deterministic page voting.
    pub selector_ranked_rows: u32,
    /// Exact ground-truth top-ten record IDs.
    pub ground_truth_ids: Vec<Vec<u8>>,
    /// Code-ranked, replica-deduplicated top-ten result.
    pub ranked: V23RankedResult,
    /// Ground-truth rows physically covered by selected pages.
    pub gt_page_hits: u8,
    /// Ground-truth rows covered by `oracle_page_ordinals`.
    pub oracle_gt_page_hits: u8,
    /// Ground-truth rows returned in `ranked`.
    pub hits: u8,
    /// Per-query recall in parts per million.
    pub recall_ppm: u64,
    /// Router preparation plus production SIMD ranking time.
    pub cpu_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Aggregate D2 evidence for one quantizer/page/replication arm.
pub struct V23D2Arm {
    /// Passing D1 quantizer authority used by this arm.
    pub d1_key: V23D1ArmKey,
    /// Compact resident codec used only for content-addressed page selection.
    pub selector_key: V23D1ArmKey,
    /// Content-addressed packed selector consumed identically by D2 and D3.
    pub selector: V23SelectorRef,
    /// Exact resident coarse cells admitted before mini-code scoring.
    pub selector_routing_cells: u16,
    /// Maximum mini-code-ranked rows admitted to page voting.
    pub selector_ranked_row_cap: u32,
    /// Registered primary rows targeted per page.
    pub primary_target_rows: u16,
    /// Maximum primary plus replica assignments permitted per row.
    pub maximum_assignments_per_row: u8,
    /// Maximum posting pages selected before code ranking.
    pub maximum_query_pages: u8,
    /// Maximum authenticated raw record-ID width inherited from D1.
    pub maximum_record_id_bytes: u16,
    /// Complete immutable page directory.
    pub pages: Vec<V23PageRef>,
    /// Unique live corpus rows.
    pub unique_rows: u64,
    /// Primary plus replica row assignments.
    pub total_assignments: u64,
    /// `total_assignments / unique_rows` in parts per million.
    pub storage_amplification_ppm: u64,
    /// Conservative compact-root byte projection at 100M rows.
    pub projected_root_bytes: u64,
    /// Conservative serving-process RAM projection.
    pub projected_ram_bytes: u64,
    /// Conservative peak decoded builder working set for the complete D2 run.
    ///
    /// The same run-wide peak is repeated on every retained frontier arm.
    pub projected_build_bytes: u64,
    /// Query-major page simulation evidence.
    pub query_samples: Vec<V23D2QuerySample>,
    /// Aggregate recall in parts per million.
    pub aggregate_recall_ppm: u64,
    /// Worst frozen-query recall in parts per million.
    pub minimum_query_recall_ppm: u64,
    /// Best-achievable bounded-page containment under the immutable layout.
    pub coverage_oracle_recall_ppm: u64,
    /// Worst-query best-achievable containment under the immutable layout.
    pub coverage_oracle_minimum_query_recall_ppm: u64,
    /// Actual selected-page hits divided by oracle-layout hits.
    pub selector_regret_ppm: u64,
    /// Nearest-rank p99 CPU time across frozen queries.
    pub cpu_p99_ns: u64,
    /// Exact result of every D2 scientific gate.
    pub passed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Complete claim-ineligible V23 D2 report.
pub struct V23D2Report {
    /// Exact evidence schema name.
    pub schema: String,
    /// BLAKE3 of the prerequisite canonical D1 report.
    pub d1_report_checksum: String,
    /// Strictly increasing frozen source-query ordinals inherited from D1.
    pub query_ordinals: Vec<u64>,
    /// Unique live corpus rows.
    pub rows: u64,
    /// Canonically ordered D2 arms.
    pub arms: Vec<V23D2Arm>,
}

/// Complete immutable authority needed to run one D2 diagnostic.
#[derive(Debug, Clone, Copy)]
pub struct V23D2DiagnosticRequest<'a> {
    /// Validated D1 prerequisite report.
    pub d1_report: &'a V23D1Report,
    /// Passing D1 arm selected for page planning.
    pub d1_key: V23D1ArmKey,
    /// Strictly increasing frozen query ordinals.
    pub query_ordinals: &'a [u64],
    /// Query vectors paired with `query_ordinals`.
    pub queries: &'a [Vec<f32>],
    /// Exact top-ten ground-truth IDs paired with the queries.
    pub ground_truth: &'a [Vec<String>],
    /// Parent for attempt-scoped spill storage removed before return.
    pub scratch_parent: &'a Path,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Query-scoped physical evidence for one real D3 S3 wave.
pub struct V23WaveSample {
    /// Zero-based position in the registered query authority.
    pub query_index: u32,
    /// Sorted page ordinals issued concurrently.
    pub page_ordinals: Vec<u32>,
    /// Sum of complete selected page lengths.
    pub encoded_bytes: u64,
    /// Rows scanned before replica deduplication.
    pub candidate_rows: u64,
    /// Query-scoped S3 Standard GET count.
    pub backing_gets: u32,
    /// Aggregate physical GET concurrency admitted for this executor.
    pub backing_get_concurrency: u32,
    /// Query-scoped S3 Standard response bytes.
    pub backing_bytes: u64,
    /// Sum of physical-request queue time within the backing wave.
    pub backing_queue_us_sum: u64,
    /// Longest physical-request queue time within the backing wave.
    pub backing_queue_us_max: u64,
    /// Sum of physical-request service time within the backing wave.
    pub backing_service_us_sum: u64,
    /// Longest physical-request service time within the backing wave.
    pub backing_service_us_max: u64,
    /// Query preparation, decode, and SIMD ranking time.
    pub cpu_ns: u64,
    /// Time waiting for the shared transient-byte permit.
    pub transient_admission_wait_ns: u64,
    /// Time waiting for aggregate physical-request admission.
    pub request_admission_wait_ns: u64,
    /// End-to-end query service time excluding both admission waits.
    pub service_ns: u64,
    /// Complete measured cold-query wall time.
    pub elapsed_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// One authenticated cold-page wave result.
pub struct V23D3WaveResult {
    /// Physical backing-I/O and bounded-work evidence.
    pub sample: V23WaveSample,
    /// Code-ranked, replica-deduplicated result from fetched page bytes.
    pub ranked: V23RankedResult,
    /// Maximum aggregate transient bytes observed by the shared executor gate.
    pub transient_peak_bytes: u64,
    /// Maximum aggregate backing GETs admitted by the shared executor so far.
    pub request_peak_gets: u32,
}

/// Reusable immutable publisher for authenticated diagnostic pages.
#[doc(hidden)]
pub struct V23PagePublisher {
    storage: Storage,
}

impl V23PagePublisher {
    /// Open the backing client once for a complete D2 publication run.
    pub fn new(storage_uri: &str) -> Result<Self> {
        if storage_uri.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "V23 diagnostic page publication URI is absent".to_string(),
            ));
        }
        Ok(Self {
            storage: Storage::from_uri(storage_uri)?,
        })
    }

    /// Publish one authenticated page with immutable create semantics.
    pub fn publish(&self, page: &V23PageRef, bytes: &[u8]) -> Result<()> {
        if page.path != format!("pages/{}", page.checksum)
            || bytes.len() as u64 != page.encoded_bytes
            || page.encoded_bytes == 0
            || page.encoded_bytes > V23_PAGE_MAX_ENCODED_BYTES
            || !valid_checksum(&page.checksum)
        {
            return Err(BorsukError::InvalidStorage(
                "V23 diagnostic page publication authority differs".to_string(),
            ));
        }
        self.storage
            .create_bytes_verified(&page.path, bytes, &page.checksum)?;
        Ok(())
    }

    /// Publish one authenticated packed selector with immutable create semantics.
    pub fn publish_selector(&self, selector: &V23SelectorRef, bytes: &[u8]) -> Result<()> {
        if selector.path != format!("selectors/{}", selector.checksum)
            || bytes.len() as u64 != selector.encoded_bytes
            || selector.encoded_bytes == 0
            || !valid_checksum(&selector.checksum)
            || blake3::hash(bytes).to_hex().as_str() != selector.checksum
        {
            return Err(BorsukError::InvalidStorage(
                "V23 diagnostic selector publication authority differs".to_string(),
            ));
        }
        self.storage
            .create_bytes_verified(&selector.path, bytes, &selector.checksum)?;
        Ok(())
    }
}

fn validate_v23_d3_request_capacity(
    maximum_query_pages: usize,
    backing_get_concurrency: usize,
) -> Result<()> {
    if maximum_query_pages == 0
        || backing_get_concurrency == 0
        || maximum_query_pages > backing_get_concurrency
    {
        return Err(BorsukError::InvalidStorage(
            "V23 D3 page fanout exceeds backing GET capacity".to_string(),
        ));
    }
    Ok(())
}

/// Cache-disabled executor for real V23 D3 backing-page waves.
///
/// The storage client and HTTP pool are reused across queries, but no page
/// bytes are cached. Every successful query therefore performs one backing
/// GET per selected page while concurrent callers share one byte gate.
pub struct V23D3Executor {
    storage: Storage,
    gate: Arc<ByteAdmissionGate>,
    request_gate: Arc<ByteAdmissionGate>,
    quantizer: GlobalScanQuantizer,
    selector: V23PageSelector,
    pages: Vec<V23PageRef>,
    metric: VectorMetric,
    dimensions: usize,
    maximum_query_pages: usize,
    maximum_record_id_bytes: u64,
    code_width: u64,
}

impl V23D3Executor {
    /// Open one cache-disabled backing client and authenticate immutable
    /// quantizer and page-directory authority before a measured query.
    pub fn new(
        storage_uri: &str,
        d1_arm: &V23D1Arm,
        selector_arm: &V23D1Arm,
        d2_arm: &V23D2Arm,
        transient_capacity_bytes: u64,
    ) -> Result<Self> {
        if storage_uri.is_empty()
            || transient_capacity_bytes == 0
            || !d1_arm.passed
            || !d2_arm.passed
            || d2_arm.d1_key != d1_arm.key
            || d2_arm.selector_key != selector_arm.key
            || selector_arm.key.family != V23QuantizerFamily::SrhtPq
            || !V23_SELECTOR_CODE_WIDTHS.contains(&selector_arm.key.code_width_bytes)
            || d2_arm.pages.is_empty()
            || d2_arm.maximum_query_pages == 0
            || usize::from(d2_arm.maximum_query_pages) > V23_WAVE_MAX_PAGES
            || d2_arm.maximum_record_id_bytes == 0
        {
            return Err(BorsukError::InvalidStorage(
                "V23 D3 executor authority differs".to_string(),
            ));
        }
        let first = &d2_arm.pages[0];
        let dimensions = usize::try_from(first.dimensions).map_err(|_| {
            BorsukError::InvalidStorage("V23 D3 dimensions exceed usize".to_string())
        })?;
        if dimensions == 0
            || d2_arm.pages.iter().enumerate().any(|(index, page)| {
                page.page_ordinal as usize != index
                    || page.generation_checksum != first.generation_checksum
                    || page.metric != first.metric
                    || page.dimensions != first.dimensions
                    || page.family != d1_arm.key.family
                    || page.code_width != d1_arm.key.code_width_bytes
                    || page.encoded_bytes == 0
                    || page.encoded_bytes > V23_PAGE_MAX_ENCODED_BYTES
                    || !valid_checksum(&page.checksum)
                    || page.path != format!("pages/{}", page.checksum)
                    || page.primary_rows == 0
            })
        {
            return Err(BorsukError::InvalidStorage(
                "V23 D3 page directory differs".to_string(),
            ));
        }
        if d2_arm.selector.generation_checksum != first.generation_checksum
            || d2_arm.selector.metric != first.metric
            || d2_arm.selector.dimensions != first.dimensions
            || usize::try_from(d2_arm.selector.page_count).ok() != Some(d2_arm.pages.len())
            || d2_arm.selector.code_width != selector_arm.key.code_width_bytes
            || d2_arm.selector.maximum_assignments_per_row
                != V23_SELECTOR_MAXIMUM_ASSIGNMENTS_PER_ROW
        {
            return Err(BorsukError::InvalidStorage(
                "V23 D3 selector authority differs".to_string(),
            ));
        }
        let quantizer = restore_v23_diagnostic_quantizer(d1_arm)?;
        quantizer.prepare_contiguous_query(&vec![0.0; dimensions])?;
        let selector_quantizer = restore_v23_diagnostic_quantizer(selector_arm)?;
        let backing_get_concurrency = crate::configured_backing_get_concurrency();
        validate_v23_d3_request_capacity(
            usize::from(d2_arm.maximum_query_pages),
            backing_get_concurrency,
        )?;
        let storage = Storage::from_uri(storage_uri)?;
        let selector_bytes =
            storage.read_range(&d2_arm.selector.path, 0..d2_arm.selector.encoded_bytes)?;
        let selector = V23PageSelector::from_encoded(
            &d2_arm.selector,
            Bytes::from(selector_bytes),
            selector_quantizer,
        )?;
        Ok(Self {
            storage,
            gate: Arc::new(ByteAdmissionGate::new(transient_capacity_bytes)),
            request_gate: Arc::new(ByteAdmissionGate::new(backing_get_concurrency as u64)),
            quantizer,
            selector,
            pages: d2_arm.pages.clone(),
            metric: first.metric.clone(),
            dimensions,
            maximum_query_pages: usize::from(d2_arm.maximum_query_pages),
            maximum_record_id_bytes: u64::from(d2_arm.maximum_record_id_bytes),
            code_width: u64::from(d1_arm.key.code_width_bytes),
        })
    }

    /// Execute one complete cold wave. Routing, admission, backing I/O,
    /// authenticated decode, deduplication, and scoring are measured.
    pub fn execute(&self, query_index: u32, query: &[f32]) -> Result<V23D3WaveResult> {
        if query.len() != self.dimensions || query.iter().any(|value| !value.is_finite()) {
            return Err(BorsukError::InvalidSearchOptions(
                "V23 D3 query authority differs".to_string(),
            ));
        }
        let elapsed_started = Instant::now();
        let routing_started = Instant::now();
        let prepared_query = if self.metric == VectorMetric::Cosine {
            crate::metric::unit_l2_normalized(query)
        } else {
            query.to_vec()
        };
        let mut page_ordinals = self
            .selector
            .select(query, self.maximum_query_pages)?
            .page_ordinals;
        page_ordinals.sort_unstable();
        let mut encoded_bytes = 0_u64;
        let mut candidate_rows = 0_u64;
        let mut requests = Vec::with_capacity(page_ordinals.len());
        let mut selected_pages = Vec::with_capacity(page_ordinals.len());
        for page_ordinal in &page_ordinals {
            let page = usize::try_from(*page_ordinal)
                .ok()
                .and_then(|index| self.pages.get(index))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("V23 D3 selected page is absent".to_string())
                })?;
            encoded_bytes = encoded_bytes
                .checked_add(page.encoded_bytes)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("V23 D3 selected bytes overflow".to_string())
                })?;
            candidate_rows = candidate_rows
                .checked_add(u64::from(page.primary_rows) + u64::from(page.replicated_rows))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("V23 D3 candidate rows overflow".to_string())
                })?;
            requests.push((page.path.clone(), 0..page.encoded_bytes));
            selected_pages.push(page);
        }
        if encoded_bytes == 0 || encoded_bytes > V23_WAVE_MAX_BYTES || candidate_rows == 0 {
            return Err(BorsukError::InvalidStorage(
                "V23 D3 selected wave exceeds its physical bound".to_string(),
            ));
        }
        let per_candidate_bytes = self
            .maximum_record_id_bytes
            .saturating_mul(2)
            .saturating_add(self.code_width.saturating_mul(2))
            .saturating_add(128);
        let adc_table_bytes = self
            .code_width
            .checked_mul(256)
            .and_then(|entries| entries.checked_mul(std::mem::size_of::<f32>() as u64))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V23 D3 ADC table memory overflows".to_string())
            })?;
        let transient_bytes = encoded_bytes
            .checked_add(
                candidate_rows
                    .checked_mul(per_candidate_bytes)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage("V23 D3 candidate memory overflows".to_string())
                    })?,
            )
            .and_then(|bytes| bytes.checked_add((self.dimensions as u64).saturating_mul(8)))
            .and_then(|bytes| bytes.checked_add(adc_table_bytes))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V23 D3 transient memory overflows".to_string())
            })?;
        if transient_bytes > self.gate.capacity_bytes() {
            return Err(BorsukError::InvalidSearchOptions(format!(
                "V23 D3 wave requires {transient_bytes} transient bytes, capacity is {}",
                self.gate.capacity_bytes()
            )));
        }
        let mut cpu_ns = u64::try_from(routing_started.elapsed().as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        let transient_admission_started = Instant::now();
        let _permit = self.gate.acquire_owned(transient_bytes);
        let transient_admission_wait_ns =
            u64::try_from(transient_admission_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let request_admission_started = Instant::now();
        let _request_permit = self.request_gate.acquire_owned(requests.len() as u64);
        let request_admission_wait_ns =
            u64::try_from(request_admission_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let mut reads = (0..requests.len()).map(|_| None).collect::<Vec<_>>();
        let stats = self.storage.for_each_range_wave_completion(
            &requests,
            requests.len(),
            None,
            |index, result| reads[index] = Some(result),
        );
        let bodies = reads
            .into_iter()
            .enumerate()
            .map(|(index, result)| {
                result.ok_or_else(|| {
                    BorsukError::InvalidStorage(format!("V23 D3 backing wave omitted page {index}"))
                })?
            })
            .collect::<Result<Vec<_>>>()?;
        if stats.attempts != requests.len() as u64
            || stats.successes != requests.len() as u64
            || stats.response_bytes != encoded_bytes
        {
            return Err(BorsukError::InvalidStorage(
                "V23 D3 backing telemetry differs".to_string(),
            ));
        }

        let ranking_started = Instant::now();
        let mut candidate_by_id = BTreeMap::<Box<[u8]>, Box<[u8]>>::new();
        for (body, expected_page) in bodies.into_iter().zip(selected_pages) {
            let decoded = decode_v23_page(Bytes::from(body), expected_page)?;
            for row_index in 0..decoded.primary_rows() + decoded.replicated_rows() {
                let id = decoded.record_id(row_index).ok_or_else(|| {
                    BorsukError::InvalidStorage("V23 D3 record ID is absent".to_string())
                })?;
                if id.len() as u64 > self.maximum_record_id_bytes {
                    return Err(BorsukError::InvalidStorage(
                        "V23 D3 record ID exceeds authenticated width".to_string(),
                    ));
                }
                let code = decoded.code(row_index).ok_or_else(|| {
                    BorsukError::InvalidStorage("V23 D3 code is absent".to_string())
                })?;
                candidate_by_id
                    .entry(id.to_vec().into_boxed_slice())
                    .or_insert_with(|| code.to_vec().into_boxed_slice());
            }
        }
        let mut codes = Vec::with_capacity(
            candidate_by_id
                .len()
                .saturating_mul(self.code_width as usize),
        );
        let mut ids = Vec::with_capacity(candidate_by_id.len());
        for (id, code) in candidate_by_id {
            codes.extend_from_slice(&code);
            ids.push(id);
        }
        let prepared = self.quantizer.prepare_contiguous_query(&prepared_query)?;
        let distances = self
            .quantizer
            .score_prepared_contiguous_codes(&prepared, &codes)?;
        let mut ranked = Vec::new();
        for (id, distance) in ids.iter().zip(distances) {
            observe_ranked(&mut ranked, distance, id)?;
        }
        let ranked = finish_d2_ranked(ranked)?;
        cpu_ns = cpu_ns.saturating_add(
            u64::try_from(ranking_started.elapsed().as_nanos())
                .unwrap_or(u64::MAX)
                .max(1),
        );
        let elapsed_ns = u64::try_from(elapsed_started.elapsed().as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        let total_admission_wait_ns = transient_admission_wait_ns
            .checked_add(request_admission_wait_ns)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V23 D3 admission time overflows".to_string())
            })?;
        let service_ns = elapsed_ns
            .checked_sub(total_admission_wait_ns)
            .filter(|service_ns| *service_ns > 0)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V23 D3 measured timing differs".to_string())
            })?;
        let sample = V23WaveSample {
            query_index,
            page_ordinals,
            encoded_bytes,
            candidate_rows,
            backing_gets: u32::try_from(stats.successes).map_err(|_| {
                BorsukError::InvalidStorage("V23 D3 backing GETs exceed u32".to_string())
            })?,
            backing_get_concurrency: u32::try_from(self.request_gate.capacity_bytes()).map_err(
                |_| BorsukError::InvalidStorage("V23 D3 GET capacity exceeds u32".to_string()),
            )?,
            backing_bytes: stats.response_bytes,
            backing_queue_us_sum: stats.queue_us_sum,
            backing_queue_us_max: stats.queue_us_max,
            backing_service_us_sum: stats.service_us_sum,
            backing_service_us_max: stats.service_us_max,
            cpu_ns,
            transient_admission_wait_ns,
            request_admission_wait_ns,
            service_ns,
            elapsed_ns,
        };
        validate_wave_sample(&sample)?;
        Ok(V23D3WaveResult {
            sample,
            ranked,
            transient_peak_bytes: self.gate.peak_bytes(),
            request_peak_gets: u32::try_from(self.request_gate.peak_bytes()).map_err(|_| {
                BorsukError::InvalidStorage("V23 D3 request peak exceeds u32".to_string())
            })?,
        })
    }
}

#[allow(dead_code, reason = "consumed by the planned D3 benchmark slice")]
pub(crate) fn validate_wave_sample(sample: &V23WaveSample) -> Result<()> {
    if sample.page_ordinals.is_empty()
        || sample.page_ordinals.len() > V23_WAVE_MAX_PAGES
        || sample
            .page_ordinals
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || sample.encoded_bytes == 0
        || sample.encoded_bytes > V23_WAVE_MAX_BYTES
        || sample.candidate_rows == 0
        || usize::try_from(sample.backing_gets).ok() != Some(sample.page_ordinals.len())
        || sample.backing_get_concurrency == 0
        || usize::try_from(sample.backing_get_concurrency)
            .ok()
            .is_none_or(|capacity| capacity < sample.page_ordinals.len())
        || sample.backing_bytes != sample.encoded_bytes
        || sample.backing_queue_us_max > sample.backing_queue_us_sum
        || sample.backing_service_us_max > sample.backing_service_us_sum
        || u128::from(sample.backing_service_us_max) * 1_000 > u128::from(sample.service_ns)
        || sample.cpu_ns == 0
        || sample.service_ns == 0
        || sample
            .transient_admission_wait_ns
            .checked_add(sample.request_admission_wait_ns)
            .and_then(|wait| wait.checked_add(sample.service_ns))
            != Some(sample.elapsed_ns)
        || sample.cpu_ns > sample.service_ns
        || sample.elapsed_ns == 0
    {
        return Err(BorsukError::InvalidStorage(
            "V23 wave authority differs".to_string(),
        ));
    }
    Ok(())
}

fn valid_checksum(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn v23_query_vectors_checksum(query_ordinals: &[u64], queries: &[Vec<f32>]) -> Result<String> {
    if query_ordinals.len() != queries.len()
        || queries.is_empty()
        || queries
            .iter()
            .any(|query| query.is_empty() || query.iter().any(|value| !value.is_finite()))
    {
        return Err(BorsukError::InvalidStorage(
            "V23 query-vector authority differs".to_string(),
        ));
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(queries.len() as u64).to_le_bytes());
    for (ordinal, query) in query_ordinals.iter().zip(queries) {
        hasher.update(&ordinal.to_le_bytes());
        hasher.update(&(query.len() as u64).to_le_bytes());
        for value in query {
            hasher.update(&value.to_bits().to_le_bytes());
        }
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn validate_v23_d2_query_binding(
    d1_report: &V23D1Report,
    selected: &V23D1Arm,
    query_ordinals: &[u64],
    queries: &[Vec<f32>],
    replayed_ground_truth: &[Vec<Vec<u8>>],
) -> Result<()> {
    if query_ordinals != d1_report.query_ordinals
        || v23_query_vectors_checksum(query_ordinals, queries)? != d1_report.query_vectors_checksum
        || replayed_ground_truth.len() != selected.query_samples.len()
        || replayed_ground_truth
            .iter()
            .zip(&selected.query_samples)
            .any(|(ground_truth, sample)| ground_truth != &sample.ground_truth_ids)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 D2 query authority differs from D1".to_string(),
        ));
    }
    Ok(())
}

fn validate_ranked_result_cardinality(result: &V23RankedResult, minimum_rows: usize) -> Result<()> {
    if !(minimum_rows..=10).contains(&result.ids.len())
        || result.distances.len() != result.ids.len()
        || result.ids.iter().any(Vec::is_empty)
        || result.ids.iter().collect::<BTreeSet<_>>().len() != result.ids.len()
        || result
            .distances
            .iter()
            .any(|distance| !distance.is_finite())
        || (1..result.ids.len()).any(|index| {
            result.distances[index - 1]
                .total_cmp(&result.distances[index])
                .then_with(|| result.ids[index - 1].cmp(&result.ids[index]))
                .is_gt()
        })
    {
        return Err(BorsukError::InvalidStorage(
            "V23 ranked result authority differs".to_string(),
        ));
    }
    Ok(())
}

fn validate_ranked_result(result: &V23RankedResult) -> Result<()> {
    validate_ranked_result_cardinality(result, 10)
}

fn validate_d2_ranked_result(result: &V23RankedResult) -> Result<()> {
    validate_ranked_result_cardinality(result, 1)
}

fn valid_diagnostic_code_width(key: V23D1ArmKey) -> bool {
    key.code_width_bytes > 0
        && match key.family {
            V23QuantizerFamily::SrhtPq => [8, 12, 16, 32, 64].contains(&key.code_width_bytes),
            V23QuantizerFamily::FastTurboQuantMse | V23QuantizerFamily::FastTurboQuantProd => {
                key.code_width_bytes <= 64
            }
            V23QuantizerFamily::F16Flat => key.code_width_bytes.is_multiple_of(2),
        }
}

fn valid_page_code_width(family: V23QuantizerFamily, code_width: u16, dimensions: u32) -> bool {
    valid_diagnostic_code_width(V23D1ArmKey {
        family,
        code_width_bytes: code_width,
    }) && (family != V23QuantizerFamily::F16Flat
        || u32::from(code_width) == dimensions.saturating_mul(2))
}

pub(crate) fn fit_v23_diagnostic_quantizer(
    family: V23QuantizerFamily,
    code_width_bytes: u16,
    dimensions: usize,
    sample: &[Vec<f32>],
) -> Result<GlobalScanQuantizer> {
    if dimensions == 0
        || sample.is_empty()
        || sample.iter().any(|vector| {
            vector.len() != dimensions || vector.iter().any(|value| !value.is_finite())
        })
        || !valid_diagnostic_code_width(V23D1ArmKey {
            family,
            code_width_bytes,
        })
    {
        return Err(BorsukError::InvalidMetricInput(
            "V23 diagnostic quantizer authority is invalid".to_string(),
        ));
    }
    match family {
        V23QuantizerFamily::SrhtPq => Ok(GlobalScanQuantizer::from(RotatedProductQuantizer::fit(
            ProductQuantizerConfig {
                rotation: ProductRotation::Srht,
                seed: 23,
                dimensions,
                subspaces: usize::from(code_width_bytes),
                centroids: 256,
                sample_limit: sample.len().min(65_536),
                iterations: 8,
            },
            sample,
        )?)),
        V23QuantizerFamily::FastTurboQuantMse => (1_u8..=8)
            .find_map(|bits| {
                let quantizer =
                    FastTurboQuantMseScanQuantizer::new(23, dimensions, bits, 1).ok()?;
                (quantizer.packed_code_len() == usize::from(code_width_bytes))
                    .then(|| GlobalScanQuantizer::from(quantizer))
            })
            .ok_or_else(|| {
                BorsukError::InvalidMetricInput(
                    "V23 Fast-TurboQuant MSE width is unavailable".to_string(),
                )
            }),
        V23QuantizerFamily::FastTurboQuantProd => (2_u8..=8)
            .find_map(|bits| {
                let quantizer = FastTurboQuantProdScanQuantizer::new(23, dimensions, bits).ok()?;
                (quantizer.packed_code_len() == usize::from(code_width_bytes))
                    .then(|| GlobalScanQuantizer::from(quantizer))
            })
            .ok_or_else(|| {
                BorsukError::InvalidMetricInput(
                    "V23 production Fast-TurboQuant width is unavailable".to_string(),
                )
            }),
        V23QuantizerFamily::F16Flat => {
            let expected = u16::try_from(dimensions.saturating_mul(2)).map_err(|_| {
                BorsukError::InvalidMetricInput("V23 f16-flat width exceeds u16".to_string())
            })?;
            if code_width_bytes != expected {
                return Err(BorsukError::InvalidMetricInput(
                    "V23 f16-flat width differs from dimensions".to_string(),
                ));
            }
            Ok(GlobalScanQuantizer::from(F16FlatScanQuantizer::new(
                dimensions,
            )?))
        }
    }
}

fn v23_canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(v23_canonical_json_value).collect())
        }
        serde_json::Value::Object(values) => {
            let ordered = values
                .into_iter()
                .map(|(key, value)| (key, v23_canonical_json_value(value)))
                .collect::<BTreeMap<_, _>>();
            serde_json::Value::Object(ordered.into_iter().collect())
        }
        scalar => scalar,
    }
}

fn v23_quantizer_state_checksum(state: &GlobalScanQuantizerState) -> Result<String> {
    let value = serde_json::to_value(state).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 D1 quantizer state cannot be canonicalized: {error}"
        ))
    })?;
    let bytes = serde_json::to_vec(&v23_canonical_json_value(value)).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 D1 quantizer state cannot be serialized: {error}"
        ))
    })?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

fn v23_d1_report_checksum(report: &V23D1Report) -> Result<String> {
    let mut normalized = report.clone();
    for arm in &mut normalized.arms {
        let state: GlobalScanQuantizerState = serde_json::from_value(arm.quantizer_state.clone())
            .map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "V23 D1 quantizer state cannot be decoded: {error}"
            ))
        })?;
        arm.quantizer_state = serde_json::to_value(state).map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "V23 D1 quantizer state cannot be canonicalized: {error}"
            ))
        })?;
    }
    let value = serde_json::to_value(normalized).map_err(|error| {
        BorsukError::InvalidStorage(format!("V23 D1 report cannot be canonicalized: {error}"))
    })?;
    let bytes = serde_json::to_vec(&v23_canonical_json_value(value)).map_err(|error| {
        BorsukError::InvalidStorage(format!("V23 D1 report cannot be serialized: {error}"))
    })?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

pub(crate) fn restore_v23_diagnostic_quantizer(arm: &V23D1Arm) -> Result<GlobalScanQuantizer> {
    if !valid_diagnostic_code_width(arm.key) || !valid_checksum(&arm.quantizer_checksum) {
        return Err(BorsukError::InvalidStorage(
            "V23 D1 quantizer authority differs".to_string(),
        ));
    }
    let state: GlobalScanQuantizerState = serde_json::from_value(arm.quantizer_state.clone())
        .map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "V23 D1 quantizer state cannot be decoded: {error}"
            ))
        })?;
    if v23_quantizer_state_checksum(&state)? != arm.quantizer_checksum {
        return Err(BorsukError::InvalidStorage(
            "V23 D1 quantizer state checksum differs".to_string(),
        ));
    }
    let family_matches = matches!(
        (&state, arm.key.family),
        (GlobalScanQuantizerState::Pq(_), V23QuantizerFamily::SrhtPq)
            | (
                GlobalScanQuantizerState::FastTurboQuantMse(_),
                V23QuantizerFamily::FastTurboQuantMse
            )
            | (
                GlobalScanQuantizerState::FastTurboQuantProd(_),
                V23QuantizerFamily::FastTurboQuantProd
            )
            | (
                GlobalScanQuantizerState::F16Flat(_),
                V23QuantizerFamily::F16Flat
            )
    );
    if !family_matches {
        return Err(BorsukError::InvalidStorage(
            "V23 D1 quantizer family differs".to_string(),
        ));
    }
    let quantizer = GlobalScanQuantizer::from_state(state)?;
    if quantizer.code_bytes_per_vector() != usize::from(arm.key.code_width_bytes) {
        return Err(BorsukError::InvalidStorage(
            "V23 D1 quantizer code width differs".to_string(),
        ));
    }
    Ok(quantizer)
}

pub(crate) struct V23D1CorpusAuthority<'a> {
    pub(crate) root_checksum: &'a str,
    pub(crate) codebook_checksum: &'a str,
    pub(crate) rows: u64,
    pub(crate) routing_cell_count: usize,
    pub(crate) scratch: &'a V22StageLSpill,
    pub(crate) query_ordinals: &'a [u64],
    pub(crate) queries: &'a [Vec<f32>],
    pub(crate) query_prefixes: &'a [V22StageLQueryPrefix],
    pub(crate) routing_ranks: &'a [Vec<(u32, u32)>],
    pub(crate) routing_gates: &'a [(usize, u64)],
    pub(crate) normalize: bool,
}

fn v23_d1_arm_keys(dimensions: usize) -> Vec<V23D1ArmKey> {
    let mut keys = BTreeSet::new();
    for code_width_bytes in [8_u16, 12, 16, 32, 64] {
        if usize::from(code_width_bytes) <= dimensions {
            keys.insert(V23D1ArmKey {
                family: V23QuantizerFamily::SrhtPq,
                code_width_bytes,
            });
        }
    }
    for bits in 1_u8..=8 {
        if let Ok(quantizer) = FastTurboQuantMseScanQuantizer::new(23, dimensions, bits, 1)
            && let Ok(code_width_bytes) = u16::try_from(quantizer.packed_code_len())
            && code_width_bytes <= 64
        {
            keys.insert(V23D1ArmKey {
                family: V23QuantizerFamily::FastTurboQuantMse,
                code_width_bytes,
            });
        }
    }
    for bits in 2_u8..=8 {
        if let Ok(quantizer) = FastTurboQuantProdScanQuantizer::new(23, dimensions, bits)
            && let Ok(code_width_bytes) = u16::try_from(quantizer.packed_code_len())
            && code_width_bytes <= 64
        {
            keys.insert(V23D1ArmKey {
                family: V23QuantizerFamily::FastTurboQuantProd,
                code_width_bytes,
            });
        }
    }
    if let Ok(code_width_bytes) = u16::try_from(dimensions.saturating_mul(2)) {
        keys.insert(V23D1ArmKey {
            family: V23QuantizerFamily::F16Flat,
            code_width_bytes,
        });
    }
    keys.into_iter().collect()
}

fn observe_ranked(
    ranked: &mut Vec<(f32, Box<[u8]>)>,
    distance: f32,
    canonical_record_id: &[u8],
) -> Result<()> {
    if !distance.is_finite() || canonical_record_id.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "V23 D1 code ranking produced invalid evidence".to_string(),
        ));
    }
    let insertion = ranked.partition_point(|(current_distance, current_id)| {
        current_distance
            .total_cmp(&distance)
            .then_with(|| current_id.as_ref().cmp(canonical_record_id))
            .is_le()
    });
    if insertion < 10 {
        ranked.insert(insertion, (distance, canonical_record_id.into()));
        ranked.truncate(10);
    }
    Ok(())
}

fn finish_ranked(ranked: Vec<(f32, Box<[u8]>)>) -> Result<V23RankedResult> {
    if ranked.len() != 10 {
        return Err(BorsukError::InvalidStorage(
            "V23 D1 code-ranked top ten is incomplete".to_string(),
        ));
    }
    Ok(V23RankedResult {
        ids: ranked.iter().map(|(_, id)| id.to_vec()).collect::<Vec<_>>(),
        distances: ranked.into_iter().map(|(distance, _)| distance).collect(),
    })
}

fn finish_d2_ranked(ranked: Vec<(f32, Box<[u8]>)>) -> Result<V23RankedResult> {
    if ranked.is_empty() || ranked.len() > 10 {
        return Err(BorsukError::InvalidStorage(
            "V23 D2 code-ranked evidence is empty or oversized".to_string(),
        ));
    }
    Ok(V23RankedResult {
        ids: ranked.iter().map(|(_, id)| id.to_vec()).collect::<Vec<_>>(),
        distances: ranked.into_iter().map(|(distance, _)| distance).collect(),
    })
}

fn v23_rankings_equivalent_within_tolerance(
    left: &V23RankedResult,
    right: &V23RankedResult,
) -> bool {
    if left.ids == right.ids {
        return true;
    }
    let left_ids = left.ids.iter().collect::<BTreeSet<_>>();
    let right_ids = right.ids.iter().collect::<BTreeSet<_>>();
    if left_ids == right_ids {
        return true;
    }
    let Some(&left_boundary) = left.distances.last() else {
        return false;
    };
    let Some(&right_boundary) = right.distances.last() else {
        return false;
    };
    let within_tolerance = |distance: f32, boundary: f32| {
        let scale = f64::from(distance.abs().max(boundary.abs()).max(1.0));
        f64::from((distance - boundary).abs()) * 1_000_000.0
            <= scale * V23_SCALAR_SIMD_MAX_DISTANCE_DELTA_PPM as f64
    };
    left.ids
        .iter()
        .zip(&left.distances)
        .filter(|(id, _)| !right_ids.contains(id))
        .all(|(_, distance)| within_tolerance(*distance, right_boundary))
        && right
            .ids
            .iter()
            .zip(&right.distances)
            .filter(|(id, _)| !left_ids.contains(id))
            .all(|(_, distance)| within_tolerance(*distance, left_boundary))
}

fn v23_d1_projected_page_rows(code_width: u16, maximum_record_id_bytes: u16) -> u64 {
    let row_bytes = 4_u64
        .saturating_add(u64::from(code_width))
        .saturating_add(u64::from(maximum_record_id_bytes));
    V23_PAGE_MAX_ENCODED_BYTES
        .saturating_sub(V23_PAGE_HEADER_BYTES + 4)
        .checked_div(row_bytes)
        .unwrap_or(0)
        .min(V23_D1_PROJECTED_PAGE_ROWS)
}

fn v23_d1_projected_page_bytes(code_width: u16, maximum_record_id_bytes: u16) -> u64 {
    let rows = v23_d1_projected_page_rows(code_width, maximum_record_id_bytes);
    V23_PAGE_HEADER_BYTES
        .saturating_add(4 * (rows + 1))
        .saturating_add(
            rows.saturating_mul(u64::from(code_width) + u64::from(maximum_record_id_bytes)),
        )
}

fn v23_d1_arm_is_eligible_for_wave(key: V23D1ArmKey, maximum_record_id_bytes: u16) -> bool {
    v23_d1_projected_page_rows(key.code_width_bytes, maximum_record_id_bytes)
        .saturating_mul(V23_WAVE_MAX_PAGES as u64)
        >= V23_D1_PROJECTED_PAGE_ROWS
}

fn v23_d1_bounded_wave_codes(
    oracle_codes: &[u8],
    code_width_bytes: u16,
    maximum_record_id_bytes: u16,
) -> Result<Vec<u8>> {
    let code_width = usize::from(code_width_bytes);
    if code_width == 0 || oracle_codes.is_empty() || !oracle_codes.len().is_multiple_of(code_width)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 D1 oracle codes differ from the arm width".to_string(),
        ));
    }
    let oracle_rows = oracle_codes.len() / code_width;
    let wave_rows = usize::try_from(
        v23_d1_projected_page_rows(code_width_bytes, maximum_record_id_bytes)
            .saturating_mul(V23_WAVE_MAX_PAGES as u64),
    )
    .map_err(|_| BorsukError::InvalidStorage("V23 D1 wave rows overflow".to_string()))?;
    if wave_rows < oracle_rows {
        return Err(BorsukError::InvalidStorage(
            "V23 D1 bounded wave cannot carry the oracle shortlist".to_string(),
        ));
    }
    let mut wave = Vec::with_capacity(wave_rows.saturating_mul(code_width));
    for row in 0..wave_rows {
        let source = (row % oracle_rows) * code_width;
        wave.extend_from_slice(&oracle_codes[source..source + code_width]);
    }
    Ok(wave)
}

pub(crate) fn build_v23_d1_report(authority: V23D1CorpusAuthority<'_>) -> Result<V23D1Report> {
    if authority.query_ordinals.len() != V23_DIAGNOSTIC_QUERIES
        || authority
            .query_ordinals
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || authority.queries.len() != V23_DIAGNOSTIC_QUERIES
        || authority.query_prefixes.len() != authority.queries.len()
        || authority.routing_ranks.len() != authority.queries.len()
        || authority.routing_gates.len() != authority.queries.len()
        || authority.scratch.total_rows() != authority.rows
    {
        return Err(BorsukError::InvalidStorage(
            "V23 D1 corpus authority differs".to_string(),
        ));
    }
    let dimensions = authority.scratch.dimensions();
    let element_type = authority.scratch.element_type();
    let sample_rows = usize::try_from(authority.rows.min(65_536)).map_err(|_| {
        BorsukError::InvalidStorage("V23 D1 sample cardinality exceeds usize".to_string())
    })?;
    let sample_ordinals = (0..sample_rows)
        .map(|index| (index as u64).saturating_mul(authority.rows) / sample_rows as u64)
        .collect::<Vec<_>>();
    let sample_set = sample_ordinals.iter().copied().collect::<BTreeSet<_>>();
    if sample_set.len() != sample_rows {
        return Err(BorsukError::InvalidStorage(
            "V23 D1 ordinal reservoir is not unique".to_string(),
        ));
    }
    let mut sample = Vec::with_capacity(sample_rows);
    let mut maximum_record_id_bytes = 0_usize;
    for (cell, _) in authority.scratch.cell_rows() {
        for row in authority.scratch.read_cell(cell)? {
            maximum_record_id_bytes = maximum_record_id_bytes.max(row.canonical_record_id.len());
            if sample_set.contains(&row.source_ordinal) {
                sample.push((
                    row.source_ordinal,
                    row.geometry(dimensions, element_type, authority.normalize)?
                        .into_vec(),
                ));
            }
        }
    }
    sample.sort_unstable_by_key(|(ordinal, _)| *ordinal);
    if sample
        .iter()
        .map(|(ordinal, _)| *ordinal)
        .collect::<Vec<_>>()
        != sample_ordinals
    {
        return Err(BorsukError::InvalidStorage(
            "V23 D1 scratch omits its registered ordinal reservoir".to_string(),
        ));
    }
    let maximum_record_id_bytes = u16::try_from(maximum_record_id_bytes).map_err(|_| {
        BorsukError::InvalidStorage("V23 D1 record ID width exceeds u16".to_string())
    })?;
    let mut ordinal_hasher = blake3::Hasher::new();
    for (ordinal, _) in &sample {
        ordinal_hasher.update(&ordinal.to_le_bytes());
    }
    let sample_vectors = sample
        .into_iter()
        .map(|(_, vector)| vector)
        .collect::<Vec<_>>();
    let prepared_queries = authority
        .queries
        .iter()
        .map(|query| {
            if authority.normalize {
                crate::metric::unit_l2_normalized(query)
            } else {
                query.clone()
            }
        })
        .collect::<Vec<_>>();
    let oracle_ordinals = authority
        .query_prefixes
        .iter()
        .map(|prefix| {
            prefix
                .rows
                .iter()
                .map(|row| row.record_id)
                .collect::<BTreeSet<_>>()
        })
        .collect::<Vec<_>>();
    if oracle_ordinals
        .iter()
        .any(|ordinals| ordinals.len() != 2_048)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 D1 exact oracle pool is incomplete".to_string(),
        ));
    }
    let mut oracle_membership = BTreeMap::<u64, Vec<usize>>::new();
    for (query_index, ordinals) in oracle_ordinals.iter().enumerate() {
        for ordinal in ordinals {
            oracle_membership
                .entry(*ordinal)
                .or_default()
                .push(query_index);
        }
    }

    let mut arms = Vec::new();
    for key in v23_d1_arm_keys(dimensions) {
        let wave_candidate_rows =
            v23_d1_projected_page_rows(key.code_width_bytes, maximum_record_id_bytes)
                .saturating_mul(V23_WAVE_MAX_PAGES as u64);
        if !v23_d1_arm_is_eligible_for_wave(key, maximum_record_id_bytes) {
            continue;
        }
        let quantizer = fit_v23_diagnostic_quantizer(
            key.family,
            key.code_width_bytes,
            dimensions,
            &sample_vectors,
        )?;
        let typed_quantizer_state = quantizer.state();
        let quantizer_state = serde_json::to_value(&typed_quantizer_state).map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "V23 D1 quantizer state cannot be canonicalized: {error}"
            ))
        })?;
        let quantizer_checksum = v23_quantizer_state_checksum(&typed_quantizer_state)?;
        let mut routed_ranked = (0..authority.queries.len())
            .map(|_| Vec::<(f32, Box<[u8]>)>::new())
            .collect::<Vec<_>>();
        let mut oracle_codes = (0..authority.queries.len())
            .map(|_| Vec::<u8>::new())
            .collect::<Vec<_>>();
        let mut oracle_ids = (0..authority.queries.len())
            .map(|_| Vec::<Box<[u8]>>::new())
            .collect::<Vec<_>>();
        let mut routed_rows = vec![0_u64; authority.queries.len()];
        let mut cpu_ns = Vec::with_capacity(authority.queries.len());
        let mut prepared = Vec::with_capacity(authority.queries.len());
        for query in &prepared_queries {
            let started = Instant::now();
            prepared.push(quantizer.prepare_contiguous_query(query)?);
            cpu_ns.push(started.elapsed().as_nanos().max(1) as u64);
        }
        for (cell, _) in authority.scratch.cell_rows() {
            let rows = authority.scratch.read_cell(cell)?;
            let mut codes = Vec::with_capacity(
                rows.len()
                    .checked_mul(usize::from(key.code_width_bytes))
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "V23 D1 contiguous code bytes overflow".to_string(),
                        )
                    })?,
            );
            for row in &rows {
                let geometry = row.geometry(dimensions, element_type, authority.normalize)?;
                codes.extend_from_slice(&quantizer.encode(&geometry)?);
            }
            for (row_index, row) in rows.iter().enumerate() {
                if let Some(query_indexes) = oracle_membership.get(&row.source_ordinal) {
                    for query_index in query_indexes {
                        let start = row_index * usize::from(key.code_width_bytes);
                        oracle_codes[*query_index].extend_from_slice(
                            &codes[start..start + usize::from(key.code_width_bytes)],
                        );
                        oracle_ids[*query_index].push(row.canonical_record_id.clone());
                    }
                }
            }
            for query_index in 0..authority.queries.len() {
                let rank = authority.routing_ranks[query_index]
                    .binary_search_by_key(&cell, |(candidate, _)| *candidate)
                    .ok()
                    .map(|index| authority.routing_ranks[query_index][index].1 as usize)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "V23 D1 scratch cell is absent from routing authority".to_string(),
                        )
                    })?;
                if rank <= authority.routing_gates[query_index].0 {
                    let distances = quantizer
                        .score_prepared_contiguous_codes(&prepared[query_index], &codes)?;
                    for (row, distance) in rows.iter().zip(distances) {
                        observe_ranked(
                            &mut routed_ranked[query_index],
                            distance,
                            &row.canonical_record_id,
                        )?;
                    }
                    routed_rows[query_index] = routed_rows[query_index]
                        .checked_add(rows.len() as u64)
                        .ok_or_else(|| {
                            BorsukError::InvalidStorage(
                                "V23 D1 routed candidate rows overflow".to_string(),
                            )
                        })?;
                }
            }
        }
        let mut query_samples = Vec::with_capacity(authority.queries.len());
        let mut scalar_simd_ids_equal = true;
        let mut scalar_simd_rank_equivalent = true;
        let mut scalar_simd_max_distance_delta_ppm = 0_u64;
        for query_index in 0..authority.queries.len() {
            if oracle_ids[query_index].len() != 2_048
                || routed_rows[query_index] != authority.routing_gates[query_index].1
            {
                return Err(BorsukError::InvalidStorage(
                    "V23 D1 candidate pools conflict with authority".to_string(),
                ));
            }
            let wave_codes = v23_d1_bounded_wave_codes(
                &oracle_codes[query_index],
                key.code_width_bytes,
                maximum_record_id_bytes,
            )?;
            let started = Instant::now();
            let wave_distances =
                quantizer.score_prepared_contiguous_codes(&prepared[query_index], &wave_codes)?;
            cpu_ns[query_index] =
                cpu_ns[query_index].saturating_add(started.elapsed().as_nanos().max(1) as u64);
            let oracle_distances = wave_distances
                .get(..oracle_ids[query_index].len())
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V23 D1 bounded-wave scores are incomplete".to_string(),
                    )
                })?
                .to_vec();
            let scalar_distances = quantizer.score_codes(
                &prepared_queries[query_index],
                oracle_codes[query_index].chunks_exact(usize::from(key.code_width_bytes)),
            )?;
            let mut oracle_ranked = Vec::new();
            let mut scalar_ranked = Vec::new();
            for ((id, simd_distance), scalar_distance) in oracle_ids[query_index]
                .iter()
                .zip(oracle_distances)
                .zip(scalar_distances)
            {
                observe_ranked(&mut oracle_ranked, simd_distance, id)?;
                observe_ranked(&mut scalar_ranked, scalar_distance, id)?;
            }
            let oracle = finish_ranked(oracle_ranked)?;
            let scalar_oracle = finish_ranked(scalar_ranked)?;
            scalar_simd_ids_equal &= oracle.ids == scalar_oracle.ids;
            scalar_simd_rank_equivalent &=
                v23_rankings_equivalent_within_tolerance(&oracle, &scalar_oracle);
            for (simd, scalar) in oracle.distances.iter().zip(&scalar_oracle.distances) {
                let normalized =
                    f64::from((simd - scalar).abs()) / f64::from(scalar.abs().max(1.0));
                scalar_simd_max_distance_delta_ppm = scalar_simd_max_distance_delta_ppm
                    .max((normalized * 1_000_000.0).ceil().min(u64::MAX as f64) as u64);
            }
            let routed = finish_ranked(std::mem::take(&mut routed_ranked[query_index]))?;
            let ground_truth_ids = authority.query_prefixes[query_index].rows[..10]
                .iter()
                .map(|row| row.canonical_record_id.to_vec())
                .collect::<Vec<_>>();
            let truth = ground_truth_ids.iter().collect::<BTreeSet<_>>();
            let oracle_hits = oracle.ids.iter().filter(|id| truth.contains(id)).count() as u8;
            let routed_hits = routed.ids.iter().filter(|id| truth.contains(id)).count() as u8;
            query_samples.push(V23D1QuerySample {
                query_index: query_index as u32,
                ground_truth_ids,
                oracle,
                scalar_oracle,
                routed,
                oracle_candidate_rows: 2_048,
                routed_candidate_rows: routed_rows[query_index],
                wave_candidate_rows,
                oracle_hits,
                routed_hits,
                cpu_ns: cpu_ns[query_index].max(1),
            });
        }
        let denominator = (query_samples.len() as u64).saturating_mul(10);
        let oracle_recall_ppm = query_samples
            .iter()
            .map(|sample| u64::from(sample.oracle_hits))
            .sum::<u64>()
            .saturating_mul(1_000_000)
            / denominator;
        let routed_recall_ppm = query_samples
            .iter()
            .map(|sample| u64::from(sample.routed_hits))
            .sum::<u64>()
            .saturating_mul(1_000_000)
            / denominator;
        let mut cpu = query_samples
            .iter()
            .map(|sample| sample.cpu_ns)
            .collect::<Vec<_>>();
        cpu.sort_unstable();
        let cpu_p99_ns = cpu[cpu.len() - 1];
        let projected_page_bytes =
            v23_d1_projected_page_bytes(key.code_width_bytes, maximum_record_id_bytes);
        let wave_projected_bytes = projected_page_bytes
            .checked_mul(V23_WAVE_MAX_PAGES as u64)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V23 D1 wave projection overflows".to_string())
            })?;
        arms.push(V23D1Arm {
            key,
            quantizer_checksum,
            quantizer_state,
            query_samples,
            oracle_recall_ppm,
            routed_recall_ppm,
            scalar_simd_ids_equal,
            scalar_simd_max_distance_delta_ppm,
            cpu_p99_ns,
            wave_projected_bytes,
            passed: oracle_recall_ppm >= 990_000
                && routed_recall_ppm >= 975_000
                && scalar_simd_rank_equivalent
                && scalar_simd_max_distance_delta_ppm <= V23_SCALAR_SIMD_MAX_DISTANCE_DELTA_PPM
                && cpu_p99_ns <= V23_D1_CPU_MAX_NS
                && projected_page_bytes <= V23_PAGE_MAX_ENCODED_BYTES
                && wave_projected_bytes <= V23_WAVE_MAX_BYTES,
        });
    }
    let report = V23D1Report {
        schema: "borsuk-v23-d1-v5".to_string(),
        v20_root_checksum: authority.root_checksum.to_string(),
        v20_codebook_checksum: authority.codebook_checksum.to_string(),
        sample_ordinals_checksum: ordinal_hasher.finalize().to_hex().to_string(),
        query_vectors_checksum: v23_query_vectors_checksum(
            authority.query_ordinals,
            authority.queries,
        )?,
        query_ordinals: authority.query_ordinals.to_vec(),
        rows: authority.rows,
        dimensions: u32::try_from(dimensions)
            .map_err(|_| BorsukError::InvalidStorage("V23 D1 dimensions exceed u32".to_string()))?,
        routing_cell_count: authority.routing_cell_count,
        maximum_record_id_bytes,
        arms,
    };
    validate_d1_report(&report)?;
    Ok(report)
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct V23PlanningRow {
    pub(crate) source_ordinal: u64,
    pub(crate) canonical_record_id: Box<[u8]>,
    pub(crate) primary_cell: u32,
    pub(crate) geometry: Box<[f32]>,
    pub(crate) code: Box<[u8]>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct V23PagePlan {
    pub(crate) page_ordinal: u32,
    pub(crate) primary_cell: u32,
    pub(crate) centroid: Box<[f32]>,
    pub(crate) primary_source_ordinals: Box<[u64]>,
    pub(crate) replicated_source_ordinals: Box<[u64]>,
    pub(crate) encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct V23PagePlanningResult {
    pub(crate) pages: Vec<V23PagePlan>,
    pub(crate) maximum_secondary_pages_evaluated_per_row: usize,
    pub(crate) maximum_replica_candidates_retained: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23PageCoverage {
    pub(crate) page_ordinals: Vec<u32>,
    pub(crate) hits: usize,
}

/// Exact small-cardinality oracle used only for diagnostic truth (ten GT rows).
pub(crate) fn best_v23_page_coverage(
    truth_assignments: &[Vec<u32>],
    maximum_pages: usize,
) -> Result<V23PageCoverage> {
    if truth_assignments.is_empty()
        || maximum_pages == 0
        || maximum_pages > V23_WAVE_MAX_PAGES
        || truth_assignments.iter().any(|pages| {
            pages.is_empty() || pages.len() > 3 || pages.windows(2).any(|pair| pair[0] >= pair[1])
        })
    {
        return Err(BorsukError::InvalidStorage(
            "V23 coverage-oracle authority differs".to_string(),
        ));
    }
    let candidates = truth_assignments
        .iter()
        .flatten()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut best = V23PageCoverage {
        page_ordinals: Vec::new(),
        hits: 0,
    };
    fn visit(
        candidates: &[u32],
        truth_assignments: &[Vec<u32>],
        maximum_pages: usize,
        start: usize,
        selected: &mut Vec<u32>,
        best: &mut V23PageCoverage,
    ) {
        if !selected.is_empty() {
            let hits = truth_assignments
                .iter()
                .filter(|pages| {
                    pages
                        .iter()
                        .any(|page| selected.binary_search(page).is_ok())
                })
                .count();
            if hits > best.hits
                || (hits == best.hits
                    && (best.page_ordinals.is_empty() || *selected < best.page_ordinals))
            {
                best.hits = hits;
                best.page_ordinals.clone_from(selected);
            }
        }
        if selected.len() == maximum_pages {
            return;
        }
        for index in start..candidates.len() {
            selected.push(candidates[index]);
            visit(
                candidates,
                truth_assignments,
                maximum_pages,
                index + 1,
                selected,
                best,
            );
            selected.pop();
        }
    }
    visit(
        &candidates,
        truth_assignments,
        maximum_pages,
        0,
        &mut Vec::with_capacity(maximum_pages),
        &mut best,
    );
    Ok(best)
}

struct V23ContentSelector {
    key: V23D1ArmKey,
    quantizer: GlobalScanQuantizer,
    coarse_centroids: Vec<Vec<f32>>,
    cell_remap: BTreeMap<u32, u32>,
}

impl V23ContentSelector {
    fn build(
        d1_report: &V23D1Report,
        planning_rows: &[V23PlanningRow],
        selector_key: V23D1ArmKey,
        _metric: &VectorMetric,
    ) -> Result<Self> {
        if d1_report.dimensions != V23_SELECTOR_DIMENSIONS {
            return Err(BorsukError::InvalidSearchOptions(
                "V23 selector requires the registered 96-dimensional authority".to_string(),
            ));
        }
        let selector_arm = d1_report
            .arms
            .iter()
            .find(|arm| arm.key == selector_key)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "V23 selector quantizer authority is absent".to_string(),
                )
            })?;
        let quantizer = restore_v23_diagnostic_quantizer(selector_arm)?;
        let dimensions = planning_rows.first().map_or(0, |row| row.geometry.len());
        let mut by_cell = BTreeMap::<u32, Vec<usize>>::new();
        for (index, row) in planning_rows.iter().enumerate() {
            by_cell.entry(row.primary_cell).or_default().push(index);
        }
        if dimensions == 0 || by_cell.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "V23 selector cell authority differs".to_string(),
            ));
        }
        let cell_remap = by_cell
            .keys()
            .copied()
            .enumerate()
            .map(|(dense, source)| {
                u32::try_from(dense)
                    .map(|dense| (source, dense))
                    .map_err(|_| {
                        BorsukError::InvalidStorage(
                            "V23 selector cell count exceeds u32".to_string(),
                        )
                    })
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let cell_rows = by_cell.into_values().collect::<Vec<_>>();
        let centroids = cell_rows
            .iter()
            .map(|indexes| {
                let mut centroid = vec![0.0_f64; dimensions];
                for index in indexes {
                    for (sum, value) in centroid
                        .iter_mut()
                        .zip(planning_rows[*index].geometry.iter())
                    {
                        *sum += f64::from(*value);
                    }
                }
                let denominator = indexes.len() as f64;
                centroid
                    .into_iter()
                    .map(|value| (value / denominator) as f32)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        Ok(Self {
            key: selector_arm.key,
            quantizer,
            coarse_centroids: centroids,
            cell_remap,
        })
    }
}

fn build_v23_packed_page_selector(
    selector: &V23ContentSelector,
    planning_rows: &[V23PlanningRow],
    planning: &V23PagePlanningResult,
    page_assignments: &[Vec<u32>],
    primary_pages: &[u32],
    generation_checksum: [u8; 32],
    metric: &VectorMetric,
) -> Result<(V23SelectorRef, Bytes, V23PageSelector)> {
    let page_count = u32::try_from(planning.pages.len())
        .map_err(|_| BorsukError::InvalidStorage("V23 selector pages exceed u32".to_string()))?;
    let dimensions = planning_rows.first().map_or(0, |row| row.geometry.len());
    if dimensions == 0 || planning.pages.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "V23 selector planning authority is empty".to_string(),
        ));
    }
    if page_assignments.len() != planning_rows.len() || primary_pages.len() != planning_rows.len() {
        return Err(BorsukError::InvalidStorage(
            "V23 selector row assignment cardinality differs".to_string(),
        ));
    }
    let mut rows = planning_rows
        .iter()
        .zip(page_assignments)
        .zip(primary_pages)
        .map(|((row, assignments), primary_page)| {
            if assignments.is_empty() || assignments.len() > 2 {
                return Err(BorsukError::InvalidStorage(
                    "V23 selector row assignment authority differs".to_string(),
                ));
            }
            let replica_page = assignments
                .iter()
                .copied()
                .find(|page| page != primary_page);
            Ok(V23SelectorRow::new(
                *selector.cell_remap.get(&row.primary_cell).ok_or_else(|| {
                    BorsukError::InvalidStorage("V23 selector row cell is absent".to_string())
                })?,
                *primary_page,
                replica_page,
                row.source_ordinal,
                &selector.quantizer.encode(&row.geometry)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    rows.sort_unstable_by_key(|row| (row.coarse_cell, row.source_ordinal));
    let input = V23SelectorInput {
        generation_checksum,
        metric: metric.clone(),
        dimensions: u32::try_from(dimensions).map_err(|_| {
            BorsukError::InvalidStorage("V23 selector dimensions exceed u32".to_string())
        })?,
        page_count,
        code_width: selector.key.code_width_bytes,
        maximum_assignments_per_row: V23_SELECTOR_MAXIMUM_ASSIGNMENTS_PER_ROW,
        coarse_centroids: selector.coarse_centroids.clone(),
        rows,
    };
    let bytes = encode_v23_selector(&input)?;
    let checksum = blake3::hash(&bytes).to_hex().to_string();
    let selector_ref = V23SelectorRef {
        generation_checksum,
        metric: metric.clone(),
        dimensions: input.dimensions,
        coarse_cells: u32::try_from(input.coarse_centroids.len()).map_err(|_| {
            BorsukError::InvalidStorage("V23 selector cells exceed u32".to_string())
        })?,
        page_count,
        maximum_assignments_per_row: input.maximum_assignments_per_row,
        code_width: input.code_width,
        row_count: u64::try_from(input.rows.len())
            .map_err(|_| BorsukError::InvalidStorage("V23 selector rows exceed u64".to_string()))?,
        path: format!("selectors/{checksum}"),
        checksum,
        encoded_bytes: bytes.len() as u64,
    };
    let page_selector =
        V23PageSelector::from_encoded(&selector_ref, bytes.clone(), selector.quantizer.clone())?;
    Ok((selector_ref, bytes, page_selector))
}

#[derive(Debug, Clone, Copy)]
struct V23ReplicaCandidate {
    ratio: f32,
    page: usize,
    source_ordinal: u64,
    row_index: usize,
}

impl PartialEq for V23ReplicaCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.ratio.total_cmp(&other.ratio) == Ordering::Equal
            && self.page == other.page
            && self.source_ordinal == other.source_ordinal
    }
}

impl Eq for V23ReplicaCandidate {}

impl PartialOrd for V23ReplicaCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for V23ReplicaCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ratio
            .total_cmp(&other.ratio)
            .then_with(|| self.page.cmp(&other.page))
            .then_with(|| self.source_ordinal.cmp(&other.source_ordinal))
    }
}

fn planning_distance(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let delta = left - right;
            delta * delta
        })
        .sum()
}

fn planning_farthest(rows: &[V23PlanningRow], indexes: &[usize], from: usize) -> usize {
    let mut farthest = indexes[0];
    let mut farthest_distance = planning_distance(&rows[farthest].geometry, &rows[from].geometry);
    for &index in &indexes[1..] {
        let distance = planning_distance(&rows[index].geometry, &rows[from].geometry);
        if distance.total_cmp(&farthest_distance).is_gt()
            || (distance.total_cmp(&farthest_distance).is_eq()
                && rows[index]
                    .canonical_record_id
                    .cmp(&rows[farthest].canonical_record_id)
                    .then_with(|| {
                        rows[index]
                            .source_ordinal
                            .cmp(&rows[farthest].source_ordinal)
                    })
                    .is_lt())
        {
            farthest = index;
            farthest_distance = distance;
        }
    }
    farthest
}

fn split_planning_rows(
    rows: &[V23PlanningRow],
    indexes: &mut [usize],
    leaf_count: usize,
    leaves: &mut Vec<Vec<usize>>,
) {
    if leaf_count == 1 {
        leaves.push(indexes.to_vec());
        return;
    }
    let anchor = *indexes
        .iter()
        .min_by(|left, right| {
            rows[**left]
                .canonical_record_id
                .cmp(&rows[**right].canonical_record_id)
                .then_with(|| {
                    rows[**left]
                        .source_ordinal
                        .cmp(&rows[**right].source_ordinal)
                })
        })
        .expect("nonempty V23 semantic split");
    let first_pivot = planning_farthest(rows, indexes, anchor);
    let second_pivot = planning_farthest(rows, indexes, first_pivot);
    let mut scored = indexes
        .iter()
        .map(|index| {
            let score = planning_distance(&rows[*index].geometry, &rows[first_pivot].geometry)
                - planning_distance(&rows[*index].geometry, &rows[second_pivot].geometry);
            (*index, score)
        })
        .collect::<Vec<_>>();
    scored.sort_by(|(left, left_score), (right, right_score)| {
        left_score
            .total_cmp(right_score)
            .then_with(|| {
                rows[*left]
                    .canonical_record_id
                    .cmp(&rows[*right].canonical_record_id)
            })
            .then_with(|| rows[*left].source_ordinal.cmp(&rows[*right].source_ordinal))
    });
    for (target, (index, _)) in indexes.iter_mut().zip(scored) {
        *target = index;
    }
    let left_leaf_count = leaf_count / 2;
    let right_leaf_count = leaf_count - left_leaf_count;
    let middle = indexes.len() * left_leaf_count / leaf_count;
    let (left, right) = indexes.split_at_mut(middle);
    split_planning_rows(rows, left, left_leaf_count, leaves);
    split_planning_rows(rows, right, right_leaf_count, leaves);
}

fn planning_page_bytes<'a>(rows: impl IntoIterator<Item = &'a V23PlanningRow>) -> Result<u64> {
    let rows = rows.into_iter().collect::<Vec<_>>();
    let code_width = rows.first().map_or(0, |row| row.code.len());
    if rows.is_empty() || code_width == 0 || rows.iter().any(|row| row.code.len() != code_width) {
        return Err(BorsukError::InvalidStorage(
            "V23 page rows have inconsistent code authority".to_string(),
        ));
    }
    let id_bytes = rows.iter().try_fold(0_u64, |total, row| {
        total.checked_add(row.canonical_record_id.len() as u64)
    });
    V23_PAGE_HEADER_BYTES
        .checked_add(4_u64.saturating_mul(rows.len() as u64 + 1))
        .and_then(|bytes| bytes.checked_add(id_bytes?))
        .and_then(|bytes| bytes.checked_add((rows.len() as u64).saturating_mul(code_width as u64)))
        .ok_or_else(|| BorsukError::InvalidStorage("V23 page bytes overflow".to_string()))
}

#[cfg(test)]
pub(crate) fn plan_v23_pages(
    rows: &[V23PlanningRow],
    primary_target_rows: u16,
    maximum_assignments_per_row: u8,
) -> Result<V23PagePlanningResult> {
    plan_v23_pages_for_metric(
        rows,
        primary_target_rows,
        maximum_assignments_per_row,
        &VectorMetric::SquaredEuclidean,
    )
}

fn plan_v23_pages_for_metric(
    rows: &[V23PlanningRow],
    primary_target_rows: u16,
    maximum_assignments_per_row: u8,
    metric: &VectorMetric,
) -> Result<V23PagePlanningResult> {
    let dimensions = rows.first().map_or(0, |row| row.geometry.len());
    let code_width = rows.first().map_or(0, |row| row.code.len());
    if rows.is_empty()
        || primary_target_rows == 0
        || !(1..=3).contains(&maximum_assignments_per_row)
        || dimensions == 0
        || code_width == 0
        || code_width > usize::from(u16::MAX)
        || !matches!(
            metric,
            VectorMetric::Euclidean | VectorMetric::SquaredEuclidean | VectorMetric::Cosine
        )
        || rows.iter().any(|row| {
            row.canonical_record_id.is_empty()
                || row.geometry.len() != dimensions
                || row.geometry.iter().any(|value| !value.is_finite())
                || row.code.len() != code_width
        })
        || rows
            .iter()
            .map(|row| row.source_ordinal)
            .collect::<BTreeSet<_>>()
            .len()
            != rows.len()
        || rows
            .iter()
            .map(|row| row.canonical_record_id.as_ref())
            .collect::<BTreeSet<_>>()
            .len()
            != rows.len()
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V23 page-planning authority is invalid".to_string(),
        ));
    }
    let mut by_cell = BTreeMap::<u32, Vec<usize>>::new();
    for (index, row) in rows.iter().enumerate() {
        by_cell.entry(row.primary_cell).or_default().push(index);
    }
    let mut page_rows = Vec::<(u32, Vec<usize>)>::new();
    for (primary_cell, mut indexes) in by_cell {
        indexes.sort_unstable_by(|left, right| {
            rows[*left]
                .canonical_record_id
                .cmp(&rows[*right].canonical_record_id)
                .then_with(|| rows[*left].source_ordinal.cmp(&rows[*right].source_ordinal))
        });
        let maximum_id_bytes = indexes
            .iter()
            .map(|index| rows[*index].canonical_record_id.len() as u64)
            .max()
            .expect("nonempty V23 parent cell");
        let row_bytes = 4_u64
            .checked_add(maximum_id_bytes)
            .and_then(|bytes| bytes.checked_add(code_width as u64))
            .ok_or_else(|| BorsukError::InvalidStorage("V23 row bytes overflow".to_string()))?;
        let maximum_rows_by_bytes = V23_PAGE_MAX_ENCODED_BYTES
            .checked_sub(V23_PAGE_HEADER_BYTES + 4)
            .map(|bytes| bytes / row_bytes)
            .unwrap_or(0);
        let effective_target =
            usize::from(primary_target_rows).min(usize::try_from(maximum_rows_by_bytes).map_err(
                |_| BorsukError::InvalidStorage("V23 page row capacity exceeds usize".to_string()),
            )?);
        if effective_target == 0 {
            return Err(BorsukError::InvalidStorage(
                "V23 primary row exceeds its encoded-byte cap".to_string(),
            ));
        }
        let leaf_count = indexes.len().div_ceil(effective_target);
        let mut leaves = Vec::with_capacity(leaf_count);
        split_planning_rows(rows, &mut indexes, leaf_count, &mut leaves);
        for leaf in leaves {
            if planning_page_bytes(leaf.iter().map(|index| &rows[*index]))?
                > V23_PAGE_MAX_ENCODED_BYTES
            {
                return Err(BorsukError::InvalidStorage(
                    "V23 primary page exceeds its encoded-byte cap".to_string(),
                ));
            }
            page_rows.push((primary_cell, leaf));
        }
    }
    let raw_centroids = page_rows
        .iter()
        .map(|(_, indexes)| {
            let mut centroid = vec![0.0_f64; dimensions];
            for index in indexes {
                for (sum, value) in centroid.iter_mut().zip(rows[*index].geometry.iter()) {
                    *sum += f64::from(*value);
                }
            }
            let denominator = indexes.len() as f64;
            centroid
                .into_iter()
                .map(|value| (value / denominator) as f32)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let page_catalog = std::sync::Arc::new(LogicalCellCatalog::from_centroids(
        23,
        dimensions,
        metric.clone(),
        raw_centroids,
    )?);
    let centroids = page_catalog.centroids.clone();
    let page_router = CatalogRouter::build(
        page_catalog,
        metric.clone(),
        CatalogRoutingStrategy::production(metric, page_rows.len()),
    )?;
    let mut primary_owner = vec![usize::MAX; rows.len()];
    for (page, (_, indexes)) in page_rows.iter().enumerate() {
        for index in indexes {
            primary_owner[*index] = page;
        }
    }
    let mut replicas = vec![Vec::<usize>::new(); page_rows.len()];
    let mut page_encoded_bytes = page_rows
        .iter()
        .map(|(_, indexes)| planning_page_bytes(indexes.iter().map(|index| &rows[*index])))
        .collect::<Result<Vec<_>>>()?;
    let mut maximum_secondary_pages_evaluated_per_row = 0_usize;
    let mut maximum_replica_candidates_retained = 0_usize;
    if maximum_assignments_per_row > 1 {
        let mut candidate_heaps = page_rows
            .iter()
            .map(|(_, primary_indexes)| BinaryHeap::with_capacity(primary_indexes.len()))
            .collect::<Vec<_>>();
        for (row_index, row) in rows.iter().enumerate() {
            let owner = primary_owner[row_index];
            let primary_distance =
                metric.centroid_geometry_distance_unchecked(&row.geometry, &centroids[owner])?;
            let mut secondary = page_router
                .nearest(&row.geometry, page_rows.len().min(17))?
                .into_iter()
                .filter_map(|page| {
                    let page = usize::try_from(page).ok()?;
                    (page != owner).then_some(page)
                })
                .map(|page| {
                    metric
                        .centroid_geometry_distance_unchecked(&row.geometry, &centroids[page])
                        .map(|distance| (distance / primary_distance.max(f32::MIN_POSITIVE), page))
                })
                .collect::<Result<Vec<_>>>()?;
            maximum_secondary_pages_evaluated_per_row =
                maximum_secondary_pages_evaluated_per_row.max(secondary.len());
            secondary.sort_by(|left, right| {
                left.0
                    .total_cmp(&right.0)
                    .then_with(|| left.1.cmp(&right.1))
            });
            for (ratio, page) in secondary
                .into_iter()
                .take(usize::from(maximum_assignments_per_row - 1))
            {
                let candidate = V23ReplicaCandidate {
                    ratio,
                    page,
                    source_ordinal: row.source_ordinal,
                    row_index,
                };
                let capacity = page_rows[page].1.len();
                let heap = &mut candidate_heaps[page];
                if heap.len() < capacity {
                    heap.push(candidate);
                } else if heap
                    .peek()
                    .is_some_and(|weakest_retained| candidate < *weakest_retained)
                {
                    heap.pop();
                    heap.push(candidate);
                }
            }
        }
        maximum_replica_candidates_retained = candidate_heaps.iter().map(BinaryHeap::len).sum();
        for (page, candidates) in candidate_heaps.into_iter().enumerate() {
            for candidate in candidates.into_sorted_vec() {
                let additional_bytes = 4_u64
                    .checked_add(rows[candidate.row_index].canonical_record_id.len() as u64)
                    .and_then(|bytes| bytes.checked_add(code_width as u64))
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage("V23 replica bytes overflow".to_string())
                    })?;
                if page_encoded_bytes[page]
                    .checked_add(additional_bytes)
                    .is_some_and(|bytes| bytes <= V23_PAGE_MAX_ENCODED_BYTES)
                {
                    replicas[page].push(candidate.row_index);
                    page_encoded_bytes[page] += additional_bytes;
                }
            }
        }
    }
    let pages = page_rows
        .into_iter()
        .enumerate()
        .map(|(page_ordinal, (primary_cell, primary_indexes))| {
            let encoded_bytes = page_encoded_bytes[page_ordinal];
            Ok(V23PagePlan {
                page_ordinal: u32::try_from(page_ordinal).map_err(|_| {
                    BorsukError::InvalidStorage("V23 page ordinal exceeds u32".to_string())
                })?,
                primary_cell,
                centroid: centroids[page_ordinal].clone().into_boxed_slice(),
                primary_source_ordinals: primary_indexes
                    .iter()
                    .map(|index| rows[*index].source_ordinal)
                    .collect(),
                replicated_source_ordinals: replicas[page_ordinal]
                    .iter()
                    .map(|index| rows[*index].source_ordinal)
                    .collect(),
                encoded_bytes,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(V23PagePlanningResult {
        pages,
        maximum_secondary_pages_evaluated_per_row,
        maximum_replica_candidates_retained,
    })
}

pub(crate) struct V23D2CorpusAuthority<'a> {
    pub(crate) d1_report: &'a V23D1Report,
    pub(crate) d1_key: V23D1ArmKey,
    pub(crate) scratch: &'a V22StageLSpill,
    pub(crate) query_ordinals: &'a [u64],
    pub(crate) queries: &'a [Vec<f32>],
    pub(crate) query_prefixes: &'a [V22StageLQueryPrefix],
    pub(crate) metric: VectorMetric,
    pub(crate) normalize: bool,
}

fn validate_v23_d2_query_prefixes(prefixes: &[V22StageLQueryPrefix]) -> Result<()> {
    if prefixes.len() != V23_DIAGNOSTIC_QUERIES
        || prefixes.iter().enumerate().any(|(index, prefix)| {
            prefix.query_index != index
                || prefix.rows.len() != V22_MAX_EXACT_PREFIX_ROWS
                || prefix
                    .rows
                    .iter()
                    .take(10)
                    .any(|row| row.canonical_record_id.is_empty())
                || prefix
                    .rows
                    .iter()
                    .take(10)
                    .map(|row| row.canonical_record_id.as_ref())
                    .collect::<BTreeSet<_>>()
                    .len()
                    != 10
        })
    {
        return Err(BorsukError::InvalidStorage(
            "V23 D2 exact-prefix authority differs".to_string(),
        ));
    }
    Ok(())
}

fn v23_d2_query_views(query: &[f32], normalize: bool) -> (&[f32], Vec<f32>) {
    let scoring_query = if normalize {
        crate::metric::unit_l2_normalized(query)
    } else {
        query.to_vec()
    };
    (query, scoring_query)
}

fn v23_d2_projected_memory(
    unique_rows: u64,
    page_count: usize,
    dimensions: usize,
    selector_coarse_cells: u32,
    selector_code_width: u16,
) -> Result<(u64, u64)> {
    if unique_rows == 0
        || page_count == 0
        || dimensions == 0
        || selector_coarse_cells == 0
        || !V23_SELECTOR_CODE_WIDTHS.contains(&selector_code_width)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 RAM projection authority is empty".to_string(),
        ));
    }
    let page_count = u64::try_from(page_count)
        .map_err(|_| BorsukError::InvalidStorage("V23 page count exceeds u64".to_string()))?;
    let dimensions = u64::try_from(dimensions)
        .map_err(|_| BorsukError::InvalidStorage("V23 dimensions exceed u64".to_string()))?;
    let projected_pages = page_count
        .checked_mul(V23_PROJECTED_ROWS)
        .ok_or_else(|| BorsukError::InvalidStorage("V23 projected pages overflow".to_string()))?
        .div_ceil(unique_rows)
        .max(page_count);
    let centroid_bytes = dimensions
        .checked_mul(4)
        .ok_or_else(|| BorsukError::InvalidStorage("V23 centroid bytes overflow".to_string()))?;
    let projected_root_bytes = projected_pages
        .checked_mul(V23_PROJECTED_ROOT_FIXED_BYTES_PER_PAGE)
        .and_then(|bytes| bytes.checked_add(V23_PROJECTED_ROOT_HEADER_BYTES))
        .ok_or_else(|| BorsukError::InvalidStorage("V23 projected root overflows".to_string()))?;
    let projected_selector_coarse_cells =
        u64::from(selector_coarse_cells).max(V23_PROJECTED_SELECTOR_COARSE_CELLS);
    let selector_centroid_bytes = projected_selector_coarse_cells
        .checked_mul(centroid_bytes)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 projected selector centroids overflow".to_string())
        })?;
    let selector_offset_bytes = projected_selector_coarse_cells
        .checked_add(1)
        .and_then(|cells| cells.checked_mul(4))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 projected selector offsets overflow".to_string())
        })?;
    let projected_row_bytes = V23_PROJECTED_ROWS
        .checked_mul(u64::from(selector_code_width).saturating_add(8))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 projected selector rows overflow".to_string())
        })?;
    let projected_selector_bytes = (V23_SELECTOR_HEADER_BYTES as u64)
        .checked_add(selector_centroid_bytes)
        .and_then(|bytes| bytes.checked_add(selector_offset_bytes))
        .and_then(|bytes| bytes.checked_add(projected_row_bytes))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 projected selector object overflows".to_string())
        })?;
    let projected_decoded_selector_bytes = selector_centroid_bytes
        .checked_add(selector_offset_bytes)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 decoded selector projection overflows".to_string())
        })?;
    let projected_ram_bytes = projected_root_bytes
        .checked_add(projected_selector_bytes)
        .and_then(|bytes| bytes.checked_add(projected_decoded_selector_bytes))
        .and_then(|bytes| bytes.checked_add(V23_PROJECTED_FIXED_RUNTIME_BYTES))
        .and_then(|bytes| bytes.checked_add(V23_WAVE_MAX_BYTES.saturating_mul(2)))
        .ok_or_else(|| BorsukError::InvalidStorage("V23 RAM projection overflows".to_string()))?;
    Ok((projected_root_bytes, projected_ram_bytes))
}

fn v23_d2_projected_build_memory(
    rows: u64,
    page_count: usize,
    encoded_page_bytes: u64,
    dimensions: usize,
    maximum_record_id_bytes: u16,
    code_width: u16,
    maximum_assignments_per_row: u8,
) -> Result<u64> {
    if rows == 0
        || page_count == 0
        || dimensions == 0
        || maximum_record_id_bytes == 0
        || code_width == 0
        || !(1..=3).contains(&maximum_assignments_per_row)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 builder projection authority is empty".to_string(),
        ));
    }
    let dimensions = u64::try_from(dimensions)
        .map_err(|_| BorsukError::InvalidStorage("V23 dimensions exceed u64".to_string()))?;
    let decoded_row_bytes = u64::try_from(std::mem::size_of::<V23PlanningRow>())
        .ok()
        .and_then(|bytes| bytes.checked_add(u64::from(maximum_record_id_bytes)))
        .and_then(|bytes| bytes.checked_add(dimensions.checked_mul(4)?))
        .and_then(|bytes| bytes.checked_add(u64::from(code_width)))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 decoded row projection overflows".to_string())
        })?;
    let candidate_bytes = u64::try_from(std::mem::size_of::<V23ReplicaCandidate>())
        .ok()
        .and_then(|bytes| {
            bytes.checked_mul(u64::from(
                maximum_assignments_per_row.saturating_sub(1).min(1),
            ))
        })
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 replica candidate projection overflows".to_string())
        })?;
    let index_bytes = u64::try_from(std::mem::size_of::<usize>())
        .ok()
        // Conservatively cover cell indexes, semantic-split leaves, the
        // primary-owner vector, replica vectors, and materialized ordinal
        // vectors that can overlap at the planner/materialization boundary.
        .and_then(|bytes| bytes.checked_mul(7))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 planner index projection overflows".to_string())
        })?;
    let decoded_and_planner = rows
        .checked_mul(
            decoded_row_bytes
                .checked_add(u64::from(V23_SELECTOR_CODE_WIDTHS[1]))
                .and_then(|bytes| bytes.checked_add(32))
                .and_then(|bytes| bytes.checked_add(candidate_bytes))
                .and_then(|bytes| bytes.checked_add(index_bytes))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("V23 builder row projection overflows".to_string())
                })?,
        )
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 builder projection overflows".to_string())
        })?;
    let page_count = u64::try_from(page_count)
        .map_err(|_| BorsukError::InvalidStorage("V23 page count exceeds u64".to_string()))?;
    let page_authority_bytes = page_count
        .checked_mul(
            dimensions
                .checked_mul(4)
                .and_then(|bytes| bytes.checked_add(V23_PROJECTED_ROUTER_BYTES_PER_PAGE))
                .and_then(|bytes| bytes.checked_add(512))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V23 builder page authority projection overflows".to_string(),
                    )
                })?,
        )
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 builder page projection overflows".to_string())
        })?;
    let lightweight_evidence_bytes = V23_D2_EVALUATED_ARMS
        .checked_mul(V23_DIAGNOSTIC_QUERIES as u64)
        .and_then(|samples| {
            samples.checked_mul(
                V23_D2_LIGHTWEIGHT_SAMPLE_SLACK_BYTES
                    .checked_add(u64::from(maximum_record_id_bytes).saturating_mul(20))?,
            )
        })
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 lightweight evidence projection overflows".to_string())
        })?;
    decoded_and_planner
        .checked_add(encoded_page_bytes)
        // One directory is under construction while at most three selected
        // directories survive the second pass.
        .and_then(|bytes| bytes.checked_add(page_authority_bytes.saturating_mul(4)))
        .and_then(|bytes| bytes.checked_add(lightweight_evidence_bytes))
        .and_then(|bytes| bytes.checked_add(V23_WAVE_MAX_BYTES.saturating_mul(2)))
        .ok_or_else(|| BorsukError::InvalidStorage("V23 builder projection overflows".to_string()))
}

fn v23_d2_arm_build_projection(arm: &V23D2Arm) -> Result<u64> {
    let dimensions = usize::try_from(arm.selector.dimensions)
        .ok()
        .filter(|dimensions| *dimensions > 0)
        .ok_or_else(|| BorsukError::InvalidStorage("V23 D2 dimensions are empty".to_string()))?;
    let encoded_page_bytes = arm.pages.iter().try_fold(0_u64, |total, page| {
        total.checked_add(page.encoded_bytes).ok_or_else(|| {
            BorsukError::InvalidStorage("V23 encoded page bytes overflow".to_string())
        })
    })?;
    v23_d2_projected_build_memory(
        arm.unique_rows,
        arm.pages.len(),
        encoded_page_bytes,
        dimensions,
        arm.maximum_record_id_bytes,
        arm.d1_key.code_width_bytes,
        arm.maximum_assignments_per_row,
    )
}

#[derive(Clone, Copy)]
struct V23PageArmConfig {
    primary_target_rows: u16,
    maximum_assignments_per_row: u8,
}

#[derive(Clone, Copy)]
struct V23D2ArmBuildContext<'a> {
    authority: &'a V23D2CorpusAuthority<'a>,
    quantizer: &'a GlobalScanQuantizer,
    selector: &'a V23ContentSelector,
    planning_rows: &'a [V23PlanningRow],
}

fn build_v23_d2_arms(
    context: V23D2ArmBuildContext<'_>,
    page_config: V23PageArmConfig,
    materialized_page_budgets: Option<&BTreeSet<u8>>,
    page_sink: Option<&mut V23PageSink<'_>>,
    selector_sink: Option<&mut V23SelectorSink<'_>>,
) -> Result<Vec<V23D2Arm>> {
    let V23D2ArmBuildContext {
        authority,
        quantizer,
        selector,
        planning_rows,
    } = context;
    let V23PageArmConfig {
        primary_target_rows,
        maximum_assignments_per_row,
    } = page_config;
    let planning = plan_v23_pages_for_metric(
        planning_rows,
        primary_target_rows,
        maximum_assignments_per_row,
        &authority.metric,
    )?;
    let mut page_assignments = vec![Vec::<u32>::new(); planning_rows.len()];
    let mut primary_pages = vec![None::<u32>; planning_rows.len()];
    for page in &planning.pages {
        for source_ordinal in &page.primary_source_ordinals {
            let row_index = usize::try_from(*source_ordinal).map_err(|_| {
                BorsukError::InvalidStorage("V23 page assignment exceeds usize".to_string())
            })?;
            let assignments = page_assignments.get_mut(row_index).ok_or_else(|| {
                BorsukError::InvalidStorage("V23 page assignment row is absent".to_string())
            })?;
            assignments.push(page.page_ordinal);
            let primary = primary_pages.get_mut(row_index).ok_or_else(|| {
                BorsukError::InvalidStorage("V23 primary page row is absent".to_string())
            })?;
            if primary.replace(page.page_ordinal).is_some() {
                return Err(BorsukError::InvalidStorage(
                    "V23 row has multiple primary pages".to_string(),
                ));
            }
        }
        for source_ordinal in &page.replicated_source_ordinals {
            let row_index = usize::try_from(*source_ordinal).map_err(|_| {
                BorsukError::InvalidStorage("V23 replica assignment exceeds usize".to_string())
            })?;
            page_assignments
                .get_mut(row_index)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("V23 replica assignment row is absent".to_string())
                })?
                .push(page.page_ordinal);
        }
    }
    if page_assignments.iter_mut().any(|assignments| {
        assignments.sort_unstable();
        assignments.is_empty()
            || assignments.len() > usize::from(maximum_assignments_per_row)
            || assignments.windows(2).any(|pair| pair[0] >= pair[1])
    }) {
        return Err(BorsukError::InvalidStorage(
            "V23 page assignment authority differs".to_string(),
        ));
    }
    let primary_pages = primary_pages
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| BorsukError::InvalidStorage("V23 primary page is absent".to_string()))?;
    let dimensions = authority.scratch.dimensions();
    let generation_checksum = *blake3::Hash::from_hex(&authority.d1_report.v20_root_checksum)
        .map_err(|_| {
            BorsukError::InvalidStorage("V23 source generation checksum differs".to_string())
        })?
        .as_bytes();
    let (selector_ref, selector_bytes, page_selector) = build_v23_packed_page_selector(
        selector,
        planning_rows,
        &planning,
        &page_assignments,
        &primary_pages,
        generation_checksum,
        &authority.metric,
    )?;
    if let Some(sink) = selector_sink {
        sink(&selector_ref, &selector_bytes)?;
    }
    let encoded_pages = planning
        .pages
        .iter()
        .map(|page| {
            let page_rows = |ordinals: &[u64]| -> Result<Vec<V23PageRow>> {
                let mut rows = ordinals
                    .iter()
                    .map(|ordinal| {
                        let row = usize::try_from(*ordinal)
                            .ok()
                            .and_then(|index| planning_rows.get(index))
                            .filter(|row| row.source_ordinal == *ordinal)
                            .ok_or_else(|| {
                                BorsukError::InvalidStorage(
                                    "V23 page row ordinal differs".to_string(),
                                )
                            })?;
                        Ok(V23PageRow {
                            canonical_record_id: row.canonical_record_id.clone(),
                            code: row.code.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                rows.sort_unstable_by(|left, right| {
                    left.canonical_record_id.cmp(&right.canonical_record_id)
                });
                Ok(rows)
            };
            let input = V23PageInput {
                generation_checksum,
                page_ordinal: page.page_ordinal,
                metric: authority.metric.clone(),
                dimensions: u32::try_from(dimensions).map_err(|_| {
                    BorsukError::InvalidStorage("V23 dimensions exceed u32".to_string())
                })?,
                family: authority.d1_key.family,
                code_width: authority.d1_key.code_width_bytes,
                primary_rows: page_rows(&page.primary_source_ordinals)?,
                replicated_rows: page_rows(&page.replicated_source_ordinals)?,
            };
            let bytes = encode_v23_page(&input)?;
            if bytes.len() as u64 != page.encoded_bytes {
                return Err(BorsukError::InvalidStorage(
                    "V23 planned and encoded page lengths differ".to_string(),
                ));
            }
            let checksum = blake3::hash(&bytes).to_hex().to_string();
            Ok((
                V23PageRef {
                    generation_checksum,
                    page_ordinal: page.page_ordinal,
                    metric: authority.metric.clone(),
                    dimensions: input.dimensions,
                    family: authority.d1_key.family,
                    code_width: authority.d1_key.code_width_bytes,
                    path: format!("pages/{checksum}"),
                    checksum,
                    encoded_bytes: bytes.len() as u64,
                    primary_rows: u32::try_from(page.primary_source_ordinals.len()).map_err(
                        |_| {
                            BorsukError::InvalidStorage(
                                "V23 primary page rows exceed u32".to_string(),
                            )
                        },
                    )?,
                    replicated_rows: u32::try_from(page.replicated_source_ordinals.len()).map_err(
                        |_| {
                            BorsukError::InvalidStorage(
                                "V23 replicated page rows exceed u32".to_string(),
                            )
                        },
                    )?,
                },
                bytes,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let (pages, page_bytes): (Vec<_>, Vec<_>) = encoded_pages.into_iter().unzip();
    if let Some(sink) = page_sink {
        stream_v23_materialized_pages(&pages, &page_bytes, sink)?;
    }
    let total_assignments = pages.iter().try_fold(0_u64, |total, page| {
        total
            .checked_add(u64::from(page.primary_rows) + u64::from(page.replicated_rows))
            .ok_or_else(|| BorsukError::InvalidStorage("V23 assignments overflow".to_string()))
    })?;
    let unique_rows = authority.scratch.total_rows();
    let storage_amplification_ppm = total_assignments.saturating_mul(1_000_000) / unique_rows;
    let (projected_root_bytes, projected_ram_bytes) = v23_d2_projected_memory(
        unique_rows,
        pages.len(),
        dimensions,
        selector_ref.coarse_cells,
        selector_ref.code_width,
    )?;
    let total_encoded_page_bytes = pages.iter().try_fold(0_u64, |total, page| {
        total.checked_add(page.encoded_bytes).ok_or_else(|| {
            BorsukError::InvalidStorage("V23 encoded page bytes overflow".to_string())
        })
    })?;
    let projected_build_bytes = v23_d2_projected_build_memory(
        unique_rows,
        pages.len(),
        total_encoded_page_bytes,
        dimensions,
        authority.d1_report.maximum_record_id_bytes,
        authority.d1_key.code_width_bytes,
        maximum_assignments_per_row,
    )?;

    let mut built_arms = Vec::with_capacity(1);
    for maximum_query_pages in [V23_WAVE_MAX_PAGES as u8] {
        let mut query_samples = Vec::with_capacity(authority.queries.len());
        for (query_index, query) in authority.queries.iter().enumerate() {
            let (selector_query, prepared_query) = v23_d2_query_views(query, authority.normalize);
            let truth_assignments = authority.query_prefixes[query_index].rows[..10]
                .iter()
                .map(|row| {
                    usize::try_from(row.record_id)
                        .ok()
                        .and_then(|index| page_assignments.get(index))
                        .cloned()
                        .ok_or_else(|| {
                            BorsukError::InvalidStorage(
                                "V23 ground-truth page assignment is absent".to_string(),
                            )
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            let oracle =
                best_v23_page_coverage(&truth_assignments, usize::from(maximum_query_pages))?;
            let started = Instant::now();
            let selection =
                page_selector.select(selector_query, usize::from(maximum_query_pages))?;
            let page_ordinals = selection.page_ordinals;
            let selector_candidate_rows = selection.candidate_rows;
            let selector_routed_cells = selection.routed_cells;
            let selector_ranked_rows = selection.ranked_rows;
            let mut candidate_by_id = BTreeMap::<Box<[u8]>, Box<[u8]>>::new();
            let mut candidate_rows = 0_u64;
            let mut encoded_bytes = 0_u64;
            for page_ordinal in &page_ordinals {
                let page_index = usize::try_from(*page_ordinal).map_err(|_| {
                    BorsukError::InvalidStorage("V23 selected page exceeds usize".to_string())
                })?;
                let page = pages.get(page_index).ok_or_else(|| {
                    BorsukError::InvalidStorage("V23 router selected an absent page".to_string())
                })?;
                candidate_rows = candidate_rows
                    .checked_add(u64::from(page.primary_rows) + u64::from(page.replicated_rows))
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage("V23 candidate rows overflow".to_string())
                    })?;
                encoded_bytes = encoded_bytes
                    .checked_add(page.encoded_bytes)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage("V23 selected bytes overflow".to_string())
                    })?;
                let decoded = decode_v23_page(
                    page_bytes.get(page_index).cloned().ok_or_else(|| {
                        BorsukError::InvalidStorage("V23 encoded page is absent".to_string())
                    })?,
                    page,
                )?;
                for row_index in 0..decoded.primary_rows() + decoded.replicated_rows() {
                    let id = decoded.record_id(row_index).ok_or_else(|| {
                        BorsukError::InvalidStorage("V23 decoded record ID is absent".to_string())
                    })?;
                    let code = decoded.code(row_index).ok_or_else(|| {
                        BorsukError::InvalidStorage("V23 decoded code is absent".to_string())
                    })?;
                    candidate_by_id
                        .entry(id.to_vec().into_boxed_slice())
                        .or_insert_with(|| code.to_vec().into_boxed_slice());
                }
            }
            let mut codes = Vec::with_capacity(
                candidate_by_id
                    .len()
                    .saturating_mul(usize::from(authority.d1_key.code_width_bytes)),
            );
            let mut ids = Vec::with_capacity(candidate_by_id.len());
            for (id, code) in candidate_by_id {
                codes.extend_from_slice(&code);
                ids.push(id);
            }
            let prepared = quantizer.prepare_contiguous_query(&prepared_query)?;
            let distances = quantizer.score_prepared_contiguous_codes(&prepared, &codes)?;
            let mut ranked = Vec::new();
            for (id, distance) in ids.iter().zip(distances) {
                observe_ranked(&mut ranked, distance, id)?;
            }
            let ranked = finish_d2_ranked(ranked)?;
            let cpu_ns = started.elapsed().as_nanos().max(1) as u64;
            let ground_truth_ids = authority.query_prefixes[query_index].rows[..10]
                .iter()
                .map(|row| row.canonical_record_id.to_vec())
                .collect::<Vec<_>>();
            let truth = ground_truth_ids.iter().collect::<BTreeSet<_>>();
            let physical_ids = ids.iter().map(|id| id.as_ref()).collect::<BTreeSet<_>>();
            let gt_page_hits = ground_truth_ids
                .iter()
                .filter(|id| physical_ids.contains(id.as_slice()))
                .count() as u8;
            let hits = ranked.ids.iter().filter(|id| truth.contains(id)).count() as u8;
            query_samples.push(V23D2QuerySample {
                query_index: query_index as u32,
                page_ordinals,
                oracle_page_ordinals: oracle.page_ordinals,
                ground_truth_page_assignments: truth_assignments,
                encoded_bytes,
                candidate_rows,
                selector_candidate_rows,
                selector_routed_cells,
                selector_ranked_rows,
                ground_truth_ids,
                ranked,
                gt_page_hits,
                oracle_gt_page_hits: u8::try_from(oracle.hits).map_err(|_| {
                    BorsukError::InvalidStorage("V23 oracle hits exceed u8".to_string())
                })?,
                hits,
                recall_ppm: u64::from(hits).saturating_mul(100_000),
                cpu_ns,
            });
        }
        let denominator = (query_samples.len() as u64).saturating_mul(10);
        let aggregate_recall_ppm = query_samples
            .iter()
            .map(|sample| u64::from(sample.hits))
            .sum::<u64>()
            .saturating_mul(1_000_000)
            / denominator;
        let minimum_query_recall_ppm = query_samples
            .iter()
            .map(|sample| sample.recall_ppm)
            .min()
            .unwrap_or(0);
        let oracle_hits = query_samples
            .iter()
            .map(|sample| u64::from(sample.oracle_gt_page_hits))
            .sum::<u64>();
        let coverage_oracle_recall_ppm = oracle_hits.saturating_mul(1_000_000) / denominator;
        let coverage_oracle_minimum_query_recall_ppm = query_samples
            .iter()
            .map(|sample| u64::from(sample.oracle_gt_page_hits).saturating_mul(100_000))
            .min()
            .unwrap_or(0);
        let selected_hits = query_samples
            .iter()
            .map(|sample| u64::from(sample.gt_page_hits))
            .sum::<u64>();
        let selector_regret_ppm = selected_hits.saturating_mul(1_000_000) / oracle_hits.max(1);
        let mut cpu = query_samples
            .iter()
            .map(|sample| sample.cpu_ns)
            .collect::<Vec<_>>();
        cpu.sort_unstable();
        let cpu_p99_ns = cpu[cpu.len() - 1];
        let passed = aggregate_recall_ppm >= 975_000
            && minimum_query_recall_ppm >= 800_000
            && coverage_oracle_recall_ppm >= 985_000
            && coverage_oracle_minimum_query_recall_ppm >= 900_000
            && selector_regret_ppm >= 995_000
            && storage_amplification_ppm <= 2_000_000
            && projected_ram_bytes <= V23_PROCESS_MAX_BYTES
            && cpu_p99_ns <= V23_D1_CPU_MAX_NS;
        if materialized_page_budgets.is_some_and(|budgets| !budgets.contains(&maximum_query_pages))
        {
            continue;
        }
        built_arms.push(V23D2Arm {
            d1_key: authority.d1_key,
            selector_key: selector.key,
            selector: selector_ref.clone(),
            selector_routing_cells: u16::try_from(
                V23_SELECTOR_ROUTING_CELLS.min(selector_ref.coarse_cells as usize),
            )
            .unwrap(),
            selector_ranked_row_cap: u32::try_from(V23_SELECTOR_RANKED_ROWS).unwrap(),
            primary_target_rows,
            maximum_assignments_per_row,
            maximum_query_pages,
            maximum_record_id_bytes: authority.d1_report.maximum_record_id_bytes,
            pages: materialized_page_budgets.map_or_else(Vec::new, |_| pages.clone()),
            unique_rows,
            total_assignments,
            storage_amplification_ppm,
            projected_root_bytes,
            projected_ram_bytes,
            projected_build_bytes: projected_build_bytes.max(1),
            query_samples,
            aggregate_recall_ppm,
            minimum_query_recall_ppm,
            coverage_oracle_recall_ppm,
            coverage_oracle_minimum_query_recall_ppm,
            selector_regret_ppm,
            cpu_p99_ns,
            passed,
        });
    }
    Ok(built_arms)
}

pub(crate) fn build_v23_d2_report(authority: V23D2CorpusAuthority<'_>) -> Result<V23D2Report> {
    build_v23_d2_report_inner(authority, None, None)
}

pub(crate) fn build_v23_d2_report_with_artifact_sinks(
    authority: V23D2CorpusAuthority<'_>,
    page_sink: &mut V23PageSink<'_>,
    selector_sink: &mut V23SelectorSink<'_>,
) -> Result<V23D2Report> {
    build_v23_d2_report_inner(authority, Some(page_sink), Some(selector_sink))
}

fn build_v23_d2_report_inner(
    authority: V23D2CorpusAuthority<'_>,
    mut page_sink: Option<&mut V23PageSink<'_>>,
    mut selector_sink: Option<&mut V23SelectorSink<'_>>,
) -> Result<V23D2Report> {
    validate_d1_report(authority.d1_report)?;
    validate_v23_d2_query_prefixes(authority.query_prefixes)?;
    let selected = authority
        .d1_report
        .arms
        .iter()
        .find(|arm| arm.key == authority.d1_key && arm.passed)
        .ok_or_else(|| {
            BorsukError::InvalidSearchOptions(
                "V23 D2 requires one passing D1 quantizer arm".to_string(),
            )
        })?;
    let replayed_ground_truth = authority
        .query_prefixes
        .iter()
        .map(|prefix| {
            prefix.rows[..10]
                .iter()
                .map(|row| row.canonical_record_id.to_vec())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    if authority.queries.len() != V23_DIAGNOSTIC_QUERIES
        || authority.query_prefixes.len() != authority.queries.len()
        || authority.scratch.total_rows() != authority.d1_report.rows
        || authority.scratch.dimensions() != authority.d1_report.dimensions as usize
    {
        return Err(BorsukError::InvalidStorage(
            "V23 D2 corpus authority differs from D1".to_string(),
        ));
    }
    validate_v23_d2_query_binding(
        authority.d1_report,
        selected,
        authority.query_ordinals,
        authority.queries,
        &replayed_ground_truth,
    )?;
    let dimensions = authority.scratch.dimensions();
    let element_type = authority.scratch.element_type();
    let mut planning_rows = Vec::with_capacity(
        usize::try_from(authority.scratch.total_rows()).map_err(|_| {
            BorsukError::InvalidStorage("V23 D2 row count exceeds usize".to_string())
        })?,
    );
    for (primary_cell, _) in authority.scratch.cell_rows() {
        for row in authority.scratch.read_cell(primary_cell)? {
            let geometry = row.geometry(dimensions, element_type, authority.normalize)?;
            planning_rows.push(V23PlanningRow {
                source_ordinal: row.source_ordinal,
                canonical_record_id: row.canonical_record_id,
                primary_cell,
                geometry,
                code: Box::new([]),
            });
        }
    }
    planning_rows.sort_unstable_by_key(|row| row.source_ordinal);
    if planning_rows
        .iter()
        .enumerate()
        .any(|(index, row)| row.source_ordinal != index as u64)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 D2 source ordinals are not contiguous".to_string(),
        ));
    }
    let maximum_record_id_bytes = planning_rows
        .iter()
        .map(|row| row.canonical_record_id.len())
        .max()
        .and_then(|bytes| u16::try_from(bytes).ok())
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 D2 record-ID width exceeds u16".to_string())
        })?;
    if maximum_record_id_bytes != authority.d1_report.maximum_record_id_bytes {
        return Err(BorsukError::InvalidStorage(
            "V23 D2 record-ID width differs from D1".to_string(),
        ));
    }
    let sample_rows = planning_rows.len().min(65_536);
    let sample_ordinals = (0..sample_rows)
        .map(|index| index.saturating_mul(planning_rows.len()) / sample_rows)
        .collect::<Vec<_>>();
    let mut ordinal_hasher = blake3::Hasher::new();
    for ordinal in &sample_ordinals {
        ordinal_hasher.update(&(*ordinal as u64).to_le_bytes());
    }
    if ordinal_hasher.finalize().to_hex().as_str() != authority.d1_report.sample_ordinals_checksum {
        return Err(BorsukError::InvalidStorage(
            "V23 D2 training sample differs from D1".to_string(),
        ));
    }
    let quantizer = restore_v23_diagnostic_quantizer(selected)?;
    for row in &mut planning_rows {
        row.code = quantizer.encode(&row.geometry)?.into_boxed_slice();
    }
    let selectors = V23_SELECTOR_CODE_WIDTHS
        .into_iter()
        .map(|code_width_bytes| {
            V23ContentSelector::build(
                authority.d1_report,
                &planning_rows,
                V23D1ArmKey {
                    family: V23QuantizerFamily::SrhtPq,
                    code_width_bytes,
                },
                &authority.metric,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let mut evaluated_arms = Vec::with_capacity(V23_D2_EVALUATED_ARMS as usize);
    for selector in &selectors {
        evaluated_arms.extend(build_v23_d2_arms(
            V23D2ArmBuildContext {
                authority: &authority,
                quantizer: &quantizer,
                selector,
                planning_rows: &planning_rows,
            },
            V23PageArmConfig {
                primary_target_rows: 384,
                maximum_assignments_per_row: 2,
            },
            None,
            None,
            None,
        )?);
    }
    let selected = evaluated_arms
        .into_iter()
        .filter(|arm| arm.maximum_query_pages as usize == V23_WAVE_MAX_PAGES)
        .collect::<Vec<_>>();
    if selected.len() != V23_SELECTOR_CODE_WIDTHS.len()
        || selected
            .iter()
            .map(|arm| arm.selector_key.code_width_bytes)
            .ne(V23_SELECTOR_CODE_WIDTHS)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 D2 registered arm matrix differs".to_string(),
        ));
    }
    let mut nondominated = Vec::with_capacity(selected.len());
    let mut emitted_page_paths = BTreeSet::new();
    let mut emitted_selector_paths = BTreeSet::new();
    for selected_arm in &selected {
        let selector = selectors
            .iter()
            .find(|selector| selector.key == selected_arm.selector_key)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V23 D2 selector rehydration is absent".to_string())
            })?;
        let build_context = V23D2ArmBuildContext {
            authority: &authority,
            quantizer: &quantizer,
            selector,
            planning_rows: &planning_rows,
        };
        let budgets = BTreeSet::from([selected_arm.maximum_query_pages]);
        let rehydrated = match (page_sink.as_deref_mut(), selector_sink.as_deref_mut()) {
            (Some(page_sink), Some(selector_sink)) => {
                let mut unique_sink = |page: &V23PageRef, bytes: &Bytes| {
                    if emitted_page_paths.insert(page.path.clone()) {
                        page_sink(page, bytes)
                    } else {
                        Ok(())
                    }
                };
                let mut unique_selector_sink = |selector: &V23SelectorRef, bytes: &Bytes| {
                    if emitted_selector_paths.insert(selector.path.clone()) {
                        selector_sink(selector, bytes)
                    } else {
                        Ok(())
                    }
                };
                build_v23_d2_arms(
                    build_context,
                    V23PageArmConfig {
                        primary_target_rows: selected_arm.primary_target_rows,
                        maximum_assignments_per_row: selected_arm.maximum_assignments_per_row,
                    },
                    Some(&budgets),
                    Some(&mut unique_sink),
                    Some(&mut unique_selector_sink),
                )
            }
            (None, None) => build_v23_d2_arms(
                build_context,
                V23PageArmConfig {
                    primary_target_rows: selected_arm.primary_target_rows,
                    maximum_assignments_per_row: selected_arm.maximum_assignments_per_row,
                },
                Some(&budgets),
                None,
                None,
            ),
            _ => Err(BorsukError::InvalidStorage(
                "V23 D2 artifact sinks differ".to_string(),
            )),
        }?;
        for materialized in rehydrated {
            let key = d2_arm_key(&materialized);
            if d2_arm_key(selected_arm) != key {
                return Err(BorsukError::InvalidStorage(
                    "V23 D2 selected-arm rehydration differs".to_string(),
                ));
            }
            let mut evaluated = selected_arm.clone();
            if evaluated.selector_key != materialized.selector_key {
                return Err(BorsukError::InvalidStorage(
                    "V23 D2 selected-arm selector differs".to_string(),
                ));
            }
            evaluated.pages = materialized.pages;
            evaluated.selector = materialized.selector;
            evaluated.projected_build_bytes = materialized.projected_build_bytes;
            nondominated.push(evaluated);
        }
    }
    nondominated.sort_unstable_by_key(|arm| arm.selector_key.code_width_bytes);
    if nondominated.len() != selected.len()
        || nondominated
            .iter()
            .map(d2_arm_key)
            .ne(selected.iter().map(d2_arm_key))
    {
        return Err(BorsukError::InvalidStorage(
            "V23 D2 selected-arm rehydration differs".to_string(),
        ));
    }
    let projected_build_peak_bytes = nondominated
        .iter()
        .map(|arm| arm.projected_build_bytes)
        .max()
        .ok_or_else(|| BorsukError::InvalidStorage("V23 D2 frontier is empty".to_string()))?;
    for arm in &mut nondominated {
        arm.projected_build_bytes = projected_build_peak_bytes;
    }
    let report = V23D2Report {
        schema: "borsuk-v23-d2-v9".to_string(),
        d1_report_checksum: v23_d1_report_checksum(authority.d1_report)?,
        query_ordinals: authority.query_ordinals.to_vec(),
        rows: authority.scratch.total_rows(),
        arms: nondominated,
    };
    validate_d2_report(&report)?;
    Ok(report)
}

pub(crate) fn validate_d1_report(report: &V23D1Report) -> Result<()> {
    if report.schema != "borsuk-v23-d1-v5"
        || !valid_checksum(&report.v20_root_checksum)
        || !valid_checksum(&report.v20_codebook_checksum)
        || !valid_checksum(&report.sample_ordinals_checksum)
        || !valid_checksum(&report.query_vectors_checksum)
        || report.query_ordinals.len() != V23_DIAGNOSTIC_QUERIES
        || report
            .query_ordinals
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || report.rows == 0
        || report.dimensions == 0
        || report.routing_cell_count == 0
        || report.maximum_record_id_bytes == 0
        || report.arms.is_empty()
        || report
            .arms
            .windows(2)
            .any(|pair| pair[0].key >= pair[1].key)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 D1 report authority differs".to_string(),
        ));
    }
    for arm in &report.arms {
        let quantizer = restore_v23_diagnostic_quantizer(arm)?;
        let expected_page_projection =
            v23_d1_projected_page_bytes(arm.key.code_width_bytes, report.maximum_record_id_bytes);
        let expected_wave_projection =
            expected_page_projection.saturating_mul(V23_WAVE_MAX_PAGES as u64);
        let expected_wave_rows =
            v23_d1_projected_page_rows(arm.key.code_width_bytes, report.maximum_record_id_bytes)
                .saturating_mul(V23_WAVE_MAX_PAGES as u64);
        if !valid_diagnostic_code_width(arm.key)
            || !valid_checksum(&arm.quantizer_checksum)
            || quantizer.dimensions() != report.dimensions as usize
            || arm.query_samples.len() != V23_DIAGNOSTIC_QUERIES
            || arm.wave_projected_bytes != expected_wave_projection
        {
            return Err(BorsukError::InvalidStorage(
                "V23 D1 arm authority differs".to_string(),
            ));
        }
        let mut oracle_hits = 0_u64;
        let mut routed_hits = 0_u64;
        let mut cpu = Vec::with_capacity(V23_DIAGNOSTIC_QUERIES);
        let mut scalar_simd_ids_equal = true;
        let mut scalar_simd_rank_equivalent = true;
        let mut scalar_simd_max_distance_delta_ppm = 0_u64;
        for (expected_index, sample) in arm.query_samples.iter().enumerate() {
            let truth = sample.ground_truth_ids.iter().collect::<BTreeSet<_>>();
            validate_ranked_result(&sample.oracle)?;
            validate_ranked_result(&sample.scalar_oracle)?;
            validate_ranked_result(&sample.routed)?;
            scalar_simd_ids_equal &= sample.oracle.ids == sample.scalar_oracle.ids;
            scalar_simd_rank_equivalent &=
                v23_rankings_equivalent_within_tolerance(&sample.oracle, &sample.scalar_oracle);
            for (simd, scalar) in sample
                .oracle
                .distances
                .iter()
                .zip(&sample.scalar_oracle.distances)
            {
                let normalized =
                    f64::from((simd - scalar).abs()) / f64::from(scalar.abs().max(1.0));
                scalar_simd_max_distance_delta_ppm = scalar_simd_max_distance_delta_ppm
                    .max((normalized * 1_000_000.0).ceil().min(u64::MAX as f64) as u64);
            }
            let expected_oracle_hits = sample
                .oracle
                .ids
                .iter()
                .filter(|id| truth.contains(id))
                .count();
            let expected_routed_hits = sample
                .routed
                .ids
                .iter()
                .filter(|id| truth.contains(id))
                .count();
            if usize::try_from(sample.query_index).ok() != Some(expected_index)
                || truth.len() != 10
                || sample.ground_truth_ids.iter().any(Vec::is_empty)
                || sample.oracle_candidate_rows != 2_048
                || sample.routed_candidate_rows == 0
                || sample.routed_candidate_rows > report.rows
                || sample.wave_candidate_rows != expected_wave_rows
                || sample.cpu_ns == 0
                || usize::from(sample.oracle_hits) != expected_oracle_hits
                || usize::from(sample.routed_hits) != expected_routed_hits
            {
                return Err(BorsukError::InvalidStorage(
                    "V23 D1 query authority differs".to_string(),
                ));
            }
            oracle_hits = oracle_hits.saturating_add(u64::from(sample.oracle_hits));
            routed_hits = routed_hits.saturating_add(u64::from(sample.routed_hits));
            cpu.push(sample.cpu_ns);
        }
        cpu.sort_unstable();
        let denominator = (V23_DIAGNOSTIC_QUERIES as u64).saturating_mul(10);
        let expected_oracle_recall = oracle_hits.saturating_mul(1_000_000) / denominator;
        let expected_routed_recall = routed_hits.saturating_mul(1_000_000) / denominator;
        let expected_cpu_p99 = cpu[V23_DIAGNOSTIC_QUERIES - 1];
        let expected_passed = expected_oracle_recall >= 990_000
            && expected_routed_recall >= 975_000
            && scalar_simd_rank_equivalent
            && scalar_simd_max_distance_delta_ppm <= V23_SCALAR_SIMD_MAX_DISTANCE_DELTA_PPM
            && expected_cpu_p99 <= V23_D1_CPU_MAX_NS
            && expected_wave_rows >= V23_D1_PROJECTED_PAGE_ROWS
            && expected_page_projection <= V23_PAGE_MAX_ENCODED_BYTES
            && arm.wave_projected_bytes <= V23_WAVE_MAX_BYTES;
        if arm.oracle_recall_ppm != expected_oracle_recall
            || arm.routed_recall_ppm != expected_routed_recall
            || arm.scalar_simd_ids_equal != scalar_simd_ids_equal
            || arm.scalar_simd_max_distance_delta_ppm != scalar_simd_max_distance_delta_ppm
            || arm.cpu_p99_ns != expected_cpu_p99
            || arm.passed != expected_passed
        {
            return Err(BorsukError::InvalidStorage(
                "V23 D1 derived authority differs".to_string(),
            ));
        }
    }
    Ok(())
}

fn d2_arm_key(arm: &V23D2Arm) -> (V23D1ArmKey, V23D1ArmKey, u16, u8, u8) {
    (
        arm.d1_key,
        arm.selector_key,
        arm.primary_target_rows,
        arm.maximum_assignments_per_row,
        arm.maximum_query_pages,
    )
}

pub(crate) fn validate_d2_report(report: &V23D2Report) -> Result<()> {
    if report.schema != "borsuk-v23-d2-v9"
        || !valid_checksum(&report.d1_report_checksum)
        || report.query_ordinals.len() != V23_DIAGNOSTIC_QUERIES
        || report
            .query_ordinals
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || report.rows == 0
        || report.arms.len() != V23_SELECTOR_CODE_WIDTHS.len()
        || report
            .arms
            .iter()
            .map(|arm| arm.selector_key.code_width_bytes)
            .ne(V23_SELECTOR_CODE_WIDTHS)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 D2 report authority differs".to_string(),
        ));
    }
    let expected_projected_build_peak = report
        .arms
        .iter()
        .map(v23_d2_arm_build_projection)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .max()
        .ok_or_else(|| BorsukError::InvalidStorage("V23 D2 frontier is empty".to_string()))?;
    for arm in &report.arms {
        if !valid_diagnostic_code_width(arm.d1_key)
            || arm.selector_key.family != V23QuantizerFamily::SrhtPq
            || !V23_SELECTOR_CODE_WIDTHS.contains(&arm.selector_key.code_width_bytes)
            || arm.selector.code_width != arm.selector_key.code_width_bytes
            || usize::from(arm.selector_routing_cells)
                != V23_SELECTOR_ROUTING_CELLS.min(arm.selector.coarse_cells as usize)
            || usize::try_from(arm.selector_ranked_row_cap).ok() != Some(V23_SELECTOR_RANKED_ROWS)
            || arm.primary_target_rows != 384
            || arm.maximum_assignments_per_row != 2
            || arm.maximum_query_pages as usize != V23_WAVE_MAX_PAGES
            || arm.maximum_record_id_bytes == 0
            || arm.pages.is_empty()
            || arm.unique_rows != report.rows
            || arm.projected_root_bytes == 0
            || arm.projected_build_bytes == 0
            || arm.query_samples.len() != V23_DIAGNOSTIC_QUERIES
        {
            return Err(BorsukError::InvalidStorage(
                "V23 D2 arm authority differs".to_string(),
            ));
        }
        let mut primary_rows = 0_u64;
        let mut assignments = 0_u64;
        let dimensions = usize::try_from(arm.selector.dimensions).map_err(|_| {
            BorsukError::InvalidStorage("V23 D2 dimensions exceed usize".to_string())
        })?;
        let generation_checksum = arm.pages[0].generation_checksum;
        let metric = &arm.pages[0].metric;
        let selector_centroid_bytes = u64::from(arm.selector.coarse_cells)
            .checked_mul(arm.selector.dimensions.into())
            .and_then(|values| values.checked_mul(4));
        let selector_offset_bytes = u64::from(arm.selector.coarse_cells)
            .checked_add(1)
            .and_then(|values| values.checked_mul(4));
        let expected_selector_bytes = selector_centroid_bytes
            .and_then(|bytes| bytes.checked_add(selector_offset_bytes?))
            .and_then(|bytes| {
                bytes.checked_add(
                    arm.selector
                        .row_count
                        .checked_mul(u64::from(arm.selector.code_width).checked_add(8)?)?,
                )
            })
            .and_then(|bytes| bytes.checked_add(V23_SELECTOR_HEADER_BYTES as u64));
        if arm.selector.generation_checksum != generation_checksum
            || &arm.selector.metric != metric
            || dimensions == 0
            || usize::try_from(arm.selector.coarse_cells).map_or(true, |cells| {
                cells < usize::from(arm.selector_routing_cells)
            })
            || usize::try_from(arm.selector.page_count).ok() != Some(arm.pages.len())
            || arm.selector.maximum_assignments_per_row != V23_SELECTOR_MAXIMUM_ASSIGNMENTS_PER_ROW
            || !V23_SELECTOR_CODE_WIDTHS.contains(&arm.selector.code_width)
            || arm.selector.row_count != arm.unique_rows
            || !valid_checksum(&arm.selector.checksum)
            || arm.selector.path != format!("selectors/{}", arm.selector.checksum)
            || expected_selector_bytes != Some(arm.selector.encoded_bytes)
        {
            return Err(BorsukError::InvalidStorage(
                "V23 D2 selector authority differs".to_string(),
            ));
        }
        for (page_index, page) in arm.pages.iter().enumerate() {
            let expected_path = format!("pages/{}", page.checksum);
            if usize::try_from(page.page_ordinal).ok() != Some(page_index)
                || page.generation_checksum == [0; 32]
                || page.generation_checksum != generation_checksum
                || &page.metric != metric
                || !matches!(
                    &page.metric,
                    VectorMetric::Euclidean | VectorMetric::SquaredEuclidean | VectorMetric::Cosine
                )
                || usize::try_from(page.dimensions).ok() != Some(dimensions)
                || page.family != arm.d1_key.family
                || page.code_width != arm.d1_key.code_width_bytes
                || !valid_page_code_width(page.family, page.code_width, page.dimensions)
                || !valid_checksum(&page.checksum)
                || page.path != expected_path
                || page.encoded_bytes == 0
                || page.encoded_bytes > V23_PAGE_MAX_ENCODED_BYTES
                || page.primary_rows == 0
            {
                return Err(BorsukError::InvalidStorage(
                    "V23 D2 page authority differs".to_string(),
                ));
            }
            primary_rows = primary_rows.saturating_add(u64::from(page.primary_rows));
            assignments = assignments
                .saturating_add(u64::from(page.primary_rows))
                .saturating_add(u64::from(page.replicated_rows));
        }
        let expected_amplification = assignments.saturating_mul(1_000_000) / arm.unique_rows;
        let (expected_projected_root_bytes, expected_projected_ram_bytes) =
            v23_d2_projected_memory(
                arm.unique_rows,
                arm.pages.len(),
                dimensions,
                arm.selector.coarse_cells,
                arm.selector.code_width,
            )?;
        if primary_rows != arm.unique_rows
            || assignments != arm.total_assignments
            || expected_amplification != arm.storage_amplification_ppm
            || arm.projected_root_bytes != expected_projected_root_bytes
            || arm.projected_ram_bytes != expected_projected_ram_bytes
            || arm.projected_build_bytes != expected_projected_build_peak
        {
            return Err(BorsukError::InvalidStorage(
                "V23 D2 assignment authority differs".to_string(),
            ));
        }

        let mut total_hits = 0_u64;
        let mut total_page_hits = 0_u64;
        let mut total_oracle_hits = 0_u64;
        let mut minimum_recall = 1_000_000_u64;
        let mut minimum_oracle_recall = 1_000_000_u64;
        let mut cpu = Vec::with_capacity(V23_DIAGNOSTIC_QUERIES);
        for (expected_index, sample) in arm.query_samples.iter().enumerate() {
            validate_d2_ranked_result(&sample.ranked)?;
            let truth = sample.ground_truth_ids.iter().collect::<BTreeSet<_>>();
            let expected_hits = sample
                .ranked
                .ids
                .iter()
                .filter(|id| truth.contains(id))
                .count();
            let page_refs = sample
                .page_ordinals
                .iter()
                .map(|ordinal| {
                    usize::try_from(*ordinal)
                        .ok()
                        .and_then(|index| arm.pages.get(index))
                })
                .collect::<Option<Vec<_>>>();
            let expected_bytes = page_refs.as_ref().and_then(|pages| {
                pages
                    .iter()
                    .try_fold(0_u64, |sum, page| sum.checked_add(page.encoded_bytes))
            });
            let expected_rows = page_refs.as_ref().and_then(|pages| {
                pages.iter().try_fold(0_u64, |sum, page| {
                    sum.checked_add(u64::from(page.primary_rows) + u64::from(page.replicated_rows))
                })
            });
            let expected_recall = (expected_hits as u64).saturating_mul(100_000);
            let recomputed_oracle = best_v23_page_coverage(
                &sample.ground_truth_page_assignments,
                usize::from(arm.maximum_query_pages),
            )?;
            let recomputed_page_hits = sample
                .ground_truth_page_assignments
                .iter()
                .filter(|assignments| {
                    assignments
                        .iter()
                        .any(|page| sample.page_ordinals.binary_search(page).is_ok())
                })
                .count();
            if usize::try_from(sample.query_index).ok() != Some(expected_index)
                || sample.page_ordinals.is_empty()
                || sample.page_ordinals.len() > V23_WAVE_MAX_PAGES
                || sample.page_ordinals.len() > usize::from(arm.maximum_query_pages)
                || sample
                    .page_ordinals
                    .windows(2)
                    .any(|pair| pair[0] >= pair[1])
                || sample.oracle_page_ordinals.is_empty()
                || sample.oracle_page_ordinals.len() > usize::from(arm.maximum_query_pages)
                || sample
                    .oracle_page_ordinals
                    .windows(2)
                    .any(|pair| pair[0] >= pair[1])
                || sample
                    .oracle_page_ordinals
                    .iter()
                    .any(|page| usize::try_from(*page).map_or(true, |page| page >= arm.pages.len()))
                || sample.ground_truth_page_assignments.len() != 10
                || recomputed_oracle.page_ordinals != sample.oracle_page_ordinals
                || recomputed_oracle.hits != usize::from(sample.oracle_gt_page_hits)
                || recomputed_page_hits != usize::from(sample.gt_page_hits)
                || expected_bytes != Some(sample.encoded_bytes)
                || sample.encoded_bytes > V23_WAVE_MAX_BYTES
                || expected_rows != Some(sample.candidate_rows)
                || sample.selector_candidate_rows == 0
                || sample.selector_routed_cells != arm.selector_routing_cells
                || u64::from(sample.selector_ranked_rows)
                    != sample
                        .selector_candidate_rows
                        .min(V23_SELECTOR_RANKED_ROWS as u64)
                || truth.len() != 10
                || sample.ground_truth_ids.iter().any(Vec::is_empty)
                || sample.gt_page_hits > 10
                || sample.oracle_gt_page_hits > 10
                || sample.oracle_gt_page_hits < sample.gt_page_hits
                || sample.gt_page_hits < sample.hits
                || usize::from(sample.hits) != expected_hits
                || sample.recall_ppm != expected_recall
                || sample.cpu_ns == 0
            {
                return Err(BorsukError::InvalidStorage(
                    "V23 D2 query authority differs".to_string(),
                ));
            }
            total_hits = total_hits.saturating_add(u64::from(sample.hits));
            total_page_hits = total_page_hits.saturating_add(u64::from(sample.gt_page_hits));
            total_oracle_hits =
                total_oracle_hits.saturating_add(u64::from(sample.oracle_gt_page_hits));
            minimum_recall = minimum_recall.min(sample.recall_ppm);
            minimum_oracle_recall = minimum_oracle_recall
                .min(u64::from(sample.oracle_gt_page_hits).saturating_mul(100_000));
            cpu.push(sample.cpu_ns);
        }
        cpu.sort_unstable();
        let expected_aggregate = total_hits.saturating_mul(1_000_000)
            / ((V23_DIAGNOSTIC_QUERIES as u64).saturating_mul(10));
        let expected_cpu_p99 = cpu[V23_DIAGNOSTIC_QUERIES - 1];
        let expected_coverage_oracle = total_oracle_hits.saturating_mul(1_000_000)
            / ((V23_DIAGNOSTIC_QUERIES as u64).saturating_mul(10));
        let expected_selector_regret =
            total_page_hits.saturating_mul(1_000_000) / total_oracle_hits.max(1);
        let expected_maximum_pages = usize::from(arm.maximum_query_pages).min(arm.pages.len());
        let expected_passed = expected_aggregate >= 975_000
            && minimum_recall >= 800_000
            && expected_coverage_oracle >= 985_000
            && minimum_oracle_recall >= 900_000
            && expected_selector_regret >= 995_000
            && arm.storage_amplification_ppm <= 2_000_000
            && arm.projected_ram_bytes <= V23_PROCESS_MAX_BYTES
            && expected_cpu_p99 <= V23_D1_CPU_MAX_NS;
        if arm.aggregate_recall_ppm != expected_aggregate
            || arm.minimum_query_recall_ppm != minimum_recall
            || arm.coverage_oracle_recall_ppm != expected_coverage_oracle
            || arm.coverage_oracle_minimum_query_recall_ppm != minimum_oracle_recall
            || arm.selector_regret_ppm != expected_selector_regret
            || arm.cpu_p99_ns != expected_cpu_p99
            || arm
                .query_samples
                .iter()
                .map(|sample| sample.page_ordinals.len())
                .max()
                != Some(expected_maximum_pages)
            || arm.passed != expected_passed
        {
            return Err(BorsukError::InvalidStorage(
                "V23 D2 derived authority differs".to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        fs,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
    use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
    use sha2::{Digest, Sha256};

    use super::{
        V23_DIAGNOSTIC_QUERIES, V23_PAGE_HEADER_BYTES, V23_SELECTOR_CODE_WIDTHS,
        V23_SELECTOR_HEADER_BYTES, V23_SELECTOR_RANKED_ROWS, V23_SELECTOR_ROUTING_CELLS,
        V23_WAVE_MAX_BYTES, V23_WAVE_MAX_PAGES, V23D1Arm, V23D1ArmKey, V23D1QuerySample,
        V23D1Report, V23D2Arm, V23D2QuerySample, V23D2Report, V23D3Executor,
        V23GlobalAdcArtifactRequest, V23GlobalAdcArtifactResult, V23GlobalAdcAuthority,
        V23GlobalAdcCausalClass, V23GlobalAdcEvidenceIdentity, V23GlobalAdcLocalArtifactPaths,
        V23GlobalAdcObjectIdentity, V23GlobalAdcRequest, V23PageInput, V23PageRef, V23PageRow,
        V23PageSelector, V23PlanningRow, V23QuantizerFamily, V23RankedResult, V23ReplicaCandidate,
        V23SelectorInput, V23SelectorRef, V23SelectorRow, V23WaveSample, best_v23_page_coverage,
        canonical_v23_global_adc_artifact_result_bytes, canonical_v23_global_adc_result_bytes,
        classify_v23_global_adc, decode_v23_page, decode_v23_selector, diagnose_v23_global_adc,
        encode_v23_page, encode_v23_selector, fit_v23_diagnostic_quantizer,
        load_v23_global_adc_local_artifacts, plan_v23_pages, plan_v23_pages_for_metric,
        read_v23_u16, restore_v23_diagnostic_quantizer, stream_v23_materialized_pages,
        v23_d1_arm_keys, v23_d1_bounded_wave_codes, v23_d1_projected_page_bytes,
        v23_d1_projected_page_rows, v23_d1_report_checksum, v23_d2_projected_build_memory,
        v23_d2_projected_memory, v23_d2_query_views, validate_d1_report, validate_d2_report,
        validate_v23_d2_query_binding, validate_v23_d2_query_prefixes,
        validate_v23_d3_request_capacity, validate_v23_global_adc_artifact_request,
        validate_wave_sample,
    };
    use crate::metric::VectorMetric;
    use crate::v22_feasibility::V22StageLQueryPrefix;

    struct GlobalAdcFixture {
        d1_selector_arm: V23D1Arm,
        selector_ref: V23SelectorRef,
        selector_bytes: Bytes,
        pages: Vec<V23PageRef>,
        queries: Vec<Vec<f32>>,
        ground_truth_page_assignments: Vec<Vec<Vec<u32>>>,
    }

    impl GlobalAdcFixture {
        fn request(&self) -> V23GlobalAdcRequest<'_> {
            V23GlobalAdcRequest {
                authority: V23GlobalAdcAuthority {
                    d1_selector_arm: &self.d1_selector_arm,
                    d2_selector: &self.selector_ref,
                    pages: &self.pages,
                    selector_bytes: self.selector_bytes.clone(),
                },
                queries: &self.queries,
                ground_truth_page_assignments: &self.ground_truth_page_assignments,
            }
        }
    }

    fn global_adc_fixture() -> GlobalAdcFixture {
        let dimensions = 96;
        let sample = (0..256)
            .map(|row| {
                (0..dimensions)
                    .map(|dimension| ((row * 17 + dimension * 13) % 251) as f32 / 251.0)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let quantizer =
            fit_v23_diagnostic_quantizer(V23QuantizerFamily::SrhtPq, 12, dimensions, &sample)
                .unwrap();
        let quantizer_state = serde_json::to_value(quantizer.state()).unwrap();
        let quantizer_checksum = blake3::hash(
            &serde_json::to_vec(&super::v23_canonical_json_value(quantizer_state.clone())).unwrap(),
        )
        .to_hex()
        .to_string();
        let d1_selector_arm = V23D1Arm {
            key: V23D1ArmKey {
                family: V23QuantizerFamily::SrhtPq,
                code_width_bytes: 12,
            },
            quantizer_checksum,
            quantizer_state,
            query_samples: Vec::new(),
            oracle_recall_ppm: 0,
            routed_recall_ppm: 0,
            scalar_simd_ids_equal: false,
            scalar_simd_max_distance_delta_ppm: 0,
            cpu_p99_ns: 0,
            wave_projected_bytes: 0,
            passed: false,
        };
        let generation_checksum = [9; 32];
        let pages = (0_u32..16)
            .map(|page_ordinal| {
                let checksum = format!("{:064x}", page_ordinal + 1);
                V23PageRef {
                    generation_checksum,
                    page_ordinal,
                    metric: VectorMetric::SquaredEuclidean,
                    dimensions: dimensions as u32,
                    family: V23QuantizerFamily::F16Flat,
                    code_width: (dimensions * 2) as u16,
                    path: format!("pages/{checksum}"),
                    checksum,
                    encoded_bytes: 100_000 + u64::from(page_ordinal),
                    primary_rows: 2,
                    replicated_rows: 2,
                }
            })
            .collect::<Vec<_>>();
        let rows = (0_u64..32)
            .map(|row| {
                let cell = if row == 31 { 4_095 } else { row as u32 };
                let primary = (row % 16) as u32;
                V23SelectorRow::new(
                    cell,
                    primary,
                    Some(primary ^ 1),
                    row,
                    &quantizer.encode(&sample[row as usize]).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let selector_input = V23SelectorInput {
            generation_checksum,
            metric: VectorMetric::SquaredEuclidean,
            dimensions: dimensions as u32,
            page_count: pages.len() as u32,
            code_width: 12,
            maximum_assignments_per_row: 2,
            coarse_centroids: vec![vec![0.0; dimensions]; 4_096],
            rows,
        };
        let selector_bytes = encode_v23_selector(&selector_input).unwrap();
        let selector_checksum = blake3::hash(&selector_bytes).to_hex().to_string();
        let selector_ref = V23SelectorRef {
            generation_checksum,
            metric: VectorMetric::SquaredEuclidean,
            dimensions: dimensions as u32,
            coarse_cells: 4_096,
            page_count: pages.len() as u32,
            maximum_assignments_per_row: 2,
            code_width: 12,
            row_count: 32,
            path: format!("selectors/{selector_checksum}"),
            checksum: selector_checksum,
            encoded_bytes: selector_bytes.len() as u64,
        };
        let queries = vec![sample[17].clone(); V23_DIAGNOSTIC_QUERIES];
        let ground_truth_page_assignments = (0..V23_DIAGNOSTIC_QUERIES)
            .map(|_| (0_u32..10).map(|rank| vec![rank % 16]).collect())
            .collect();
        GlobalAdcFixture {
            d1_selector_arm,
            selector_ref,
            selector_bytes,
            pages,
            queries,
            ground_truth_page_assignments,
        }
    }

    #[test]
    fn v23_global_adc_scans_all_cells_and_authenticates_width_12_inputs() {
        let fixture = global_adc_fixture();
        let result = diagnose_v23_global_adc(fixture.request()).unwrap();
        assert_eq!(result.selector_cells_scanned, 4_096);
        assert_eq!(result.selector_rows_scanned, 32);
        assert_eq!(result.selector_code_width, 12);
        assert_eq!(result.page_body_reads, 0);

        let mut checksum_drift = global_adc_fixture();
        checksum_drift.selector_bytes = {
            let mut bytes = checksum_drift.selector_bytes.to_vec();
            bytes[95] ^= 1;
            Bytes::from(bytes)
        };
        assert!(diagnose_v23_global_adc(checksum_drift.request()).is_err());

        let mut header_drift = global_adc_fixture();
        let mut bytes = header_drift.selector_bytes.to_vec();
        bytes[4] = 4;
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        header_drift.selector_ref.checksum = checksum.clone();
        header_drift.selector_ref.path = format!("selectors/{checksum}");
        header_drift.selector_bytes = Bytes::from(bytes);
        assert!(diagnose_v23_global_adc(header_drift.request()).is_err());

        let mut length_drift = global_adc_fixture();
        length_drift.selector_ref.encoded_bytes += 1;
        assert!(diagnose_v23_global_adc(length_drift.request()).is_err());

        let mut state_drift = global_adc_fixture();
        state_drift.d1_selector_arm.quantizer_checksum = "0".repeat(64);
        assert!(diagnose_v23_global_adc(state_drift.request()).is_err());

        let mut page_ref_drift = global_adc_fixture();
        page_ref_drift.pages.pop();
        assert!(diagnose_v23_global_adc(page_ref_drift.request()).is_err());
    }

    #[test]
    fn v23_global_adc_reducers_are_finite_deterministic_and_cap_eight_pages() {
        let fixture = global_adc_fixture();
        let first = diagnose_v23_global_adc(fixture.request()).unwrap();
        let second = diagnose_v23_global_adc(fixture.request()).unwrap();
        assert_eq!(first, second);
        for reducer in [&first.faithful, &first.per_page_min] {
            assert_eq!(reducer.query_samples.len(), V23_DIAGNOSTIC_QUERIES);
            for sample in &reducer.query_samples {
                assert_eq!(sample.page_ordinals.len(), 8);
                assert!(
                    sample
                        .page_ordinals
                        .windows(2)
                        .all(|pair| pair[0] < pair[1])
                );
                assert!(sample.minimum_distance.is_finite());
            }
        }

        let mut non_finite = global_adc_fixture();
        non_finite.queries[0][0] = f32::NAN;
        assert!(diagnose_v23_global_adc(non_finite.request()).is_err());
    }

    #[test]
    fn v23_global_adc_classifies_all_reducer_and_router_causal_states() {
        assert_eq!(
            classify_v23_global_adc(false, false).unwrap(),
            V23GlobalAdcCausalClass::TestedReducers
        );
        assert_eq!(
            classify_v23_global_adc(false, true).unwrap(),
            V23GlobalAdcCausalClass::FaithfulReducer
        );
        assert_eq!(
            classify_v23_global_adc(true, true).unwrap(),
            V23GlobalAdcCausalClass::Router
        );
        assert_eq!(
            classify_v23_global_adc(true, false).unwrap(),
            V23GlobalAdcCausalClass::Router
        );
    }

    #[test]
    fn v23_global_adc_result_is_canonical_claim_ineligible_and_uses_frozen_gates() {
        let fixture = global_adc_fixture();
        let result = diagnose_v23_global_adc(fixture.request()).unwrap();
        assert!(!result.claim_eligible);
        assert_eq!(result.selection_width, 8);
        assert_eq!(result.gates.aggregate_recall_ppm, 975_000);
        assert_eq!(result.gates.minimum_query_recall_ppm, 800_000);
        assert_eq!(result.gates.oracle_attainment_ppm, 995_000);
        let first = canonical_v23_global_adc_result_bytes(&result).unwrap();
        let second = canonical_v23_global_adc_result_bytes(&result).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.last(), Some(&b'\n'));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&first).unwrap()["claim_eligible"],
            false
        );
    }

    fn global_adc_object_identity(
        role: &str,
        marker: char,
        encoded_bytes: u64,
    ) -> V23GlobalAdcObjectIdentity {
        V23GlobalAdcObjectIdentity {
            role: role.to_string(),
            uri: format!("s3://frozen-v23/{role}"),
            digest_algorithm: if role == "selector" {
                "blake3"
            } else {
                "sha256"
            }
            .to_string(),
            digest: marker.to_string().repeat(64),
            encoded_bytes,
        }
    }

    fn global_adc_evidence_identity(fixture: &GlobalAdcFixture) -> V23GlobalAdcEvidenceIdentity {
        let mut identity = V23GlobalAdcEvidenceIdentity {
            source_commit: "1".repeat(40),
            source_archive_sha256: "2".repeat(64),
            index_id: "bvs3-frozen-index".to_string(),
            d1_report: global_adc_object_identity("d1-report", '3', 3_749_135),
            d2_terminal: global_adc_object_identity("d2-terminal", '4', 2_893),
            d2_result: global_adc_object_identity("d2-result", '5', 4_096),
            d2_report: global_adc_object_identity("d2-report", '6', 131_072),
            roster: global_adc_object_identity("page-roster", '7', 65_536),
            query: global_adc_object_identity("query-parquet", '8', 32 * 96 * 4),
            selector: global_adc_object_identity("selector", '9', 201_389_348),
        };
        identity.selector.digest = fixture.selector_ref.checksum.clone();
        identity.selector.encoded_bytes = fixture.selector_ref.encoded_bytes;
        identity
    }

    fn global_adc_artifact_reports(fixture: &GlobalAdcFixture) -> (V23D1Report, V23D2Report) {
        let dimensions = fixture.selector_ref.dimensions as usize;
        let d1_query_samples = |key: V23D1ArmKey| {
            let wave_candidate_rows =
                v23_d1_projected_page_rows(key.code_width_bytes, 32) * V23_WAVE_MAX_PAGES as u64;
            (0_u32..V23_DIAGNOSTIC_QUERIES as u32)
                .map(|query_index| V23D1QuerySample {
                    query_index,
                    ground_truth_ids: ranked_top_ten().ids,
                    oracle: ranked_top_ten(),
                    scalar_oracle: ranked_top_ten(),
                    routed: ranked_top_ten(),
                    oracle_candidate_rows: 2_048,
                    routed_candidate_rows: fixture.selector_ref.row_count,
                    wave_candidate_rows,
                    oracle_hits: 10,
                    routed_hits: 10,
                    cpu_ns: 1_000_000,
                })
                .collect::<Vec<_>>()
        };
        let mut d1_arms = [
            V23D1ArmKey {
                family: V23QuantizerFamily::F16Flat,
                code_width_bytes: (dimensions * 2) as u16,
            },
            V23D1ArmKey {
                family: V23QuantizerFamily::SrhtPq,
                code_width_bytes: 8,
            },
            V23D1ArmKey {
                family: V23QuantizerFamily::SrhtPq,
                code_width_bytes: 12,
            },
        ]
        .into_iter()
        .map(|key| {
            let (quantizer_state, quantizer_checksum) = if key
                == (V23D1ArmKey {
                    family: V23QuantizerFamily::SrhtPq,
                    code_width_bytes: 12,
                }) {
                (
                    fixture.d1_selector_arm.quantizer_state.clone(),
                    fixture.d1_selector_arm.quantizer_checksum.clone(),
                )
            } else {
                serialized_test_quantizer(key.family, key.code_width_bytes, dimensions)
            };
            V23D1Arm {
                key,
                quantizer_checksum,
                quantizer_state,
                query_samples: d1_query_samples(key),
                oracle_recall_ppm: 1_000_000,
                routed_recall_ppm: 1_000_000,
                scalar_simd_ids_equal: true,
                scalar_simd_max_distance_delta_ppm: 0,
                cpu_p99_ns: 1_000_000,
                wave_projected_bytes: v23_d1_projected_page_bytes(key.code_width_bytes, 32)
                    * V23_WAVE_MAX_PAGES as u64,
                passed: true,
            }
        })
        .collect::<Vec<_>>();
        d1_arms.sort_unstable_by_key(|arm| arm.key);
        let query_ordinals = (0_u64..V23_DIAGNOSTIC_QUERIES as u64).collect::<Vec<_>>();
        let d1_report = V23D1Report {
            schema: "borsuk-v23-d1-v5".to_string(),
            v20_root_checksum: "a".repeat(64),
            v20_codebook_checksum: "b".repeat(64),
            sample_ordinals_checksum: "c".repeat(64),
            query_vectors_checksum: super::v23_query_vectors_checksum(
                &query_ordinals,
                &fixture.queries,
            )
            .unwrap(),
            query_ordinals,
            rows: fixture.selector_ref.row_count,
            dimensions: fixture.selector_ref.dimensions,
            routing_cell_count: usize::try_from(fixture.selector_ref.coarse_cells).unwrap(),
            maximum_record_id_bytes: 32,
            arms: d1_arms,
        };

        let ground_truth_ids = ranked_top_ten().ids;
        let ranked = V23RankedResult {
            ids: ground_truth_ids[..8]
                .iter()
                .cloned()
                .chain([vec![b'x', 0], vec![b'x', 1]])
                .collect(),
            distances: (0_u8..10).map(f32::from).collect(),
        };
        let selected_pages = (0_u32..8).collect::<Vec<_>>();
        let encoded_bytes = selected_pages
            .iter()
            .map(|page| fixture.pages[*page as usize].encoded_bytes)
            .sum();
        let candidate_rows = selected_pages
            .iter()
            .map(|page| {
                let page = &fixture.pages[*page as usize];
                u64::from(page.primary_rows) + u64::from(page.replicated_rows)
            })
            .sum();
        let query_samples = (0_u32..V23_DIAGNOSTIC_QUERIES as u32)
            .map(|query_index| V23D2QuerySample {
                query_index,
                page_ordinals: selected_pages.clone(),
                oracle_page_ordinals: selected_pages.clone(),
                ground_truth_page_assignments: (0_u32..10).map(|rank| vec![rank % 16]).collect(),
                encoded_bytes,
                candidate_rows,
                selector_candidate_rows: fixture.selector_ref.row_count,
                selector_routed_cells: V23_SELECTOR_ROUTING_CELLS as u16,
                selector_ranked_rows: fixture.selector_ref.row_count as u32,
                ground_truth_ids: ground_truth_ids.clone(),
                ranked: ranked.clone(),
                gt_page_hits: 8,
                oracle_gt_page_hits: 8,
                hits: 8,
                recall_ppm: 800_000,
                cpu_ns: 1_000_000,
            })
            .collect::<Vec<_>>();
        let d1_key = V23D1ArmKey {
            family: V23QuantizerFamily::F16Flat,
            code_width_bytes: (dimensions * 2) as u16,
        };
        let mut d2_arms = V23_SELECTOR_CODE_WIDTHS
            .into_iter()
            .map(|code_width| {
                let mut selector = fixture.selector_ref.clone();
                selector.code_width = code_width;
                if code_width != fixture.selector_ref.code_width {
                    selector.checksum = format!("{:064x}", u64::from(code_width));
                    selector.path = format!("selectors/{}", selector.checksum);
                    selector.encoded_bytes = super::V23_SELECTOR_HEADER_BYTES as u64
                        + u64::from(selector.coarse_cells) * u64::from(selector.dimensions) * 4
                        + (u64::from(selector.coarse_cells) + 1) * 4
                        + selector.row_count * (u64::from(code_width) + 8);
                }
                let (projected_root_bytes, projected_ram_bytes) = v23_d2_projected_memory(
                    selector.row_count,
                    fixture.pages.len(),
                    dimensions,
                    selector.coarse_cells,
                    code_width,
                )
                .unwrap();
                V23D2Arm {
                    d1_key,
                    selector_key: V23D1ArmKey {
                        family: V23QuantizerFamily::SrhtPq,
                        code_width_bytes: code_width,
                    },
                    selector,
                    selector_routing_cells: V23_SELECTOR_ROUTING_CELLS as u16,
                    selector_ranked_row_cap: V23_SELECTOR_RANKED_ROWS as u32,
                    primary_target_rows: 384,
                    maximum_assignments_per_row: 2,
                    maximum_query_pages: V23_WAVE_MAX_PAGES as u8,
                    maximum_record_id_bytes: 32,
                    pages: fixture.pages.clone(),
                    unique_rows: fixture.selector_ref.row_count,
                    total_assignments: fixture
                        .pages
                        .iter()
                        .map(|page| u64::from(page.primary_rows) + u64::from(page.replicated_rows))
                        .sum(),
                    storage_amplification_ppm: fixture
                        .pages
                        .iter()
                        .map(|page| u64::from(page.primary_rows) + u64::from(page.replicated_rows))
                        .sum::<u64>()
                        .saturating_mul(1_000_000)
                        / fixture.selector_ref.row_count,
                    projected_root_bytes,
                    projected_ram_bytes,
                    projected_build_bytes: 1,
                    query_samples: query_samples.clone(),
                    aggregate_recall_ppm: 800_000,
                    minimum_query_recall_ppm: 800_000,
                    coverage_oracle_recall_ppm: 800_000,
                    coverage_oracle_minimum_query_recall_ppm: 800_000,
                    selector_regret_ppm: 1_000_000,
                    cpu_p99_ns: 1_000_000,
                    passed: false,
                }
            })
            .collect::<Vec<_>>();
        let projected_build_peak = d2_arms
            .iter()
            .map(super::v23_d2_arm_build_projection)
            .collect::<super::Result<Vec<_>>>()
            .unwrap()
            .into_iter()
            .max()
            .unwrap();
        for arm in &mut d2_arms {
            arm.projected_build_bytes = projected_build_peak;
        }
        let d2_report = V23D2Report {
            schema: "borsuk-v23-d2-v9".to_string(),
            d1_report_checksum: v23_d1_report_checksum(&d1_report).unwrap(),
            query_ordinals: d1_report.query_ordinals.clone(),
            rows: fixture.selector_ref.row_count,
            arms: d2_arms,
        };
        (d1_report, d2_report)
    }

    fn global_adc_artifact_request<'a>(
        fixture: &'a GlobalAdcFixture,
        d1_report: &'a V23D1Report,
        d2_report: &'a V23D2Report,
        registered_identity: &'a V23GlobalAdcEvidenceIdentity,
        observed_identity: &'a V23GlobalAdcEvidenceIdentity,
    ) -> V23GlobalAdcArtifactRequest<'a> {
        V23GlobalAdcArtifactRequest {
            d1_report,
            d2_report,
            pages: &fixture.pages,
            query_ordinals: &d2_report.query_ordinals,
            queries: &fixture.queries,
            ground_truth_page_assignments: &fixture.ground_truth_page_assignments,
            selector_bytes: fixture.selector_bytes.clone(),
            registered_identity,
            observed_identity,
        }
    }

    #[test]
    fn v23_global_adc_artifact_wrapper_rejects_inputs_and_registered_identity_drift() {
        let fixture = global_adc_fixture();
        let (d1_report, d2_report) = global_adc_artifact_reports(&fixture);
        validate_d1_report(&d1_report).unwrap();
        validate_d2_report(&d2_report).unwrap();
        let registered = global_adc_evidence_identity(&fixture);
        let observed = registered.clone();
        let baseline =
            global_adc_artifact_request(&fixture, &d1_report, &d2_report, &registered, &observed);
        assert!(validate_v23_global_adc_artifact_request(baseline).is_ok());

        let mut changed_d1 = d1_report.clone();
        changed_d1.arms[0].quantizer_checksum = "a".repeat(64);
        assert!(
            validate_v23_global_adc_artifact_request(global_adc_artifact_request(
                &fixture,
                &changed_d1,
                &d2_report,
                &registered,
                &observed,
            ))
            .is_err()
        );

        let mut changed_d2 = d2_report.clone();
        changed_d2.query_ordinals.swap(0, 1);
        assert!(
            validate_v23_global_adc_artifact_request(global_adc_artifact_request(
                &fixture,
                &d1_report,
                &changed_d2,
                &registered,
                &observed,
            ))
            .is_err()
        );

        let mut changed_fixture = global_adc_fixture();
        changed_fixture.pages[0].checksum = "b".repeat(64);
        assert!(
            validate_v23_global_adc_artifact_request(global_adc_artifact_request(
                &changed_fixture,
                &d1_report,
                &d2_report,
                &registered,
                &observed,
            ))
            .is_err()
        );

        let mut changed_fixture = global_adc_fixture();
        changed_fixture.queries[0][0] = f32::from_bits(changed_fixture.queries[0][0].to_bits() + 1);
        assert!(
            validate_v23_global_adc_artifact_request(global_adc_artifact_request(
                &changed_fixture,
                &d1_report,
                &d2_report,
                &registered,
                &observed,
            ))
            .is_err()
        );

        let mut changed_fixture = global_adc_fixture();
        let mut changed_selector = changed_fixture.selector_bytes.to_vec();
        changed_selector[0] ^= 1;
        changed_fixture.selector_bytes = Bytes::from(changed_selector);
        assert!(
            validate_v23_global_adc_artifact_request(global_adc_artifact_request(
                &changed_fixture,
                &d1_report,
                &d2_report,
                &registered,
                &observed,
            ))
            .is_err()
        );

        let mut coherent_but_unregistered = observed.clone();
        coherent_but_unregistered.query.uri = "s3://frozen-v23/replaced-query".to_string();
        coherent_but_unregistered.query.digest = "f".repeat(64);
        assert!(
            validate_v23_global_adc_artifact_request(global_adc_artifact_request(
                &fixture,
                &d1_report,
                &d2_report,
                &registered,
                &coherent_but_unregistered,
            ))
            .is_err()
        );
    }

    #[test]
    fn v23_global_adc_artifact_result_recomputes_evidence_and_rejects_identity_drift() {
        let fixture = global_adc_fixture();
        let identity = global_adc_evidence_identity(&fixture);
        let diagnostic = diagnose_v23_global_adc(fixture.request()).unwrap();
        let result = V23GlobalAdcArtifactResult {
            schema: "borsuk-v23-global-adc-diagnostic-v1".to_string(),
            claim_eligible: false,
            evidence: identity.clone(),
            diagnostic,
        };
        let baseline = canonical_v23_global_adc_artifact_result_bytes(&result, &identity).unwrap();
        assert_eq!(baseline.last(), Some(&b'\n'));

        let mut changed = result.clone();
        changed.diagnostic.faithful.query_samples[0].query_index = 1;
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.faithful.query_samples[0]
            .page_ordinals
            .swap(0, 1);
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.faithful.query_samples[0]
            .page_ordinals
            .pop();
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.faithful.query_samples[0].gt_page_hits ^= 1;
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.faithful.query_samples[0].oracle_gt_page_hits ^= 1;
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.faithful.query_samples[0].recall_ppm ^= 1;
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.faithful.query_samples[0].minimum_distance = f32::NAN;
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.faithful.reducer = "per-page-min".to_string();
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.faithful.aggregate_recall_ppm ^= 1;
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.faithful.minimum_query_recall_ppm ^= 1;
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.faithful.oracle_attainment_ppm ^= 1;
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.faithful.passed = !changed.diagnostic.faithful.passed;
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.per_page_min.aggregate_recall_ppm ^= 1;
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.per_page_min.minimum_query_recall_ppm ^= 1;
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.per_page_min.oracle_attainment_ppm ^= 1;
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.per_page_min.passed = !changed.diagnostic.per_page_min.passed;
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.scalar_simd_pages_equal = false;
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.scalar_simd_max_distance_delta_ppm =
            super::V23_SCALAR_SIMD_MAX_DISTANCE_DELTA_PPM + 1;
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.diagnostic.causal_classification = V23GlobalAdcCausalClass::FaithfulReducer;
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.schema = "borsuk-v23-global-adc-diagnostic-v2".to_string();
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let mut changed = result.clone();
        changed.claim_eligible = true;
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        let count_mutations: [fn(&mut super::V23GlobalAdcResult); 4] = [
            |diagnostic: &mut super::V23GlobalAdcResult| diagnostic.selector_cells_scanned ^= 1,
            |diagnostic: &mut super::V23GlobalAdcResult| diagnostic.selector_rows_scanned ^= 1,
            |diagnostic: &mut super::V23GlobalAdcResult| diagnostic.selection_width ^= 1,
            |diagnostic: &mut super::V23GlobalAdcResult| diagnostic.page_body_reads ^= 1,
        ];
        for mutate in count_mutations {
            let mut changed = result.clone();
            mutate(&mut changed.diagnostic);
            assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());
        }

        let mut changed = result.clone();
        changed.evidence.source_commit = "a".repeat(40);
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());
        let mut changed = result.clone();
        changed.evidence.source_archive_sha256 = "b".repeat(64);
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());
        let mut changed = result.clone();
        changed.evidence.index_id = "valid-looking-replacement-index".to_string();
        assert!(canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err());

        for role in 0..7 {
            for field in 0..3 {
                let mut changed = result.clone();
                let object = match role {
                    0 => &mut changed.evidence.d1_report,
                    1 => &mut changed.evidence.d2_terminal,
                    2 => &mut changed.evidence.d2_result,
                    3 => &mut changed.evidence.d2_report,
                    4 => &mut changed.evidence.roster,
                    5 => &mut changed.evidence.query,
                    6 => &mut changed.evidence.selector,
                    _ => unreachable!(),
                };
                match field {
                    0 => object.uri.push_str("-valid-looking-replacement"),
                    1 => object.digest = "e".repeat(64),
                    2 => object.encoded_bytes += 1,
                    _ => unreachable!(),
                }
                assert!(
                    canonical_v23_global_adc_artifact_result_bytes(&changed, &identity).is_err()
                );
            }
        }
    }

    struct GlobalAdcLocalBundle {
        _temporary: tempfile::TempDir,
        paths: V23GlobalAdcLocalArtifactPaths,
        identity: V23GlobalAdcEvidenceIdentity,
        expected_queries: Vec<Vec<f32>>,
    }

    fn write_global_adc_json(path: &PathBuf, value: &serde_json::Value) {
        let mut bytes =
            serde_json::to_vec(&super::v23_canonical_json_value(value.clone())).unwrap();
        bytes.push(b'\n');
        fs::write(path, bytes).unwrap();
    }

    struct CanonicalJsonOrder;

    impl<'de> DeserializeSeed<'de> for CanonicalJsonOrder {
        type Value = ();

        fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer.deserialize_any(CanonicalJsonOrderVisitor)
        }
    }

    struct CanonicalJsonOrderVisitor;

    impl<'de> Visitor<'de> for CanonicalJsonOrderVisitor {
        type Value = ();

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("JSON with recursively sorted object keys")
        }

        fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
            Ok(())
        }

        fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
            Ok(())
        }

        fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
            Ok(())
        }

        fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
            Ok(())
        }

        fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
            Ok(())
        }

        fn visit_string<E>(self, _value: String) -> Result<Self::Value, E> {
            Ok(())
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(())
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            while sequence.next_element_seed(CanonicalJsonOrder)?.is_some() {}
            Ok(())
        }

        fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut previous: Option<String> = None;
            while let Some(key) = object.next_key::<String>()? {
                if previous.as_ref().is_some_and(|prior| prior >= &key) {
                    return Err(serde::de::Error::custom(format!(
                        "object key {key:?} is not after {previous:?}"
                    )));
                }
                object.next_value_seed(CanonicalJsonOrder)?;
                previous = Some(key);
            }
            Ok(())
        }
    }

    fn assert_compact_sorted_json(path: &Path, bytes: &[u8]) {
        assert_eq!(
            bytes.last(),
            Some(&b'\n'),
            "{} lacks trailing LF",
            path.display()
        );
        let body = &bytes[..bytes.len() - 1];
        assert_ne!(
            body.last(),
            Some(&b'\n'),
            "{} has multiple trailing LFs",
            path.display()
        );
        let mut in_string = false;
        let mut escaped = false;
        for (offset, byte) in body.iter().copied().enumerate() {
            if in_string {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    in_string = false;
                }
            } else if byte == b'"' {
                in_string = true;
            } else {
                assert!(
                    !matches!(byte, b' ' | b'\t' | b'\r' | b'\n'),
                    "{} has noncompact whitespace at byte {offset}",
                    path.display()
                );
            }
        }
        assert!(
            !in_string && !escaped,
            "{} has unterminated JSON string",
            path.display()
        );
        let mut deserializer = serde_json::Deserializer::from_slice(body);
        CanonicalJsonOrder
            .deserialize(&mut deserializer)
            .unwrap_or_else(|error| {
                panic!("{} is not recursively key-sorted: {error}", path.display())
            });
        deserializer.end().unwrap_or_else(|error| {
            panic!("{} has trailing JSON content: {error}", path.display())
        });
    }

    fn write_global_adc_terminal(path: &PathBuf, fields: Vec<(&'static str, serde_json::Value)>) {
        let mut bytes = Vec::new();
        bytes.push(b'{');
        for (index, (key, value)) in fields.into_iter().enumerate() {
            if index != 0 {
                bytes.push(b',');
            }
            bytes.extend(serde_json::to_vec(key).unwrap());
            bytes.push(b':');
            bytes.extend(serde_json::to_vec(&value).unwrap());
        }
        bytes.extend_from_slice(b"}\n");
        fs::write(path, bytes).unwrap();
    }

    #[derive(Clone, Copy)]
    struct GlobalAdcQueryShape {
        dimensions: usize,
        child_name: &'static str,
        field_nullable: bool,
        child_nullable: bool,
    }

    const AUTHENTIC_GLOBAL_ADC_QUERY_SHAPE: GlobalAdcQueryShape = GlobalAdcQueryShape {
        dimensions: 96,
        child_name: "element",
        field_nullable: false,
        child_nullable: false,
    };

    fn write_global_adc_query_fixture(
        path: &PathBuf,
        queries: &[Vec<f32>],
        physical_rows: usize,
        shape: GlobalAdcQueryShape,
        non_finite_at: Option<(usize, usize)>,
    ) {
        assert_eq!(queries.len(), V23_DIAGNOSTIC_QUERIES);
        assert!(physical_rows >= queries.len());
        let mut values = Vec::with_capacity(physical_rows * shape.dimensions);
        for row in 0..physical_rows {
            if let Some(query) = queries.get(row) {
                values.extend_from_slice(&query[..shape.dimensions]);
            } else {
                values.extend(
                    (0..shape.dimensions)
                        .map(|dimension| (((row * 17 + dimension * 13) % 251) + 1) as f32 / 252.0),
                );
            }
        }
        if let Some((row, dimension)) = non_finite_at {
            values[row * shape.dimensions + dimension] = f32::NAN;
        }
        let item = Arc::new(Field::new(
            shape.child_name,
            DataType::Float32,
            shape.child_nullable,
        ));
        let embeddings = FixedSizeListArray::try_new(
            Arc::clone(&item),
            shape.dimensions as i32,
            Arc::new(Float32Array::from(values)),
            None,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "emb",
            DataType::FixedSizeList(item, shape.dimensions as i32),
            shape.field_nullable,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(embeddings)]).unwrap();
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn write_global_adc_queries(path: &PathBuf, queries: &[Vec<f32>], physical_rows: usize) {
        write_global_adc_query_fixture(
            path,
            queries,
            physical_rows,
            AUTHENTIC_GLOBAL_ADC_QUERY_SHAPE,
            None,
        );
    }

    fn global_adc_file_sha256(path: &PathBuf) -> String {
        format!("{:x}", Sha256::digest(fs::read(path).unwrap()))
    }

    fn refresh_global_adc_object(object: &mut V23GlobalAdcObjectIdentity, path: &PathBuf) {
        let bytes = fs::read(path).unwrap();
        object.encoded_bytes = bytes.len() as u64;
        object.digest = if object.digest_algorithm == "blake3" {
            blake3::hash(&bytes).to_hex().to_string()
        } else {
            format!("{:x}", Sha256::digest(bytes))
        };
    }

    fn global_adc_unselectable_fixture() -> GlobalAdcFixture {
        let mut fixture = global_adc_fixture();
        let mut bytes = fixture.selector_bytes.to_vec();
        let rows_start = V23_SELECTOR_HEADER_BYTES
            + 4_096 * fixture.selector_ref.dimensions as usize * 4
            + 4_097 * 4;
        let primary_start = rows_start;
        let replica_start = primary_start + fixture.selector_ref.row_count as usize * 4;
        for row in 0..fixture.selector_ref.row_count as usize {
            let primary = (row % fixture.pages.len()) as u32;
            let replica = if primary == 0 { u32::MAX } else { 0 };
            let primary_offset = primary_start + row * 4;
            let replica_offset = replica_start + row * 4;
            bytes[primary_offset..primary_offset + 4].copy_from_slice(&primary.to_le_bytes());
            bytes[replica_offset..replica_offset + 4].copy_from_slice(&replica.to_le_bytes());
        }
        for page in &mut fixture.pages {
            page.primary_rows = 2;
            page.replicated_rows = if page.page_ordinal == 0 { 30 } else { 0 };
        }
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        fixture.selector_ref.checksum = checksum.clone();
        fixture.selector_ref.path = format!("selectors/{checksum}");
        fixture.selector_bytes = Bytes::from(bytes);
        fixture
    }

    fn global_adc_local_bundle_from_fixture(
        fixture: &GlobalAdcFixture,
        d1_report: V23D1Report,
        mut d2_report: V23D2Report,
    ) -> GlobalAdcLocalBundle {
        let temporary = tempfile::tempdir().unwrap();
        let path = |name: &str| temporary.path().join(name);
        let paths = V23GlobalAdcLocalArtifactPaths {
            d1_report: path("bench_v23_d1_report.json"),
            d2_terminal: path("terminal.json"),
            d2_result: path("RESULT_COMPLETE.json"),
            d2_report: path("bench_v23_d2_report.json"),
            roster: path("bench_v23_pages.json"),
            query: path("query.parquet"),
            selector: path("selector.bvs"),
        };

        let source_commit = "1".repeat(40);
        let source_archive_sha256 = "2".repeat(64);
        let index_id = "synthetic-v23-index";
        let dataset_id = "deep-image-96";
        let base_cell_id = "r01-synthetic-base";
        let diagnostic_cell_id = "r01-synthetic-diagnostic";
        let attempt_id = "runtime-v23-d2-r01-synthetic-diagnostic-arm-0000-a0001";
        let instance_id = "i-0123456789abcdef0";
        let page_uri = "s3://frozen-v23/runtime-v23-d2/pages";

        write_global_adc_json(
            &paths.d1_report,
            &serde_json::json!({
                "claim_eligible": false,
                "dataset_id": dataset_id,
                "document_kind": "publication-v3-v23-d1-report",
                "index_id": index_id,
                "report": d1_report.clone(),
                "schema": "borsuk-v23-d1-artifact-v1",
                "source_archive_sha256": source_archive_sha256.clone(),
                "stage": "d1",
            }),
        );
        let d1_report_sha256 = global_adc_file_sha256(&paths.d1_report);
        d2_report.d1_report_checksum = v23_d1_report_checksum(&d1_report).unwrap();
        write_global_adc_json(
            &paths.d2_report,
            &serde_json::json!({
                "claim_eligible": false,
                "d1_report_sha256": d1_report_sha256.clone(),
                "dataset_id": dataset_id,
                "document_kind": "publication-v3-v23-d2-report",
                "index_id": index_id,
                "page_uri": page_uri,
                "report": d2_report,
                "schema": "borsuk-v23-d2-artifact-v1",
                "source_archive_sha256": source_archive_sha256.clone(),
                "stage": "d2",
            }),
        );
        write_global_adc_json(
            &paths.roster,
            &serde_json::json!({
                "claim_eligible": false,
                "d1_report_sha256": d1_report_sha256.clone(),
                "dataset_id": dataset_id,
                "document_kind": "publication-v3-v23-page-roster",
                "index_id": index_id,
                "page_uri": page_uri,
                "pages": fixture.pages,
                "schema": "borsuk-v23-pages-v1",
                "source_archive_sha256": source_archive_sha256.clone(),
                "stage": "d2",
            }),
        );
        write_global_adc_queries(&paths.query, &fixture.queries, 10_000);
        fs::write(&paths.selector, &fixture.selector_bytes).unwrap();

        let report_sha256 = global_adc_file_sha256(&paths.d2_report);
        let roster_sha256 = global_adc_file_sha256(&paths.roster);
        let mut resources = serde_json::Map::new();
        resources.insert("cpu_ns".to_string(), serde_json::Value::from(1));
        resources.insert("disk_read_bytes".to_string(), serde_json::Value::from(0));
        resources.insert("disk_write_bytes".to_string(), serde_json::Value::from(1));
        resources.insert(
            "peak_rss_bytes".to_string(),
            serde_json::Value::from(768_u64 * 1024 * 1024),
        );
        let mut runtime_attestation = serde_json::Map::new();
        for (key, value) in [
            ("architecture", serde_json::Value::from("aarch64")),
            ("attempt_id", serde_json::Value::from(attempt_id)),
            (
                "cache_capacity_bytes",
                serde_json::Value::from(64_u64 * 1024 * 1024 * 1024),
            ),
            ("cache_device", serde_json::Value::from("259:1")),
            (
                "cache_filesystem_bytes",
                serde_json::Value::from(4_398_046_511_104_u64),
            ),
            ("cache_is_mount", serde_json::Value::from(false)),
            ("cell_id", serde_json::Value::from(diagnostic_cell_id)),
            ("effective_disk_cache_max_bytes", serde_json::Value::from(0)),
            ("instance_id", serde_json::Value::from(instance_id)),
            ("instance_type", serde_json::Value::from("r7g.8xlarge")),
            (
                "memory_max_bytes",
                serde_json::Value::from(32_u64 * 1024 * 1024 * 1024),
            ),
            (
                "memory_peak_bytes",
                serde_json::Value::from(1024_u64 * 1024 * 1024),
            ),
            ("oom_events", serde_json::Value::from(0)),
            ("oom_kill_events", serde_json::Value::from(0)),
            ("purchase_option", serde_json::Value::from("spot")),
            ("root_device", serde_json::Value::from("259:1")),
            ("schema_version", serde_json::Value::from(2)),
            (
                "source_revision",
                serde_json::Value::from(source_commit.clone()),
            ),
            ("swap_current_bytes", serde_json::Value::from(0)),
            ("swap_max_bytes", serde_json::Value::from(0)),
            ("swap_peak_bytes", serde_json::Value::from(0)),
            ("vcpus", serde_json::Value::from(32)),
        ] {
            runtime_attestation.insert(key.to_string(), value);
        }
        let mut result_document = serde_json::Map::new();
        for (key, value) in [
            ("arms", serde_json::Value::from(2)),
            (
                "artifact_sha256",
                serde_json::Value::from(report_sha256.clone()),
            ),
            ("attempt_id", serde_json::Value::from(attempt_id)),
            ("cell_id", serde_json::Value::from(base_cell_id)),
            ("claim_eligible", serde_json::Value::from(false)),
            (
                "d1_report_sha256",
                serde_json::Value::from(d1_report_sha256.clone()),
            ),
            ("dataset_id", serde_json::Value::from(dataset_id)),
            (
                "dataset_materialization_sha256",
                serde_json::Value::from("ab".repeat(32)),
            ),
            (
                "diagnostic_cell_id",
                serde_json::Value::from(diagnostic_cell_id),
            ),
            (
                "document_kind",
                serde_json::Value::from("publication-v3-v23-d2-summary"),
            ),
            ("elapsed_ns", serde_json::Value::from(1)),
            ("index_id", serde_json::Value::from(index_id)),
            ("instance_identity", serde_json::Value::from(instance_id)),
            ("pages", serde_json::Value::from(fixture.pages.len() as u64)),
            (
                "pages_sha256",
                serde_json::Value::from(roster_sha256.clone()),
            ),
            ("passed", serde_json::Value::from(false)),
            ("passing_arm_indexes", serde_json::Value::Array(Vec::new())),
            ("publishable", serde_json::Value::from(false)),
            (
                "queries",
                serde_json::Value::from(V23_DIAGNOSTIC_QUERIES as u64),
            ),
            ("resources", serde_json::Value::Object(resources)),
            (
                "rows",
                serde_json::Value::from(fixture.selector_ref.row_count),
            ),
            (
                "runtime_attestation",
                serde_json::Value::Object(runtime_attestation),
            ),
            ("schema", serde_json::Value::from("borsuk-v23-summary-v1")),
            (
                "source_archive_sha256",
                serde_json::Value::from(source_archive_sha256.clone()),
            ),
            ("stage", serde_json::Value::from("d2")),
        ] {
            result_document.insert(key.to_string(), value);
        }
        write_global_adc_json(
            &paths.d2_result,
            &serde_json::Value::Object(result_document),
        );
        let result_sha256 = global_adc_file_sha256(&paths.d2_result);
        write_global_adc_terminal(
            &paths.d2_terminal,
            vec![
                ("schema_version", serde_json::json!(5)),
                ("status", serde_json::json!("complete")),
                ("role", serde_json::json!("runtime")),
                ("attempt", serde_json::json!(1)),
                ("attempt_id", serde_json::json!(attempt_id)),
                ("instance_id", serde_json::json!(instance_id)),
                (
                    "source_archive_sha256",
                    serde_json::json!(source_archive_sha256.clone()),
                ),
                ("manifest_sha256", serde_json::json!("33".repeat(32))),
                ("protocol_sha256", serde_json::json!("44".repeat(32))),
                ("binary_sha256", serde_json::json!("55".repeat(32))),
                ("purchase_option", serde_json::json!("spot")),
                ("runtime_profile", serde_json::json!("recall")),
                ("arm_index", serde_json::json!(0)),
                ("max_active_searches", serde_json::json!(4)),
                ("max_waiting_searches", serde_json::json!(16)),
                ("leaf_read_width", serde_json::json!(32)),
                ("max_inflight_leaf_reads", serde_json::json!(48)),
                ("max_parallel_decode_rank_tasks", serde_json::json!(2)),
                ("cpu_threads", serde_json::json!(3)),
                ("io_threads", serde_json::json!(88)),
                ("s3_get_concurrency", serde_json::json!(64)),
                (
                    "ram_budget_bytes",
                    serde_json::json!(3 * 1024 * 1024 * 1024_u64),
                ),
                ("disk_cache_max_bytes", serde_json::json!(0)),
                (
                    "exact_read_max_physical_amplification",
                    serde_json::json!(2),
                ),
                (
                    "execution_contract_sha256",
                    serde_json::json!("66".repeat(32)),
                ),
                ("artifact_upload_reconciliations", serde_json::json!(0)),
                ("claim_eligible", serde_json::json!(false)),
                ("v23_stage", serde_json::json!("d2")),
                ("v23_passed", serde_json::json!(false)),
                ("v23_result_sha256", serde_json::json!(result_sha256)),
                ("v23_page_prefix", serde_json::json!(page_uri)),
                ("v23_d2_report_sha256", serde_json::json!(report_sha256)),
                ("v23_pages_sha256", serde_json::json!(roster_sha256)),
                ("v23_summary_sha256", serde_json::json!("77".repeat(32))),
                ("v23_d1_receipt_sha256", serde_json::json!("88".repeat(32))),
                ("v23_d1_report_sha256", serde_json::json!(d1_report_sha256)),
                (
                    "v23_prerequisite_binary_sha256",
                    serde_json::json!("55".repeat(32)),
                ),
                (
                    "base_build_terminal_sha256",
                    serde_json::json!("99".repeat(32)),
                ),
                ("base_manifest_sha256", serde_json::json!("aa".repeat(32))),
                ("base_protocol_sha256", serde_json::json!("bb".repeat(32))),
                (
                    "base_source_archive_sha256",
                    serde_json::json!("cc".repeat(32)),
                ),
                (
                    "base_index_receipt_sha256",
                    serde_json::json!("dd".repeat(32)),
                ),
                (
                    "base_object_roster_sha256",
                    serde_json::json!("ee".repeat(32)),
                ),
                ("base_inventory_sha256", serde_json::json!("ff".repeat(32))),
                ("base_index_id", serde_json::json!(index_id)),
                ("base_index_uri", serde_json::json!("s3://frozen-v23/index")),
                (
                    "diagnostic_source_archive_sha256",
                    serde_json::json!(source_archive_sha256.clone()),
                ),
                (
                    "memory_max_bytes",
                    serde_json::json!(32 * 1024 * 1024 * 1024_u64),
                ),
                ("memory_swap_max_bytes", serde_json::json!(0)),
                (
                    "memory_peak_bytes",
                    serde_json::json!(1024 * 1024 * 1024_u64),
                ),
            ],
        );

        let mut identity = global_adc_evidence_identity(fixture);
        identity.source_commit = source_commit;
        identity.source_archive_sha256 = source_archive_sha256;
        identity.index_id = index_id.to_string();
        for (object, path) in [
            (&mut identity.d1_report, &paths.d1_report),
            (&mut identity.d2_terminal, &paths.d2_terminal),
            (&mut identity.d2_result, &paths.d2_result),
            (&mut identity.d2_report, &paths.d2_report),
            (&mut identity.roster, &paths.roster),
            (&mut identity.query, &paths.query),
            (&mut identity.selector, &paths.selector),
        ] {
            refresh_global_adc_object(object, path);
        }
        GlobalAdcLocalBundle {
            _temporary: temporary,
            paths,
            identity,
            expected_queries: fixture.queries.clone(),
        }
    }

    fn global_adc_local_bundle() -> GlobalAdcLocalBundle {
        let fixture = global_adc_fixture();
        let (d1_report, d2_report) = global_adc_artifact_reports(&fixture);
        global_adc_local_bundle_from_fixture(&fixture, d1_report, d2_report)
    }

    #[test]
    fn v23_global_adc_local_artifacts_bind_seven_roles_and_run_without_page_bodies() {
        let bundle = global_adc_local_bundle();
        for path in [
            &bundle.paths.d1_report,
            &bundle.paths.d2_result,
            &bundle.paths.d2_report,
            &bundle.paths.roster,
        ] {
            let bytes = fs::read(path).unwrap();
            assert_compact_sorted_json(path, &bytes);
        }
        let terminal_bytes = fs::read(&bundle.paths.d2_terminal).unwrap();
        assert!(terminal_bytes.starts_with(
            b"{\"schema_version\":5,\"status\":\"complete\",\"role\":\"runtime\",\"attempt\":1,"
        ));
        let terminal_value: serde_json::Value = serde_json::from_slice(&terminal_bytes).unwrap();
        let mut sorted_terminal =
            serde_json::to_vec(&super::v23_canonical_json_value(terminal_value)).unwrap();
        sorted_terminal.push(b'\n');
        assert_ne!(terminal_bytes, sorted_terminal);

        let query_builder =
            ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&bundle.paths.query).unwrap())
                .unwrap();
        assert_eq!(query_builder.metadata().file_metadata().num_rows(), 10_000);
        assert_eq!(
            query_builder.schema().as_ref(),
            &Schema::new(vec![Field::new(
                "emb",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float32, false)),
                    96,
                ),
                false,
            )])
        );
        let loaded =
            load_v23_global_adc_local_artifacts(&bundle.paths, &bundle.identity, &bundle.identity)
                .unwrap();
        assert_eq!(loaded.queries, bundle.expected_queries);
        let result = loaded.run().unwrap();
        assert_eq!(result.diagnostic.page_body_reads, 0);
        assert!(!result.claim_eligible);
        assert_eq!(result.evidence, bundle.identity);
        let first =
            canonical_v23_global_adc_artifact_result_bytes(&result, &bundle.identity).unwrap();
        let second =
            canonical_v23_global_adc_artifact_result_bytes(&result, &bundle.identity).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.last(), Some(&b'\n'));
    }

    #[test]
    fn v23_global_adc_local_loaders_reject_role_and_cross_object_mutations() {
        for role in 0..7 {
            let mut bundle = global_adc_local_bundle();
            let path = match role {
                0 => &bundle.paths.d1_report,
                1 => &bundle.paths.d2_terminal,
                2 => &bundle.paths.d2_result,
                3 => &bundle.paths.d2_report,
                4 => &bundle.paths.roster,
                5 => &bundle.paths.query,
                6 => &bundle.paths.selector,
                _ => unreachable!(),
            };
            fs::write(path, b"valid-looking-wrong-role").unwrap();
            let object = match role {
                0 => &mut bundle.identity.d1_report,
                1 => &mut bundle.identity.d2_terminal,
                2 => &mut bundle.identity.d2_result,
                3 => &mut bundle.identity.d2_report,
                4 => &mut bundle.identity.roster,
                5 => &mut bundle.identity.query,
                6 => &mut bundle.identity.selector,
                _ => unreachable!(),
            };
            refresh_global_adc_object(object, path);
            assert!(
                load_v23_global_adc_local_artifacts(
                    &bundle.paths,
                    &bundle.identity,
                    &bundle.identity,
                )
                .is_err()
            );
        }

        for field in [
            "source_archive_sha256",
            "attempt_id",
            "instance_id",
            "v23_d1_report_sha256",
            "v23_result_sha256",
        ] {
            let mut bundle = global_adc_local_bundle();
            let mut terminal: serde_json::Value =
                serde_json::from_slice(&fs::read(&bundle.paths.d2_terminal).unwrap()).unwrap();
            terminal[field] = serde_json::Value::String("valid-looking-drift".to_string());
            write_global_adc_json(&bundle.paths.d2_terminal, &terminal);
            refresh_global_adc_object(&mut bundle.identity.d2_terminal, &bundle.paths.d2_terminal);
            assert!(
                load_v23_global_adc_local_artifacts(
                    &bundle.paths,
                    &bundle.identity,
                    &bundle.identity,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn v23_global_adc_local_queries_select_registered_ordinals_from_full_artifact() {
        let baseline = global_adc_local_bundle();
        let loaded = load_v23_global_adc_local_artifacts(
            &baseline.paths,
            &baseline.identity,
            &baseline.identity,
        )
        .unwrap();
        assert_eq!(loaded.queries, baseline.expected_queries);

        for mutation in 0..2 {
            let fixture = global_adc_fixture();
            let (mut d1_report, mut d2_report) = global_adc_artifact_reports(&fixture);
            match mutation {
                0 => *d1_report.query_ordinals.last_mut().unwrap() = 10_000,
                1 => d1_report.query_ordinals.swap(0, 1),
                _ => unreachable!(),
            }
            d1_report.query_vectors_checksum =
                super::v23_query_vectors_checksum(&d1_report.query_ordinals, &fixture.queries)
                    .unwrap();
            d2_report.query_ordinals = d1_report.query_ordinals.clone();
            d2_report.d1_report_checksum = v23_d1_report_checksum(&d1_report).unwrap();
            let bundle = global_adc_local_bundle_from_fixture(&fixture, d1_report, d2_report);
            assert!(
                load_v23_global_adc_local_artifacts(
                    &bundle.paths,
                    &bundle.identity,
                    &bundle.identity,
                )
                .is_err()
            );
        }

        for mutation in 0..6 {
            let fixture = global_adc_fixture();
            let (d1_report, d2_report) = global_adc_artifact_reports(&fixture);
            let mut bundle = global_adc_local_bundle_from_fixture(&fixture, d1_report, d2_report);
            match mutation {
                0 => write_global_adc_queries(&bundle.paths.query, &fixture.queries, 9_999),
                1 => write_global_adc_query_fixture(
                    &bundle.paths.query,
                    &fixture.queries,
                    10_000,
                    GlobalAdcQueryShape {
                        dimensions: 95,
                        ..AUTHENTIC_GLOBAL_ADC_QUERY_SHAPE
                    },
                    None,
                ),
                2 => write_global_adc_query_fixture(
                    &bundle.paths.query,
                    &fixture.queries,
                    10_000,
                    GlobalAdcQueryShape {
                        field_nullable: true,
                        ..AUTHENTIC_GLOBAL_ADC_QUERY_SHAPE
                    },
                    None,
                ),
                3 => write_global_adc_query_fixture(
                    &bundle.paths.query,
                    &fixture.queries,
                    10_000,
                    GlobalAdcQueryShape {
                        child_nullable: true,
                        ..AUTHENTIC_GLOBAL_ADC_QUERY_SHAPE
                    },
                    None,
                ),
                4 => write_global_adc_query_fixture(
                    &bundle.paths.query,
                    &fixture.queries,
                    10_000,
                    AUTHENTIC_GLOBAL_ADC_QUERY_SHAPE,
                    Some((9_999, 95)),
                ),
                5 => write_global_adc_query_fixture(
                    &bundle.paths.query,
                    &fixture.queries,
                    10_000,
                    GlobalAdcQueryShape {
                        child_name: "item",
                        ..AUTHENTIC_GLOBAL_ADC_QUERY_SHAPE
                    },
                    None,
                ),
                _ => unreachable!(),
            }
            refresh_global_adc_object(&mut bundle.identity.query, &bundle.paths.query);
            assert!(
                load_v23_global_adc_local_artifacts(
                    &bundle.paths,
                    &bundle.identity,
                    &bundle.identity,
                )
                .is_err()
            );
        }

        let fixture = global_adc_fixture();
        let (d1_report, d2_report) = global_adc_artifact_reports(&fixture);
        let mut bundle = global_adc_local_bundle_from_fixture(&fixture, d1_report, d2_report);
        let mut changed_queries = fixture.queries.clone();
        changed_queries[17][0] += 0.125;
        write_global_adc_queries(&bundle.paths.query, &changed_queries, 10_000);
        refresh_global_adc_object(&mut bundle.identity.query, &bundle.paths.query);
        assert!(
            load_v23_global_adc_local_artifacts(&bundle.paths, &bundle.identity, &bundle.identity,)
                .is_err()
        );
    }

    #[test]
    fn v23_global_adc_local_loader_authenticates_without_executing_science() {
        let fixture = global_adc_unselectable_fixture();
        let (d1_report, d2_report) = global_adc_artifact_reports(&fixture);
        let bundle = global_adc_local_bundle_from_fixture(&fixture, d1_report, d2_report);
        let loaded =
            load_v23_global_adc_local_artifacts(&bundle.paths, &bundle.identity, &bundle.identity)
                .unwrap();
        assert!(loaded.run().is_err());
    }

    #[test]
    fn v23_global_adc_local_loader_rejects_registered_identity_drift() {
        let bundle = global_adc_local_bundle();
        for field in 0..3 {
            let mut observed = bundle.identity.clone();
            match field {
                0 => observed.selector.uri.push_str("-replacement"),
                1 => observed.selector.digest = "e".repeat(64),
                2 => observed.selector.encoded_bytes += 1,
                _ => unreachable!(),
            }
            assert!(
                load_v23_global_adc_local_artifacts(&bundle.paths, &observed, &bundle.identity,)
                    .is_err()
            );
        }
    }

    #[test]
    fn v23_row_selector_codec_binds_both_page_labels_and_cover() {
        let dimensions = 96;
        let sample = (0..256)
            .map(|row| {
                (0..dimensions)
                    .map(|dimension| ((row * 17 + dimension * 13) % 251) as f32 / 251.0)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let quantizer =
            fit_v23_diagnostic_quantizer(V23QuantizerFamily::SrhtPq, 8, dimensions, &sample)
                .unwrap();
        let query = sample[17].clone();
        let input = V23SelectorInput {
            generation_checksum: [9; 32],
            metric: VectorMetric::SquaredEuclidean,
            dimensions: dimensions as u32,
            page_count: 3,
            code_width: 8,
            maximum_assignments_per_row: 2,
            coarse_centroids: vec![query.clone()],
            rows: vec![
                V23SelectorRow::new(0, 0, Some(1), 10, &quantizer.encode(&query).unwrap()),
                V23SelectorRow::new(0, 1, None, 11, &quantizer.encode(&sample[18]).unwrap()),
                V23SelectorRow::new(0, 2, None, 12, &quantizer.encode(&sample[200]).unwrap()),
            ],
        };
        let bytes = encode_v23_selector(&input).unwrap();
        assert_eq!(&bytes[..4], b"BVS3");
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let reference = V23SelectorRef {
            generation_checksum: input.generation_checksum,
            metric: input.metric.clone(),
            dimensions: input.dimensions,
            coarse_cells: 1,
            page_count: input.page_count,
            maximum_assignments_per_row: 2,
            code_width: input.code_width,
            row_count: 3,
            path: format!("selectors/{checksum}"),
            checksum,
            encoded_bytes: bytes.len() as u64,
        };
        let decoded = decode_v23_selector(bytes.clone(), &reference).unwrap();
        assert_eq!(decoded.cell_range(0), Some(0..3));
        assert_eq!(decoded.row_pages(0), Some((0, Some(1))));
        assert_eq!(decoded.row_code(0), Some(input.rows[0].code.as_ref()));

        let selector = V23PageSelector::from_encoded(&reference, bytes.clone(), quantizer).unwrap();
        let selection = selector.select(&query, 1).unwrap();
        assert_eq!(selection.page_ordinals, vec![1]);
        assert_eq!(selection.candidate_rows, 3);
        assert_eq!(selection.ranked_rows, 3);

        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(decode_v23_selector(Bytes::from(trailing), &reference).is_err());

        let mut wrong_width = input.clone();
        wrong_width.code_width = 9;
        assert!(encode_v23_selector(&wrong_width).is_err());

        let mut reordered = input.clone();
        reordered.rows.swap(0, 1);
        assert!(encode_v23_selector(&reordered).is_err());

        let mut duplicate_assignment = input.clone();
        duplicate_assignment.rows[0].replica_page = Some(0);
        assert!(encode_v23_selector(&duplicate_assignment).is_err());

        let quantizer_12 =
            fit_v23_diagnostic_quantizer(V23QuantizerFamily::SrhtPq, 12, dimensions, &sample)
                .unwrap();
        let mut input_12 = input.clone();
        input_12.code_width = 12;
        for (row, vector) in input_12
            .rows
            .iter_mut()
            .zip([&query, &sample[18], &sample[200]])
        {
            row.code = quantizer_12.encode(vector).unwrap().into_boxed_slice();
        }
        let bytes_12 = encode_v23_selector(&input_12).unwrap();
        let checksum_12 = blake3::hash(&bytes_12).to_hex().to_string();
        let reference_12 = V23SelectorRef {
            code_width: 12,
            checksum: checksum_12.clone(),
            path: format!("selectors/{checksum_12}"),
            encoded_bytes: bytes_12.len() as u64,
            ..reference.clone()
        };
        let selection_12 = V23PageSelector::from_encoded(&reference_12, bytes_12, quantizer_12)
            .unwrap()
            .select(&query, 1)
            .unwrap();
        assert_eq!(selection_12.page_ordinals, vec![1]);

        let mut assignment_drift = reference;
        assignment_drift.maximum_assignments_per_row = 1;
        assert!(decode_v23_selector(bytes, &assignment_drift).is_err());
    }

    #[test]
    fn v23_d2_cosine_query_views_keep_raw_selector_authority() {
        let raw = vec![1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0];
        let (selector_query, scoring_query) = v23_d2_query_views(&raw, true);
        assert!(std::ptr::eq(selector_query, raw.as_slice()));
        assert_eq!(scoring_query, crate::metric::unit_l2_normalized(&raw));
    }

    #[test]
    fn v23_page_coverage_oracle_separates_layout_from_router_regret() {
        let truth_assignments = vec![
            vec![0],
            vec![0, 4],
            vec![1],
            vec![1, 4],
            vec![2],
            vec![2, 5],
            vec![3],
            vec![3, 5],
            vec![6],
            vec![7],
        ];
        let oracle = best_v23_page_coverage(&truth_assignments, 4).unwrap();
        assert_eq!(oracle.hits, 8);
        assert_eq!(oracle.page_ordinals, vec![0, 1, 2, 3]);
    }

    #[test]
    fn v23_d2_contract_binds_content_selector_and_layout_oracle_evidence() {
        let canonical = canonical_d2_report();
        validate_d2_report(&canonical).unwrap();

        let mut selector_width = canonical.clone();
        selector_width.arms[0].selector_key.code_width_bytes = 9;
        assert!(validate_d2_report(&selector_width).is_err());

        let mut selector_candidates = canonical.clone();
        selector_candidates.arms[0].query_samples[0].selector_ranked_rows -= 1;
        assert!(validate_d2_report(&selector_candidates).is_err());

        let mut oracle_hits = canonical.clone();
        oracle_hits.arms[0].query_samples[0].oracle_gt_page_hits = 9;
        assert!(validate_d2_report(&oracle_hits).is_err());

        let mut oracle_pages = canonical;
        oracle_pages.arms[0].query_samples[0]
            .oracle_page_ordinals
            .push(0);
        assert!(validate_d2_report(&oracle_pages).is_err());
    }

    fn canonical_wave() -> V23WaveSample {
        V23WaveSample {
            query_index: 7,
            page_ordinals: vec![3, 9, 12, 18, 21, 27, 30, 36],
            encoded_bytes: 1_966_080,
            candidate_rows: 16_384,
            backing_gets: 8,
            backing_get_concurrency: 64,
            backing_bytes: 1_966_080,
            backing_queue_us_sum: 40,
            backing_queue_us_max: 20,
            backing_service_us_sum: 100_000,
            backing_service_us_max: 30_000,
            cpu_ns: 2_000_000,
            transient_admission_wait_ns: 2_000_000,
            request_admission_wait_ns: 1_000_000,
            service_ns: 37_000_000,
            elapsed_ns: 40_000_000,
        }
    }

    #[test]
    fn v23_d3_request_capacity_rejects_oversized_waves_before_io() {
        validate_v23_d3_request_capacity(8, 8).unwrap();
        assert!(validate_v23_d3_request_capacity(8, 7).is_err());
        assert!(validate_v23_d3_request_capacity(1, 0).is_err());
    }

    fn ranked_top_ten() -> V23RankedResult {
        V23RankedResult {
            ids: (0_u8..10).map(|value| vec![b'i', value]).collect(),
            distances: (0_u8..10).map(f32::from).collect(),
        }
    }

    fn serialized_test_quantizer(
        family: V23QuantizerFamily,
        code_width_bytes: u16,
        dimensions: usize,
    ) -> (serde_json::Value, String) {
        let sample_rows = if family == V23QuantizerFamily::SrhtPq {
            256
        } else {
            1
        };
        let sample = (0..sample_rows)
            .map(|row| {
                (0..dimensions)
                    .map(|dimension| (row * dimensions + dimension) as f32 / 4_096.0)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let quantizer =
            fit_v23_diagnostic_quantizer(family, code_width_bytes, dimensions, &sample).unwrap();
        let state = serde_json::to_value(quantizer.state()).unwrap();
        let checksum = blake3::hash(&serde_json::to_vec(&state).unwrap())
            .to_hex()
            .to_string();
        (state, checksum)
    }

    fn canonical_d1_report() -> V23D1Report {
        static QUANTIZER: std::sync::OnceLock<(serde_json::Value, String)> =
            std::sync::OnceLock::new();
        let (quantizer_state, quantizer_checksum) = QUANTIZER
            .get_or_init(|| serialized_test_quantizer(V23QuantizerFamily::SrhtPq, 64, 64))
            .clone();
        let query_samples = (0_u32..32)
            .map(|query_index| V23D1QuerySample {
                query_index,
                ground_truth_ids: ranked_top_ten().ids,
                oracle: ranked_top_ten(),
                scalar_oracle: ranked_top_ten(),
                routed: ranked_top_ten(),
                oracle_candidate_rows: 2_048,
                routed_candidate_rows: 8_192,
                wave_candidate_rows: 16_384,
                oracle_hits: 10,
                routed_hits: 10,
                cpu_ns: 1_000_000,
            })
            .collect();
        V23D1Report {
            schema: "borsuk-v23-d1-v5".to_string(),
            v20_root_checksum: "a".repeat(64),
            v20_codebook_checksum: "b".repeat(64),
            sample_ordinals_checksum: "c".repeat(64),
            query_vectors_checksum: "9".repeat(64),
            query_ordinals: (0_u64..32).collect(),
            rows: 9_990_000,
            dimensions: 64,
            routing_cell_count: 4_096,
            maximum_record_id_bytes: 32,
            arms: vec![V23D1Arm {
                key: V23D1ArmKey {
                    family: V23QuantizerFamily::SrhtPq,
                    code_width_bytes: 64,
                },
                quantizer_checksum,
                quantizer_state,
                query_samples,
                oracle_recall_ppm: 1_000_000,
                routed_recall_ppm: 1_000_000,
                scalar_simd_ids_equal: true,
                scalar_simd_max_distance_delta_ppm: 0,
                cpu_p99_ns: 1_000_000,
                wave_projected_bytes: 8 * (96 + 4 * 2_049 + 2_048 * (64 + 32)),
                passed: true,
            }],
        }
    }

    fn canonical_d2_report() -> V23D2Report {
        let query_samples = (0_u32..32)
            .map(|query_index| V23D2QuerySample {
                query_index,
                page_ordinals: vec![0],
                oracle_page_ordinals: vec![0],
                ground_truth_page_assignments: vec![vec![0]; 10],
                encoded_bytes: 120_000,
                candidate_rows: 1_000,
                selector_candidate_rows: 10_000,
                selector_routed_cells: V23_SELECTOR_ROUTING_CELLS as u16,
                selector_ranked_rows: 4_096,
                ground_truth_ids: ranked_top_ten().ids,
                ranked: ranked_top_ten(),
                gt_page_hits: 10,
                oracle_gt_page_hits: 10,
                hits: 10,
                recall_ppm: 1_000_000,
                cpu_ns: 1_000_000,
            })
            .collect();
        let mut report = V23D2Report {
            schema: "borsuk-v23-d2-v9".to_string(),
            d1_report_checksum: "e".repeat(64),
            query_ordinals: (0_u64..32).collect(),
            rows: 1_000,
            arms: vec![V23D2Arm {
                d1_key: V23D1ArmKey {
                    family: V23QuantizerFamily::F16Flat,
                    code_width_bytes: 192,
                },
                selector_key: V23D1ArmKey {
                    family: V23QuantizerFamily::SrhtPq,
                    code_width_bytes: 8,
                },
                selector: V23SelectorRef {
                    generation_checksum: [1; 32],
                    metric: VectorMetric::SquaredEuclidean,
                    dimensions: 96,
                    coarse_cells: 4_096,
                    page_count: 1,
                    maximum_assignments_per_row: 2,
                    code_width: 8,
                    row_count: 1_000,
                    path: format!("selectors/{}", "a".repeat(64)),
                    checksum: "a".repeat(64),
                    encoded_bytes: 96 + 4_096 * 96 * 4 + (4_096 + 1) * 4 + 1_000 * 16,
                },
                selector_routing_cells: V23_SELECTOR_ROUTING_CELLS as u16,
                selector_ranked_row_cap: V23_SELECTOR_RANKED_ROWS as u32,
                primary_target_rows: 384,
                maximum_assignments_per_row: 2,
                maximum_query_pages: 8,
                maximum_record_id_bytes: 32,
                pages: vec![V23PageRef {
                    generation_checksum: [1; 32],
                    page_ordinal: 0,
                    metric: VectorMetric::SquaredEuclidean,
                    dimensions: 96,
                    family: V23QuantizerFamily::F16Flat,
                    code_width: 192,
                    path: format!("pages/{}", "f".repeat(64)),
                    checksum: "f".repeat(64),
                    encoded_bytes: 120_000,
                    primary_rows: 1_000,
                    replicated_rows: 0,
                }],
                unique_rows: 1_000,
                total_assignments: 1_000,
                storage_amplification_ppm: 1_000_000,
                projected_root_bytes: 96 + 100_000 * 320,
                projected_ram_bytes: 96
                    + 100_000 * 320
                    + 96
                    + 4_096 * 96 * 4
                    + (4_096 + 1) * 4
                    + 100_000 * 16 * 204
                    + 4_096 * 96 * 4
                    + (4_096 + 1) * 4
                    + 512 * 1024 * 1024
                    + 2 * V23_WAVE_MAX_BYTES,
                projected_build_bytes: v23_d2_projected_build_memory(
                    1_000, 1, 120_000, 96, 32, 192, 2,
                )
                .unwrap(),
                query_samples,
                aggregate_recall_ppm: 1_000_000,
                minimum_query_recall_ppm: 1_000_000,
                coverage_oracle_recall_ppm: 1_000_000,
                coverage_oracle_minimum_query_recall_ppm: 1_000_000,
                selector_regret_ppm: 1_000_000,
                cpu_p99_ns: 1_000_000,
                passed: true,
            }],
        };
        let template = report.arms[0].clone();
        report.arms = V23_SELECTOR_CODE_WIDTHS
            .into_iter()
            .map(|code_width| {
                let mut arm = template.clone();
                arm.selector_key.code_width_bytes = code_width;
                arm.selector.code_width = code_width;
                arm.selector.encoded_bytes = 96
                    + 4_096 * 96 * 4
                    + (4_096 + 1) * 4
                    + arm.selector.row_count * (u64::from(code_width) + 8);
                let (_, projected_ram_bytes) = v23_d2_projected_memory(
                    arm.unique_rows,
                    arm.pages.len(),
                    96,
                    arm.selector.coarse_cells,
                    code_width,
                )
                .unwrap();
                arm.projected_ram_bytes = projected_ram_bytes;
                arm
            })
            .collect();
        report
    }

    #[test]
    fn v23_selector_rejects_dimensions_outside_the_registered_authority() {
        let input = V23SelectorInput {
            generation_checksum: [7; 32],
            metric: VectorMetric::SquaredEuclidean,
            dimensions: 95,
            page_count: 1,
            code_width: 8,
            maximum_assignments_per_row: 2,
            coarse_centroids: vec![vec![0.0; 95]],
            rows: vec![V23SelectorRow::new(0, 0, None, 0, &[0; 8])],
        };

        assert!(encode_v23_selector(&input).is_err());
    }

    #[test]
    fn v23_contract_rejects_a_ninth_page() {
        let sample = canonical_wave();
        validate_wave_sample(&sample).unwrap();

        let mut overflow = sample;
        overflow.page_ordinals.push(42);
        overflow.backing_gets = 9;
        assert!(validate_wave_sample(&overflow).is_err());
    }

    #[test]
    fn v23_contract_rejects_inconsistent_one_wave_accounting() {
        let canonical = canonical_wave();

        let mut unordered = canonical.clone();
        unordered.page_ordinals.swap(1, 2);
        assert!(validate_wave_sample(&unordered).is_err());

        let mut duplicate = canonical.clone();
        duplicate.page_ordinals[2] = duplicate.page_ordinals[1];
        assert!(validate_wave_sample(&duplicate).is_err());

        let mut no_candidates = canonical.clone();
        no_candidates.candidate_rows = 0;
        assert!(validate_wave_sample(&no_candidates).is_err());

        let mut no_bytes = canonical.clone();
        no_bytes.encoded_bytes = 0;
        no_bytes.backing_bytes = 0;
        assert!(validate_wave_sample(&no_bytes).is_err());

        let mut too_many_bytes = canonical.clone();
        too_many_bytes.encoded_bytes = V23_WAVE_MAX_BYTES + 1;
        too_many_bytes.backing_bytes = V23_WAVE_MAX_BYTES + 1;
        assert!(validate_wave_sample(&too_many_bytes).is_err());

        let mut gets_differ = canonical.clone();
        gets_differ.backing_gets -= 1;
        assert!(validate_wave_sample(&gets_differ).is_err());

        let mut no_get_capacity = canonical.clone();
        no_get_capacity.backing_get_concurrency = 0;
        assert!(validate_wave_sample(&no_get_capacity).is_err());

        let mut insufficient_get_capacity = canonical.clone();
        insufficient_get_capacity.backing_get_concurrency = 3;
        assert!(validate_wave_sample(&insufficient_get_capacity).is_err());

        let mut backing_bytes_differ = canonical.clone();
        backing_bytes_differ.backing_bytes -= 1;
        assert!(validate_wave_sample(&backing_bytes_differ).is_err());

        let mut impossible_backing_service = canonical.clone();
        impossible_backing_service.backing_service_us_max =
            impossible_backing_service.backing_service_us_sum + 1;
        assert!(validate_wave_sample(&impossible_backing_service).is_err());

        let mut backing_outlives_query = canonical.clone();
        backing_outlives_query.backing_service_us_max = 38_000;
        assert!(validate_wave_sample(&backing_outlives_query).is_err());

        let mut no_cpu = canonical.clone();
        no_cpu.cpu_ns = 0;
        assert!(validate_wave_sample(&no_cpu).is_err());

        let mut inconsistent_latency = canonical.clone();
        inconsistent_latency.service_ns -= 1;
        assert!(validate_wave_sample(&inconsistent_latency).is_err());

        let mut impossible_admission = canonical.clone();
        impossible_admission.request_admission_wait_ns = impossible_admission.elapsed_ns;
        assert!(validate_wave_sample(&impossible_admission).is_err());

        let mut no_elapsed = canonical;
        no_elapsed.elapsed_ns = 0;
        assert!(validate_wave_sample(&no_elapsed).is_err());
    }

    #[test]
    fn v23_d1_contract_recomputes_gates_and_rejects_family_invalid_widths() {
        let canonical = canonical_d1_report();
        validate_d1_report(&canonical).unwrap();

        let mut wide = canonical.clone();
        wide.arms[0].key.code_width_bytes = 65;
        assert!(validate_d1_report(&wide).is_err());

        let mut dimension_drift = canonical.clone();
        dimension_drift.dimensions += 1;
        assert!(validate_d1_report(&dimension_drift).is_err());

        let mut aggregate_drift = canonical.clone();
        aggregate_drift.arms[0].routed_recall_ppm -= 1;
        assert!(validate_d1_report(&aggregate_drift).is_err());

        let mut non_finite = canonical.clone();
        non_finite.arms[0].query_samples[0].routed.distances[0] = f32::NAN;
        assert!(validate_d1_report(&non_finite).is_err());

        let mut projection_drift = canonical.clone();
        projection_drift.arms[0].wave_projected_bytes += 1;
        assert!(validate_d1_report(&projection_drift).is_err());

        let mut capacity_adjusted_projection = canonical.clone();
        capacity_adjusted_projection.maximum_record_id_bytes = 64;
        capacity_adjusted_projection.arms[0].wave_projected_bytes =
            8 * v23_d1_projected_page_bytes(64, 64);
        for sample in &mut capacity_adjusted_projection.arms[0].query_samples {
            sample.wave_candidate_rows = 8 * v23_d1_projected_page_rows(64, 64);
        }
        validate_d1_report(&capacity_adjusted_projection).unwrap();
        capacity_adjusted_projection.arms[0].wave_projected_bytes += 1;
        assert!(validate_d1_report(&capacity_adjusted_projection).is_err());

        let mut insufficient_wave_capacity = canonical.clone();
        insufficient_wave_capacity.maximum_record_id_bytes = 5_000;
        insufficient_wave_capacity.arms[0].wave_projected_bytes =
            8 * v23_d1_projected_page_bytes(64, 5_000);
        for sample in &mut insufficient_wave_capacity.arms[0].query_samples {
            sample.wave_candidate_rows = 8 * v23_d1_projected_page_rows(64, 5_000);
        }
        assert!(insufficient_wave_capacity.arms[0].query_samples[0].wave_candidate_rows < 2_048);
        assert!(validate_d1_report(&insufficient_wave_capacity).is_err());

        let mut duplicate_source_query = canonical.clone();
        duplicate_source_query.query_ordinals[31] = 30;
        assert!(validate_d1_report(&duplicate_source_query).is_err());

        let mut scalar_identity_drift = canonical.clone();
        scalar_identity_drift.arms[0].query_samples[0]
            .scalar_oracle
            .ids[0] = vec![b'z'];
        assert!(validate_d1_report(&scalar_identity_drift).is_err());

        let mut timed_wave_drift = canonical.clone();
        timed_wave_drift.arms[0].query_samples[0].wave_candidate_rows -= 1;
        assert!(validate_d1_report(&timed_wave_drift).is_err());

        let mut noncanonical_queries = canonical;
        noncanonical_queries.arms[0].query_samples[31].query_index = 30;
        assert!(validate_d1_report(&noncanonical_queries).is_err());
    }

    #[test]
    fn v23_d1_contract_accepts_native_fast_turboquant_width() {
        let mut report = canonical_d1_report();
        report.arms[0].key = V23D1ArmKey {
            family: V23QuantizerFamily::FastTurboQuantMse,
            code_width_bytes: 52,
        };
        let (state, checksum) =
            serialized_test_quantizer(V23QuantizerFamily::FastTurboQuantMse, 52, 128);
        report.dimensions = 128;
        report.arms[0].quantizer_state = state;
        report.arms[0].quantizer_checksum = checksum;
        report.arms[0].wave_projected_bytes = 8 * (96 + 4 * 2_049 + 2_048 * (52 + 32));
        validate_d1_report(&report).unwrap();
    }

    #[test]
    fn v23_d1_f16_arm_is_skipped_when_one_wave_is_unavailable() {
        let f16_1024d = V23D1ArmKey {
            family: V23QuantizerFamily::F16Flat,
            code_width_bytes: 2_048,
        };
        assert!(!super::v23_d1_arm_is_eligible_for_wave(f16_1024d, 16));
        let f16_96d = V23D1ArmKey {
            family: V23QuantizerFamily::F16Flat,
            code_width_bytes: 192,
        };
        assert!(super::v23_d1_arm_is_eligible_for_wave(f16_96d, 16));
        let compact_with_wide_ids = V23D1ArmKey {
            family: V23QuantizerFamily::SrhtPq,
            code_width_bytes: 64,
        };
        assert!(!super::v23_d1_arm_is_eligible_for_wave(
            compact_with_wide_ids,
            5_000
        ));
    }

    #[test]
    fn v23_d1_contract_accepts_tolerance_bound_boundary_rank_drift() {
        let mut report = canonical_d1_report();
        let sample = &mut report.arms[0].query_samples[0];
        sample.scalar_oracle.ids[9] = vec![b'i', 10];
        sample.scalar_oracle.distances[9] = 9.000_001;
        report.arms[0].scalar_simd_ids_equal = false;
        report.arms[0].scalar_simd_max_distance_delta_ppm = 1;
        report.arms[0].passed = true;
        validate_d1_report(&report).unwrap();
    }

    #[test]
    fn v23_d1_persists_restorable_quantizer_state_and_rejects_mutation() {
        let sample = (0..256)
            .map(|row| {
                (0..8)
                    .map(|dimension| (row * 8 + dimension) as f32 / 255.0)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let quantizer =
            fit_v23_diagnostic_quantizer(V23QuantizerFamily::SrhtPq, 8, 8, &sample).unwrap();
        let state = serde_json::to_value(quantizer.state()).unwrap();
        let mut report = canonical_d1_report();
        report.dimensions = 8;
        report.arms[0].key = V23D1ArmKey {
            family: V23QuantizerFamily::SrhtPq,
            code_width_bytes: 8,
        };
        report.arms[0].quantizer_checksum = blake3::hash(&serde_json::to_vec(&state).unwrap())
            .to_hex()
            .to_string();
        report.arms[0].quantizer_state = state;
        report.arms[0].wave_projected_bytes = 8 * (96 + 4 * 2_049 + 2_048 * (8 + 32));
        validate_d1_report(&report).unwrap();

        let restored = restore_v23_diagnostic_quantizer(&report.arms[0]).unwrap();
        let query = &sample[7];
        let encoded = quantizer.encode(&sample[13]).unwrap();
        let expected = quantizer
            .score_prepared_contiguous_codes(
                &quantizer.prepare_contiguous_query(query).unwrap(),
                &encoded,
            )
            .unwrap();
        let observed = restored
            .score_prepared_contiguous_codes(
                &restored.prepare_contiguous_query(query).unwrap(),
                &encoded,
            )
            .unwrap();
        assert_eq!(observed, expected);

        report.arms[0]
            .quantizer_state
            .as_object_mut()
            .unwrap()
            .insert("mutated".to_string(), serde_json::Value::Bool(true));
        assert!(validate_d1_report(&report).is_err());
    }

    #[test]
    fn v23_d1_quantizer_checksum_normalizes_equivalent_f32_json_numbers() {
        let mut report = canonical_d1_report();
        let value = &mut report.arms[0].quantizer_state["state"]["codebooks"][0][0];
        let original = value.as_f64().unwrap();
        let adjacent = f64::from_bits(original.to_bits() + 1);
        assert_ne!(original, adjacent);
        assert_eq!(original as f32, adjacent as f32);
        *value = serde_json::Value::Number(serde_json::Number::from_f64(adjacent).unwrap());

        restore_v23_diagnostic_quantizer(&report.arms[0]).unwrap();
        validate_d1_report(&report).unwrap();
    }

    #[test]
    fn v23_d2_lineage_checksum_normalizes_equivalent_d1_f32_json_numbers() {
        let report = canonical_d1_report();
        let mut equivalent = report.clone();
        let value = &mut equivalent.arms[0].quantizer_state["state"]["codebooks"][0][0];
        let original = value.as_f64().unwrap();
        let adjacent = f64::from_bits(original.to_bits() + 1);
        assert_ne!(original, adjacent);
        assert_eq!(original as f32, adjacent as f32);
        *value = serde_json::Value::Number(serde_json::Number::from_f64(adjacent).unwrap());

        assert_eq!(
            v23_d1_report_checksum(&report).unwrap(),
            v23_d1_report_checksum(&equivalent).unwrap()
        );
    }

    #[test]
    fn v23_quantizer_state_rejects_nested_unknown_fields_before_checksum() {
        let mut report = canonical_d1_report();
        report.arms[0].quantizer_state["state"]["unexpected"] = serde_json::json!(true);

        assert!(restore_v23_diagnostic_quantizer(&report.arms[0]).is_err());
        assert!(v23_d1_report_checksum(&report).is_err());
    }

    #[test]
    fn v23_diagnostic_reports_reject_pre_state_authority_schemas() {
        let mut d1 = canonical_d1_report();
        d1.schema = "borsuk-v23-d1-v2".to_string();
        assert!(validate_d1_report(&d1).is_err());

        let mut d2 = canonical_d2_report();
        d2.schema = "borsuk-v23-d2-v2".to_string();
        assert!(validate_d2_report(&d2).is_err());
    }

    #[test]
    fn v23_d2_contract_enforces_page_memory_recall_and_amplification_gates() {
        let canonical = canonical_d2_report();
        validate_d2_report(&canonical).unwrap();
        let serialized = serde_json::to_value(&canonical).unwrap();
        let serialized_arm = &serialized["arms"][0];
        assert!(serialized_arm.get("selector").is_some());
        assert!(serialized_arm.get("projected_root_bytes").is_some());
        assert!(serialized_arm.get("root_bytes").is_none());
        assert!(serialized_arm.get("projected_build_bytes").is_some());
        assert!(serialized_arm.get("build_peak_rss_bytes").is_none());
        let projected_pages = 100_000_u64;
        let projected_root_bytes = 96 + projected_pages * 320;
        let projected_selector_bytes = 96
            + 4_096 * 96 * 4
            + (4_096 + 1) * 4
            + 100_000_000 * (u64::from(V23_SELECTOR_CODE_WIDTHS[0]) + 8);
        let projected_decoded_selector_bytes = 4_096 * 96 * 4 + (4_096 + 1) * 4;
        let projected_ram_bytes = projected_root_bytes
            + projected_selector_bytes
            + projected_decoded_selector_bytes
            + 512 * 1024 * 1024
            + 2 * V23_WAVE_MAX_BYTES;
        assert_eq!(
            serialized_arm["projected_root_bytes"].as_u64(),
            Some(projected_root_bytes)
        );
        assert_eq!(
            serialized_arm["projected_ram_bytes"].as_u64(),
            Some(projected_ram_bytes)
        );

        let sparse_projection =
            v23_d2_projected_memory(1_000, 1, 4, 1, V23_SELECTOR_CODE_WIDTHS[0]).unwrap();
        let production_projection =
            v23_d2_projected_memory(1_000, 1, 4, 4_096, V23_SELECTOR_CODE_WIDTHS[0]).unwrap();
        assert_eq!(sparse_projection, production_projection);

        let mut query_authority_drift = canonical.clone();
        query_authority_drift.query_ordinals[0] += 1;
        assert!(validate_d2_report(&query_authority_drift).is_err());

        let mut too_many_arms = canonical.clone();
        too_many_arms.arms = [(512_u16, 1_u8), (512, 2), (512, 3), (1_024, 1)]
            .into_iter()
            .map(|(target, assignments)| {
                let mut arm = canonical.arms[0].clone();
                arm.primary_target_rows = target;
                arm.maximum_assignments_per_row = assignments;
                arm
            })
            .collect();
        assert!(validate_d2_report(&too_many_arms).is_err());

        let mut dominated_arm = canonical.clone();
        let mut slower = canonical.arms[0].clone();
        slower.primary_target_rows = 2_048;
        slower.projected_ram_bytes += 1;
        slower
            .query_samples
            .iter_mut()
            .for_each(|sample| sample.cpu_ns += 1);
        slower.cpu_p99_ns += 1;
        dominated_arm.arms.push(slower);
        assert!(validate_d2_report(&dominated_arm).is_err());

        let mut five_pages = canonical.clone();
        five_pages.arms[0].pages = (0_u32..5)
            .map(|page_ordinal| V23PageRef {
                generation_checksum: [1; 32],
                page_ordinal,
                metric: VectorMetric::SquaredEuclidean,
                dimensions: 4,
                family: V23QuantizerFamily::SrhtPq,
                code_width: 64,
                path: format!("pages/{}", "f".repeat(64)),
                checksum: "f".repeat(64),
                encoded_bytes: 1_000,
                primary_rows: 200,
                replicated_rows: 0,
            })
            .collect();
        five_pages.arms[0]
            .query_samples
            .iter_mut()
            .for_each(|sample| {
                sample.page_ordinals = (0_u32..5).collect();
                sample.encoded_bytes = 5_000;
                sample.candidate_rows = 1_000;
            });
        assert!(validate_d2_report(&five_pages).is_err());

        let mut oversized_page = canonical.clone();
        oversized_page.arms[0].pages[0].encoded_bytes = 245_761;
        oversized_page.arms[0].query_samples[0].encoded_bytes = 245_761;
        assert!(validate_d2_report(&oversized_page).is_err());

        let mut amplification_drift = canonical.clone();
        amplification_drift.arms[0].storage_amplification_ppm = 1_000_001;
        assert!(validate_d2_report(&amplification_drift).is_err());

        let mut routed_cell_drift = canonical.clone();
        routed_cell_drift.arms[0].query_samples[0].selector_routed_cells -= 1;
        assert!(validate_d2_report(&routed_cell_drift).is_err());

        let mut selector_ref_drift = canonical.clone();
        selector_ref_drift.arms[0].selector.row_count -= 1;
        assert!(validate_d2_report(&selector_ref_drift).is_err());

        let mut selector_width_identity_drift = canonical.clone();
        let arm = &mut selector_width_identity_drift.arms[0];
        arm.selector.code_width = V23_SELECTOR_CODE_WIDTHS[1];
        arm.selector.encoded_bytes = 96
            + u64::from(arm.selector.coarse_cells) * u64::from(arm.selector.dimensions) * 4
            + (u64::from(arm.selector.coarse_cells) + 1) * 4
            + arm.selector.row_count * (u64::from(arm.selector.code_width) + 8);
        let (_, projected_ram_bytes) = v23_d2_projected_memory(
            arm.unique_rows,
            arm.pages.len(),
            usize::try_from(arm.selector.dimensions).unwrap(),
            arm.selector.coarse_cells,
            arm.selector.code_width,
        )
        .unwrap();
        arm.projected_ram_bytes = projected_ram_bytes;
        assert!(validate_d2_report(&selector_width_identity_drift).is_err());

        let mut oracle_assignment_drift = canonical.clone();
        oracle_assignment_drift.arms[0].query_samples[0].ground_truth_page_assignments[0].clear();
        assert!(validate_d2_report(&oracle_assignment_drift).is_err());

        let mut ram_overflow = canonical.clone();
        ram_overflow.arms[0].selector.coarse_cells = 5_000_000;
        ram_overflow.arms[0].selector.encoded_bytes = 96
            + u64::from(ram_overflow.arms[0].selector.coarse_cells) * 96 * 4
            + (u64::from(ram_overflow.arms[0].selector.coarse_cells) + 1) * 4
            + ram_overflow.arms[0].selector.row_count
                * (u64::from(ram_overflow.arms[0].selector.code_width) + 8);
        let (projected_root_bytes, projected_ram_bytes) = v23_d2_projected_memory(
            ram_overflow.arms[0].unique_rows,
            ram_overflow.arms[0].pages.len(),
            96,
            ram_overflow.arms[0].selector.coarse_cells,
            ram_overflow.arms[0].selector.code_width,
        )
        .unwrap();
        assert!(projected_ram_bytes > 3 * 1024 * 1024 * 1024);
        ram_overflow.arms[0].projected_root_bytes = projected_root_bytes;
        ram_overflow.arms[0].projected_ram_bytes = projected_ram_bytes;
        ram_overflow.arms[0].projected_build_bytes = v23_d2_projected_build_memory(
            ram_overflow.arms[0].unique_rows,
            ram_overflow.arms[0].pages.len(),
            ram_overflow.arms[0]
                .pages
                .iter()
                .map(|page| page.encoded_bytes)
                .sum(),
            96,
            ram_overflow.arms[0].maximum_record_id_bytes,
            ram_overflow.arms[0].d1_key.code_width_bytes,
            ram_overflow.arms[0].maximum_assignments_per_row,
        )
        .unwrap();
        let projected_build_peak = ram_overflow.arms[0].projected_build_bytes;
        ram_overflow
            .arms
            .iter_mut()
            .for_each(|arm| arm.projected_build_bytes = projected_build_peak);
        ram_overflow.arms[0].passed = false;
        validate_d2_report(&ram_overflow).unwrap();
        ram_overflow.arms[0].passed = true;
        assert!(validate_d2_report(&ram_overflow).is_err());

        let mut low_tail_recall = canonical;
        low_tail_recall.arms[0].query_samples[0].ranked.ids[7..].fill(vec![b'x']);
        low_tail_recall.arms[0].query_samples[0].ranked.ids[8] = vec![b'y'];
        low_tail_recall.arms[0].query_samples[0].ranked.ids[9] = vec![b'z'];
        low_tail_recall.arms[0].query_samples[0].hits = 7;
        low_tail_recall.arms[0].query_samples[0].recall_ppm = 700_000;
        low_tail_recall.arms[0].aggregate_recall_ppm = 990_625;
        low_tail_recall.arms[0].minimum_query_recall_ppm = 700_000;
        low_tail_recall.arms[0].passed = false;
        validate_d2_report(&low_tail_recall).unwrap();
        low_tail_recall.arms[0].passed = true;
        assert!(validate_d2_report(&low_tail_recall).is_err());
    }

    #[test]
    fn v23_d2_within_gate_cpu_jitter_preserves_the_registered_width_matrix() {
        let mut report = canonical_d2_report();
        report.arms[0]
            .query_samples
            .iter_mut()
            .for_each(|sample| sample.cpu_ns += 1);
        report.arms[0].cpu_p99_ns += 1;
        validate_d2_report(&report).unwrap();

        report.arms[0].primary_target_rows = 640;
        assert!(validate_d2_report(&report).is_err());
    }

    #[test]
    fn v23_d2_builder_projection_covers_replica_and_index_transients() {
        assert_eq!(std::mem::size_of::<V23PlanningRow>(), 64);
        assert_eq!(std::mem::size_of::<V23ReplicaCandidate>(), 32);
        assert_eq!(std::mem::size_of::<usize>(), 8);
        let rows = 10_000_000_u64;
        let dimensions = 96_usize;
        let maximum_record_id_bytes = 32_u16;
        let projected = v23_d2_projected_build_memory(
            rows,
            5_000,
            rows * 100,
            dimensions,
            maximum_record_id_bytes,
            64,
            3,
        )
        .unwrap();
        let decoded_rows = rows
            * (std::mem::size_of::<V23PlanningRow>() as u64
                + u64::from(maximum_record_id_bytes)
                + dimensions as u64 * 4
                + 64);
        let replica_candidates = rows * std::mem::size_of::<V23ReplicaCandidate>() as u64;
        let selector_rows = rows * (u64::from(V23_SELECTOR_CODE_WIDTHS[1]) + 32);
        let index_vectors = rows * 7 * std::mem::size_of::<usize>() as u64;
        assert!(projected >= decoded_rows + selector_rows + replica_candidates + index_vectors);
    }

    #[test]
    fn v23_d2_sparse_page_records_partial_ranked_evidence_instead_of_aborting() {
        let mut report = canonical_d2_report();
        for sample in &mut report.arms[0].query_samples {
            sample.ranked.ids.truncate(5);
            sample.ranked.distances.truncate(5);
            sample.hits = 5;
            sample.recall_ppm = 500_000;
        }
        report.arms[0].aggregate_recall_ppm = 500_000;
        report.arms[0].minimum_query_recall_ppm = 500_000;
        report.arms[0].passed = false;
        validate_d2_report(&report).unwrap();
    }

    #[test]
    fn v23_d2_binds_query_vector_bits_and_ground_truth_to_d1() {
        let ordinals = (0_u64..32).collect::<Vec<_>>();
        let queries = ordinals
            .iter()
            .map(|ordinal| vec![*ordinal as f32, 1.0, 2.0, 3.0])
            .collect::<Vec<_>>();
        let mut d1 = canonical_d1_report();
        d1.query_vectors_checksum = super::v23_query_vectors_checksum(&ordinals, &queries).unwrap();
        let selected = &d1.arms[0];
        let ground_truth = selected
            .query_samples
            .iter()
            .map(|sample| sample.ground_truth_ids.clone())
            .collect::<Vec<_>>();
        validate_v23_d2_query_binding(&d1, selected, &ordinals, &queries, &ground_truth).unwrap();

        let mut changed_queries = queries.clone();
        changed_queries[0][0] = 99.0;
        assert!(
            validate_v23_d2_query_binding(
                &d1,
                selected,
                &ordinals,
                &changed_queries,
                &ground_truth,
            )
            .is_err()
        );
        let mut changed_truth = ground_truth;
        changed_truth[0][0].push(b'x');
        assert!(
            validate_v23_d2_query_binding(&d1, selected, &ordinals, &queries, &changed_truth,)
                .is_err()
        );
    }

    #[test]
    fn v23_d2_report_rejects_unbounded_metric_and_impossible_gt_coverage() {
        let canonical = canonical_d2_report();

        let mut unbounded_metric = canonical.clone();
        unbounded_metric.arms[0].pages[0].metric = VectorMetric::InnerProduct;
        assert!(validate_d2_report(&unbounded_metric).is_err());

        let mut impossible_coverage = canonical;
        impossible_coverage.arms[0].query_samples[0].gt_page_hits = 9;
        assert!(validate_d2_report(&impossible_coverage).is_err());

        let mut invalid_page_budget = canonical_d2_report();
        invalid_page_budget.arms[0].maximum_query_pages = 0;
        assert!(validate_d2_report(&invalid_page_budget).is_err());

        let mut mixed_gate_states = canonical_d2_report();
        let mut failing = mixed_gate_states.arms[0].clone();
        failing.primary_target_rows = 2_048;
        failing.passed = false;
        mixed_gate_states.arms.push(failing);
        assert!(validate_d2_report(&mixed_gate_states).is_err());
    }

    #[test]
    fn v23_d2_short_query_prefix_returns_an_error() {
        let prefixes = (0..V23_DIAGNOSTIC_QUERIES)
            .map(|query_index| V22StageLQueryPrefix {
                query_index,
                rows: Vec::new(),
            })
            .collect::<Vec<_>>();
        assert!(validate_v23_d2_query_prefixes(&prefixes).is_err());
    }

    #[test]
    fn v23_page_reference_uses_protocol_metric_spelling() {
        let page = V23PageRef {
            generation_checksum: [7; 32],
            page_ordinal: 0,
            metric: VectorMetric::SquaredEuclidean,
            dimensions: 1,
            family: V23QuantizerFamily::F16Flat,
            code_width: 2,
            path: format!("pages/{}", "a".repeat(64)),
            checksum: "a".repeat(64),
            encoded_bytes: 100,
            primary_rows: 1,
            replicated_rows: 0,
        };

        let value = serde_json::to_value(&page).unwrap();
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../scripts/fixtures/v23_page_ref.json"))
                .unwrap();
        assert_eq!(value, fixture);
        let decoded: V23PageRef = serde_json::from_value(fixture).unwrap();
        assert_eq!(decoded, page);

        for (metric, expected) in [
            (VectorMetric::Euclidean, "euclidean"),
            (VectorMetric::SquaredEuclidean, "squared-euclidean"),
            (VectorMetric::Cosine, "cosine"),
        ] {
            let mut candidate = page.clone();
            candidate.metric = metric.clone();
            let serialized = serde_json::to_value(candidate).unwrap();
            assert_eq!(serialized["metric"], serde_json::json!(expected));
            assert_eq!(
                serde_json::from_value::<V23PageRef>(serialized)
                    .unwrap()
                    .metric,
                metric
            );
        }

        for invalid in [
            "SquaredEuclidean",
            "sqeuclidean",
            "l2",
            "SQUARED-EUCLIDEAN",
            " cosine ",
            "inner-product",
        ] {
            let mut malformed = value.clone();
            malformed["metric"] = serde_json::json!(invalid);
            assert!(serde_json::from_value::<V23PageRef>(malformed).is_err());
        }
    }

    #[test]
    fn v23_page_codec_round_trips_canonical_bytes_and_rejects_mutations() {
        let input = V23PageInput {
            generation_checksum: [7; 32],
            page_ordinal: 3,
            metric: VectorMetric::Cosine,
            dimensions: 4,
            family: V23QuantizerFamily::SrhtPq,
            code_width: 8,
            primary_rows: vec![
                V23PageRow {
                    canonical_record_id: b"a".to_vec().into_boxed_slice(),
                    code: vec![1; 8].into_boxed_slice(),
                },
                V23PageRow {
                    canonical_record_id: b"bbb".to_vec().into_boxed_slice(),
                    code: vec![3; 8].into_boxed_slice(),
                },
            ],
            replicated_rows: vec![V23PageRow {
                canonical_record_id: b"cc".to_vec().into_boxed_slice(),
                code: vec![5; 8].into_boxed_slice(),
            }],
        };
        let first = encode_v23_page(&input).unwrap();
        let second = encode_v23_page(&input).unwrap();
        assert_eq!(first, second);
        let page_ref = V23PageRef {
            generation_checksum: input.generation_checksum,
            page_ordinal: input.page_ordinal,
            metric: input.metric.clone(),
            dimensions: input.dimensions,
            family: input.family,
            code_width: input.code_width,
            path: format!("pages/{}", blake3::hash(&first).to_hex()),
            checksum: blake3::hash(&first).to_hex().to_string(),
            encoded_bytes: first.len() as u64,
            primary_rows: input.primary_rows.len() as u32,
            replicated_rows: input.replicated_rows.len() as u32,
        };
        let decoded = decode_v23_page(first.clone(), &page_ref).unwrap();
        assert_eq!(decoded.primary_rows(), 2);
        assert_eq!(decoded.replicated_rows(), 1);
        assert_eq!(decoded.record_id(0), Some(b"a".as_slice()));
        assert_eq!(decoded.record_id(1), Some(b"bbb".as_slice()));
        assert_eq!(decoded.record_id(2), Some(b"cc".as_slice()));
        assert_eq!(decoded.code(0), Some([1; 8].as_slice()));
        assert_eq!(decoded.code(1), Some([3; 8].as_slice()));
        assert_eq!(decoded.code(2), Some([5; 8].as_slice()));

        let mut bad_reference = page_ref.clone();
        bad_reference.checksum = "f".repeat(64);
        assert!(decode_v23_page(first.clone(), &bad_reference).is_err());

        let mut bad_magic = first.clone().to_vec();
        bad_magic[0] ^= 1;
        let bad_magic = bytes::Bytes::from(bad_magic);
        let mut matching_hash = page_ref.clone();
        matching_hash.checksum = blake3::hash(&bad_magic).to_hex().to_string();
        matching_hash.path = format!("pages/{}", matching_hash.checksum);
        assert!(decode_v23_page(bad_magic, &matching_hash).is_err());

        let mut bad_metric = first.clone().to_vec();
        bad_metric[5] = 1;
        let bad_metric = bytes::Bytes::from(bad_metric);
        let mut matching_hash = page_ref.clone();
        matching_hash.checksum = blake3::hash(&bad_metric).to_hex().to_string();
        matching_hash.path = format!("pages/{}", matching_hash.checksum);
        assert!(decode_v23_page(bad_metric, &matching_hash).is_err());

        let mut bad_offset = first.to_vec();
        bad_offset[100..104].copy_from_slice(&0_u32.to_le_bytes());
        let bad_offset = bytes::Bytes::from(bad_offset);
        let mut matching_hash = page_ref.clone();
        matching_hash.checksum = blake3::hash(&bad_offset).to_hex().to_string();
        matching_hash.path = format!("pages/{}", matching_hash.checksum);
        assert!(decode_v23_page(bad_offset, &matching_hash).is_err());

        let matching_reference = |candidate: &bytes::Bytes| {
            let mut reference = page_ref.clone();
            reference.checksum = blake3::hash(candidate).to_hex().to_string();
            reference.path = format!("pages/{}", reference.checksum);
            reference
        };
        for mutation in [
            (4_usize, 1_u8),
            (6, 2),
            (7, 16),
            (8, 5),
            (12, 4),
            (16, 1),
            (20, 0),
            (24, 1),
            (28, 1),
            (32, 9),
            (64, 0),
            (65, 1),
        ] {
            let mut candidate = first.clone().to_vec();
            candidate[mutation.0] = mutation.1;
            let candidate = bytes::Bytes::from(candidate);
            assert!(
                decode_v23_page(candidate.clone(), &matching_reference(&candidate)).is_err(),
                "accepted mutated header byte {}",
                mutation.0
            );
        }
        let mut bad_reserved_header = first.clone().to_vec();
        bad_reserved_header[66] ^= 1;
        let bad_reserved_header = bytes::Bytes::from(bad_reserved_header);
        assert!(
            decode_v23_page(
                bad_reserved_header.clone(),
                &matching_reference(&bad_reserved_header)
            )
            .is_err()
        );

        let mut bad_id_order = first.clone().to_vec();
        bad_id_order[112] = b'b';
        bad_id_order[113..116].fill(b'a');
        let bad_id_order = bytes::Bytes::from(bad_id_order);
        assert!(decode_v23_page(bad_id_order.clone(), &matching_reference(&bad_id_order)).is_err());

        let mut reference_mutations = Vec::new();
        let mut mutation = page_ref.clone();
        mutation.generation_checksum[0] ^= 1;
        reference_mutations.push(mutation);
        let mut mutation = page_ref.clone();
        mutation.page_ordinal += 1;
        reference_mutations.push(mutation);
        let mut mutation = page_ref.clone();
        mutation.metric = VectorMetric::Euclidean;
        reference_mutations.push(mutation);
        let mut mutation = page_ref.clone();
        mutation.dimensions += 1;
        reference_mutations.push(mutation);
        let mut mutation = page_ref.clone();
        mutation.family = V23QuantizerFamily::FastTurboQuantMse;
        reference_mutations.push(mutation);
        let mut mutation = page_ref.clone();
        mutation.code_width = 16;
        reference_mutations.push(mutation);
        let mut mutation = page_ref.clone();
        mutation.primary_rows -= 1;
        reference_mutations.push(mutation);
        let mut mutation = page_ref.clone();
        mutation.replicated_rows -= 1;
        reference_mutations.push(mutation);
        let mut mutation = page_ref;
        mutation.encoded_bytes -= 1;
        reference_mutations.push(mutation);
        for mutation in reference_mutations {
            assert!(decode_v23_page(first.clone(), &mutation).is_err());
        }
    }

    #[test]
    fn v23_d2_streams_authenticated_materialized_pages_without_retaining_bytes() {
        let input = V23PageInput {
            generation_checksum: [11; 32],
            page_ordinal: 0,
            metric: VectorMetric::SquaredEuclidean,
            dimensions: 2,
            family: V23QuantizerFamily::SrhtPq,
            code_width: 8,
            primary_rows: vec![V23PageRow {
                canonical_record_id: b"row".to_vec().into_boxed_slice(),
                code: vec![7; 8].into_boxed_slice(),
            }],
            replicated_rows: Vec::new(),
        };
        let bytes = encode_v23_page(&input).unwrap();
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let page = V23PageRef {
            generation_checksum: input.generation_checksum,
            page_ordinal: input.page_ordinal,
            metric: input.metric,
            dimensions: input.dimensions,
            family: input.family,
            code_width: input.code_width,
            path: format!("pages/{checksum}"),
            checksum: checksum.clone(),
            encoded_bytes: bytes.len() as u64,
            primary_rows: 1,
            replicated_rows: 0,
        };
        let mut observed = Vec::new();
        stream_v23_materialized_pages(
            std::slice::from_ref(&page),
            std::slice::from_ref(&bytes),
            &mut |reference, body| {
                observed.push((reference.path.clone(), body.clone()));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(observed, vec![(format!("pages/{checksum}"), bytes.clone())]);

        let mut sink_calls = 0_u8;
        let sink_error = stream_v23_materialized_pages(
            std::slice::from_ref(&page),
            std::slice::from_ref(&bytes),
            &mut |_, _| {
                sink_calls += 1;
                Err(crate::BorsukError::InvalidStorage(
                    "injected sink failure".to_string(),
                ))
            },
        )
        .unwrap_err();
        assert_eq!(sink_calls, 1);
        assert!(sink_error.to_string().contains("injected sink failure"));

        let mut mismatched = page;
        mismatched.checksum = "f".repeat(64);
        assert!(
            stream_v23_materialized_pages(&[mismatched], &[bytes], &mut |_, _| panic!(
                "invalid page reached sink"
            ),)
            .is_err()
        );
    }

    #[test]
    fn v23_d3_executes_one_cache_disabled_backing_wave_under_one_byte_permit() {
        let dimensions = 96_usize;
        let training = (0..256)
            .map(|row| {
                (0..dimensions)
                    .map(|dimension| (row * dimensions + dimension) as f32 / 2_048.0)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let quantizer =
            fit_v23_diagnostic_quantizer(V23QuantizerFamily::SrhtPq, 8, dimensions, &training)
                .unwrap();
        let state = serde_json::to_value(quantizer.state()).unwrap();
        let d1_arm = V23D1Arm {
            key: V23D1ArmKey {
                family: V23QuantizerFamily::SrhtPq,
                code_width_bytes: 8,
            },
            quantizer_checksum: blake3::hash(&serde_json::to_vec(&state).unwrap())
                .to_hex()
                .to_string(),
            quantizer_state: state,
            query_samples: Vec::new(),
            oracle_recall_ppm: 1_000_000,
            routed_recall_ppm: 1_000_000,
            scalar_simd_ids_equal: true,
            scalar_simd_max_distance_delta_ppm: 0,
            cpu_p99_ns: 1,
            wave_projected_bytes: 1,
            passed: true,
        };
        let rows = [training[7].clone(), training[19].clone()];
        let page_input = V23PageInput {
            generation_checksum: [23; 32],
            page_ordinal: 0,
            metric: VectorMetric::SquaredEuclidean,
            dimensions: dimensions as u32,
            family: d1_arm.key.family,
            code_width: d1_arm.key.code_width_bytes,
            primary_rows: rows
                .iter()
                .enumerate()
                .map(|(index, row)| V23PageRow {
                    canonical_record_id: format!("row-{index}").into_bytes().into_boxed_slice(),
                    code: quantizer.encode(row).unwrap().into_boxed_slice(),
                })
                .collect(),
            replicated_rows: Vec::new(),
        };
        let page_bytes = encode_v23_page(&page_input).unwrap();
        let checksum = blake3::hash(&page_bytes).to_hex().to_string();
        let page_ref = V23PageRef {
            generation_checksum: page_input.generation_checksum,
            page_ordinal: 0,
            metric: page_input.metric.clone(),
            dimensions: page_input.dimensions,
            family: page_input.family,
            code_width: page_input.code_width,
            path: format!("pages/{checksum}"),
            checksum,
            encoded_bytes: page_bytes.len() as u64,
            primary_rows: rows.len() as u32,
            replicated_rows: 0,
        };
        let mut d2_arm = canonical_d2_report().arms.remove(0);
        d2_arm.d1_key = d1_arm.key;
        d2_arm.maximum_query_pages = 1;
        d2_arm.maximum_record_id_bytes = 5;
        d2_arm.pages = vec![page_ref];

        let selector_quantizer = fit_v23_diagnostic_quantizer(
            V23QuantizerFamily::SrhtPq,
            V23_SELECTOR_CODE_WIDTHS[0],
            dimensions,
            &training,
        )
        .unwrap();
        let selector_state = serde_json::to_value(selector_quantizer.state()).unwrap();
        let selector_arm = V23D1Arm {
            key: V23D1ArmKey {
                family: V23QuantizerFamily::SrhtPq,
                code_width_bytes: V23_SELECTOR_CODE_WIDTHS[0],
            },
            quantizer_checksum: blake3::hash(&serde_json::to_vec(&selector_state).unwrap())
                .to_hex()
                .to_string(),
            quantizer_state: selector_state,
            query_samples: Vec::new(),
            oracle_recall_ppm: 1_000_000,
            routed_recall_ppm: 1_000_000,
            scalar_simd_ids_equal: true,
            scalar_simd_max_distance_delta_ppm: 0,
            cpu_p99_ns: 1,
            wave_projected_bytes: 1,
            passed: false,
        };
        let selector_input = V23SelectorInput {
            generation_checksum: [23; 32],
            metric: VectorMetric::SquaredEuclidean,
            dimensions: dimensions as u32,
            page_count: 1,
            code_width: selector_arm.key.code_width_bytes,
            maximum_assignments_per_row: 2,
            coarse_centroids: vec![rows[0].clone()],
            rows: vec![V23SelectorRow::new(
                0,
                0,
                None,
                7,
                &selector_quantizer.encode(&rows[0]).unwrap(),
            )],
        };
        let selector_bytes = encode_v23_selector(&selector_input).unwrap();
        let selector_checksum = blake3::hash(&selector_bytes).to_hex().to_string();
        d2_arm.selector_key = selector_arm.key;
        d2_arm.selector = V23SelectorRef {
            generation_checksum: selector_input.generation_checksum,
            metric: selector_input.metric.clone(),
            dimensions: selector_input.dimensions,
            coarse_cells: 1,
            page_count: 1,
            maximum_assignments_per_row: selector_input.maximum_assignments_per_row,
            code_width: selector_input.code_width,
            row_count: 1,
            path: format!("selectors/{selector_checksum}"),
            checksum: selector_checksum,
            encoded_bytes: selector_bytes.len() as u64,
        };

        let object_root = tempfile::tempdir().unwrap();
        std::fs::create_dir(object_root.path().join("pages")).unwrap();
        std::fs::create_dir(object_root.path().join("selectors")).unwrap();
        let page_path = object_root.path().join(&d2_arm.pages[0].path);
        std::fs::write(&page_path, &page_bytes).unwrap();
        let selector_path = object_root.path().join(&d2_arm.selector.path);
        assert!(matches!(
            V23D3Executor::new(
                &format!("file://{}", object_root.path().display()),
                &d1_arm,
                &selector_arm,
                &d2_arm,
                2 * V23_WAVE_MAX_BYTES,
            ),
            Err(crate::BorsukError::ObjectStoreNotFound { .. })
        ));
        std::fs::write(&selector_path, vec![0_u8; selector_bytes.len()]).unwrap();
        assert!(matches!(
            V23D3Executor::new(
                &format!("file://{}", object_root.path().display()),
                &d1_arm,
                &selector_arm,
                &d2_arm,
                2 * V23_WAVE_MAX_BYTES,
            ),
            Err(crate::BorsukError::ChecksumMismatch { .. })
        ));
        std::fs::write(&selector_path, &selector_bytes).unwrap();
        let executor = V23D3Executor::new(
            &format!("file://{}", object_root.path().display()),
            &d1_arm,
            &selector_arm,
            &d2_arm,
            2 * V23_WAVE_MAX_BYTES,
        )
        .unwrap();
        let result = executor.execute(0, &rows[0]).unwrap();
        validate_wave_sample(&result.sample).unwrap();
        assert_eq!(result.sample.page_ordinals, vec![0]);
        assert_eq!(result.sample.backing_gets, 1);
        assert_eq!(result.sample.backing_bytes, page_bytes.len() as u64);
        assert_eq!(result.sample.encoded_bytes, page_bytes.len() as u64);
        assert_eq!(result.sample.candidate_rows, 2);
        assert!(result.sample.backing_service_us_max <= result.sample.backing_service_us_sum);
        assert!(result.sample.backing_queue_us_max <= result.sample.backing_queue_us_sum);
        assert_eq!(result.request_peak_gets, 1);
        assert_eq!(result.ranked.ids[0], b"row-0");
        assert_eq!(
            result.sample.elapsed_ns,
            result
                .sample
                .transient_admission_wait_ns
                .saturating_add(result.sample.request_admission_wait_ns)
                .saturating_add(result.sample.service_ns)
        );
        assert!(result.transient_peak_bytes <= 2 * V23_WAVE_MAX_BYTES);

        let old_transient_bytes = page_bytes.len() as u64
            + result.sample.candidate_rows
                * (2 * u64::from(d2_arm.maximum_record_id_bytes)
                    + 2 * u64::from(d1_arm.key.code_width_bytes)
                    + 128)
            + dimensions as u64 * 8;
        let under_admitted = V23D3Executor::new(
            &format!("file://{}", object_root.path().display()),
            &d1_arm,
            &selector_arm,
            &d2_arm,
            old_transient_bytes,
        )
        .unwrap();
        assert!(matches!(
            under_admitted.execute(9, &rows[0]),
            Err(crate::BorsukError::InvalidSearchOptions(_))
        ));

        let mut cosine_arm = d2_arm.clone();
        cosine_arm.pages[0].metric = VectorMetric::Cosine;
        assert!(
            V23D3Executor::new(
                &format!("file://{}", object_root.path().display()),
                &d1_arm,
                &selector_arm,
                &cosine_arm,
                2 * V23_WAVE_MAX_BYTES,
            )
            .is_err()
        );

        let one_wave_bytes = result.transient_peak_bytes;
        let mut bounded = V23D3Executor::new(
            &format!("file://{}", object_root.path().display()),
            &d1_arm,
            &selector_arm,
            &d2_arm,
            one_wave_bytes,
        )
        .unwrap();
        bounded.request_gate = std::sync::Arc::new(crate::segment_cache::ByteAdmissionGate::new(1));
        let concurrent = std::thread::scope(|scope| {
            let workers = (0_u32..2)
                .map(|query_index| {
                    let executor = &bounded;
                    let query = &rows[0];
                    scope.spawn(move || executor.execute(query_index, query).unwrap())
                })
                .collect::<Vec<_>>();
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(concurrent.iter().all(|wave| wave.sample.backing_gets == 1
            && wave.sample.backing_get_concurrency == 1
            && wave.request_peak_gets <= 1
            && wave.transient_peak_bytes <= one_wave_bytes));
        assert_eq!(bounded.request_gate.peak_bytes(), 1);

        std::fs::write(&page_path, vec![0_u8; page_bytes.len()]).unwrap();
        assert!(matches!(
            executor.execute(1, &rows[0]),
            Err(crate::BorsukError::ChecksumMismatch { .. })
        ));
        std::fs::write(&page_path, &page_bytes).unwrap();
        let retry = executor.execute(2, &rows[0]).unwrap();
        assert_eq!(retry.sample.backing_gets, 1);

        std::fs::remove_file(&page_path).unwrap();
        assert!(matches!(
            executor.execute(3, &rows[0]),
            Err(crate::BorsukError::ObjectStoreNotFound { .. })
        ));
        let constrained = V23D3Executor::new(
            &format!("file://{}", object_root.path().display()),
            &d1_arm,
            &selector_arm,
            &d2_arm,
            1,
        )
        .unwrap();
        assert!(matches!(
            constrained.execute(4, &rows[0]),
            Err(crate::BorsukError::InvalidSearchOptions(_))
        ));
    }

    #[test]
    fn v23_d2_page_plan_is_deterministic_balanced_and_primary_complete() {
        let rows = (0_u64..24)
            .map(|source_ordinal| {
                let primary_cell = (source_ordinal / 8) as u32;
                let local = (source_ordinal % 8) as f32;
                V23PlanningRow {
                    source_ordinal,
                    canonical_record_id: format!("row-{source_ordinal:02}").into_bytes().into(),
                    primary_cell,
                    geometry: vec![primary_cell as f32 * 10.0 + local, local / 8.0].into(),
                    code: vec![source_ordinal as u8; 8].into(),
                }
            })
            .collect::<Vec<_>>();
        let first = plan_v23_pages(&rows, 4, 2).unwrap();
        let second = plan_v23_pages(&rows, 4, 2).unwrap();
        assert_eq!(
            serde_json::to_vec(&first).unwrap(),
            serde_json::to_vec(&second).unwrap()
        );
        assert_eq!(first.pages.len(), 6);
        assert!(first.pages.iter().enumerate().all(|(ordinal, page)| {
            page.page_ordinal as usize == ordinal
                && page.primary_source_ordinals.len() == 4
                && page.encoded_bytes <= super::V23_PAGE_MAX_ENCODED_BYTES
                && page
                    .replicated_source_ordinals
                    .iter()
                    .all(|replica| !page.primary_source_ordinals.contains(replica))
        }));
        let mut primaries = first
            .pages
            .iter()
            .flat_map(|page| page.primary_source_ordinals.iter().copied())
            .collect::<Vec<_>>();
        primaries.sort_unstable();
        assert_eq!(primaries, (0_u64..24).collect::<Vec<_>>());
        assert!(
            first
                .pages
                .iter()
                .any(|page| !page.replicated_source_ordinals.is_empty())
        );
        assert!(first.maximum_secondary_pages_evaluated_per_row <= 17);
        assert!(first.maximum_replica_candidates_retained <= rows.len());
        let mut assignment_counts = vec![0_usize; rows.len()];
        for ordinal in first.pages.iter().flat_map(|page| {
            page.primary_source_ordinals
                .iter()
                .chain(page.replicated_source_ordinals.iter())
        }) {
            assignment_counts[*ordinal as usize] += 1;
        }
        assert!(
            assignment_counts
                .into_iter()
                .all(|count| (1..=2).contains(&count))
        );

        assert!(plan_v23_pages(&rows, 4, 0).is_err());
        assert!(plan_v23_pages(&rows, 4, 4).is_err());
    }

    #[test]
    fn v23_d2_boundary_closure_is_parent_neighbor_bounded() {
        let rows = (0_u64..40)
            .map(|source_ordinal| V23PlanningRow {
                source_ordinal,
                canonical_record_id: format!("bounded-{source_ordinal:02}").into_bytes().into(),
                primary_cell: source_ordinal as u32,
                geometry: vec![source_ordinal as f32, 0.0].into(),
                code: vec![source_ordinal as u8; 8].into(),
            })
            .collect::<Vec<_>>();
        let planned = plan_v23_pages(&rows, 1, 3).unwrap();
        assert_eq!(planned.pages.len(), 40);
        assert!(planned.maximum_secondary_pages_evaluated_per_row <= 17);
        assert!(planned.maximum_replica_candidates_retained <= rows.len());
    }

    #[test]
    fn v23_d2_boundary_closure_stays_bounded_under_one_parent_skew() {
        let rows = (0_u64..40)
            .map(|source_ordinal| V23PlanningRow {
                source_ordinal,
                canonical_record_id: format!("skewed-{source_ordinal:02}").into_bytes().into(),
                primary_cell: 0,
                geometry: vec![source_ordinal as f32, 0.0].into(),
                code: vec![source_ordinal as u8; 8].into(),
            })
            .collect::<Vec<_>>();

        let planned = plan_v23_pages(&rows, 1, 3).unwrap();
        assert_eq!(planned.pages.len(), 40);
        assert!(planned.maximum_secondary_pages_evaluated_per_row <= 17);
        assert!(planned.maximum_replica_candidates_retained <= rows.len());
    }

    #[test]
    fn v23_d2_rejects_a_metric_without_bounded_page_routing() {
        let rows = (0_u64..4)
            .map(|source_ordinal| V23PlanningRow {
                source_ordinal,
                canonical_record_id: format!("metric-{source_ordinal}").into_bytes().into(),
                primary_cell: 0,
                geometry: vec![source_ordinal as f32, 1.0].into(),
                code: vec![source_ordinal as u8; 8].into(),
            })
            .collect::<Vec<_>>();
        assert!(plan_v23_pages_for_metric(&rows, 1, 2, &VectorMetric::InnerProduct).is_err());
    }

    #[test]
    fn v23_d2_primary_pages_follow_semantic_microclusters_not_lexicographic_chunks() {
        let rows = (0_u64..8)
            .map(|source_ordinal| {
                let upper_cluster = source_ordinal % 2 == 1;
                V23PlanningRow {
                    source_ordinal,
                    canonical_record_id: format!("semantic-{source_ordinal:02}")
                        .into_bytes()
                        .into(),
                    primary_cell: 0,
                    geometry: vec![
                        source_ordinal as f32,
                        if upper_cluster { 10.0 } else { -10.0 },
                    ]
                    .into(),
                    code: vec![source_ordinal as u8; 8].into(),
                }
            })
            .collect::<Vec<_>>();

        let planned = plan_v23_pages(&rows, 4, 1).unwrap();
        assert_eq!(planned.pages.len(), 2);
        for page in &planned.pages {
            let signs = page
                .primary_source_ordinals
                .iter()
                .map(|ordinal| ordinal % 2)
                .collect::<BTreeSet<_>>();
            assert_eq!(signs.len(), 1, "page mixed semantic clusters: {page:?}");
        }
    }

    #[test]
    fn v23_d2_replica_admission_enforces_two_x_storage_ceiling() {
        let rows = (0_u64..12)
            .map(|source_ordinal| V23PlanningRow {
                source_ordinal,
                canonical_record_id: format!("amplification-{source_ordinal:02}")
                    .into_bytes()
                    .into(),
                primary_cell: (source_ordinal / 4) as u32,
                geometry: vec![source_ordinal as f32, (source_ordinal % 4) as f32].into(),
                code: vec![source_ordinal as u8; 8].into(),
            })
            .collect::<Vec<_>>();

        let planned = plan_v23_pages(&rows, 4, 3).unwrap();
        let assignments = planned
            .pages
            .iter()
            .map(|page| page.primary_source_ordinals.len() + page.replicated_source_ordinals.len())
            .sum::<usize>();
        assert!(assignments <= 2 * rows.len());
        let mut assignment_counts = vec![0_usize; rows.len()];
        for ordinal in planned.pages.iter().flat_map(|page| {
            page.primary_source_ordinals
                .iter()
                .chain(page.replicated_source_ordinals.iter())
        }) {
            assignment_counts[*ordinal as usize] += 1;
        }
        assert!(
            assignment_counts
                .into_iter()
                .all(|count| (1..=3).contains(&count))
        );
    }

    #[test]
    fn v23_d2_replica_ties_use_ratio_page_then_source_ordinal() {
        let rows = vec![
            V23PlanningRow {
                source_ordinal: 0,
                canonical_record_id: vec![b'z'; 245_635].into(),
                primary_cell: 0,
                geometry: vec![0.0].into(),
                code: vec![0; 8].into(),
            },
            V23PlanningRow {
                source_ordinal: 9,
                canonical_record_id: vec![b'a'].into(),
                primary_cell: 1,
                geometry: vec![1.0].into(),
                code: vec![1; 8].into(),
            },
            V23PlanningRow {
                source_ordinal: 7,
                canonical_record_id: vec![b'b'].into(),
                primary_cell: 2,
                geometry: vec![-1.0].into(),
                code: vec![2; 8].into(),
            },
        ];

        let planned = plan_v23_pages(&rows, 1, 2).unwrap();
        assert_eq!(planned.pages[0].encoded_bytes, 245_760);
        assert_eq!(planned.pages[0].replicated_source_ordinals.as_ref(), &[7]);
    }

    #[test]
    fn v23_d2_rejects_one_primary_row_larger_than_a_page() {
        let rows = vec![V23PlanningRow {
            source_ordinal: 0,
            canonical_record_id: vec![b'x'; 245_649].into(),
            primary_cell: 0,
            geometry: vec![0.0].into(),
            code: vec![0; 8].into(),
        }];
        assert!(plan_v23_pages(&rows, 1, 1).is_err());
    }

    #[test]
    fn v23_d1_production_contiguous_scores_match_scalar_scores() {
        let vectors = (0_usize..256)
            .map(|row| {
                (0_usize..16)
                    .map(|dimension| ((row * 17 + dimension * 13) % 101) as f32 / 50.5 - 1.0)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let query = vectors[7].clone();
        for (family, width) in [
            (V23QuantizerFamily::SrhtPq, 8_u16),
            (V23QuantizerFamily::SrhtPq, 16_u16),
            (V23QuantizerFamily::FastTurboQuantMse, 8_u16),
            (V23QuantizerFamily::FastTurboQuantProd, 16_u16),
        ] {
            let quantizer = fit_v23_diagnostic_quantizer(family, width, 16, &vectors).unwrap();
            assert_eq!(quantizer.code_bytes_per_vector(), usize::from(width));
            let encoded = vectors
                .iter()
                .map(|vector| quantizer.encode(vector).unwrap())
                .collect::<Vec<_>>();
            let contiguous = encoded.concat();
            let scalar = quantizer
                .score_codes(&query, encoded.iter().map(Vec::as_slice))
                .unwrap();
            let prepared = quantizer.prepare_contiguous_query(&query).unwrap();
            let simd = quantizer
                .score_prepared_contiguous_codes(&prepared, &contiguous)
                .unwrap();
            assert_eq!(simd.len(), scalar.len());
            for (observed, expected) in simd.iter().zip(scalar) {
                assert!(
                    (observed - expected).abs() <= 1.0e-5_f32.max(expected.abs() * 1.0e-5),
                    "{family:?}/{width}: {observed} != {expected}"
                );
            }
        }
    }

    #[test]
    fn v23_d1_f16_flat_is_near_exact_and_page_width_is_not_pq_capped() {
        let dimensions = 96_usize;
        let vectors = (0_usize..32)
            .map(|row| {
                (0..dimensions)
                    .map(|dimension| {
                        let bits = ((row * 37 + dimension * 11) % 257) as f32;
                        half::f16::from_f32(bits / 256.0 - 0.5).to_f32()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let code_width = u16::try_from(dimensions * 2).unwrap();
        let key = V23D1ArmKey {
            family: V23QuantizerFamily::F16Flat,
            code_width_bytes: code_width,
        };
        assert!(v23_d1_arm_keys(dimensions).contains(&key));
        assert_eq!(v23_d1_projected_page_rows(code_width, 32), 1_077);
        assert_eq!(v23_d1_projected_page_bytes(code_width, 32), 245_656);
        assert!(
            v23_d1_projected_page_bytes(code_width, 32) * V23_WAVE_MAX_PAGES as u64
                <= V23_WAVE_MAX_BYTES
        );

        let quantizer =
            fit_v23_diagnostic_quantizer(key.family, code_width, dimensions, &vectors).unwrap();
        assert_eq!(quantizer.code_bytes_per_vector(), dimensions * 2);
        let encoded = vectors
            .iter()
            .map(|vector| quantizer.encode(vector).unwrap())
            .collect::<Vec<_>>();
        let query = &vectors[7];
        let prepared = quantizer.prepare_contiguous_query(query).unwrap();
        let observed = quantizer
            .score_prepared_contiguous_codes(&prepared, &encoded.concat())
            .unwrap();
        let expected = vectors
            .iter()
            .map(|vector| crate::metric::squared_euclidean_simd(query, vector))
            .collect::<Vec<_>>();
        assert_eq!(observed, expected);
        let saturated = quantizer.encode(&vec![f32::MAX; dimensions]).unwrap();
        assert!(
            quantizer
                .score_codes(query, std::iter::once(saturated.as_slice()))
                .unwrap()[0]
                .is_finite()
        );
        let wave = v23_d1_bounded_wave_codes(&encoded.concat(), code_width, 32).unwrap();
        assert_eq!(wave.len() / dimensions / 2, 1_077 * V23_WAVE_MAX_PAGES);

        let page_input = V23PageInput {
            generation_checksum: [9; 32],
            page_ordinal: 0,
            metric: VectorMetric::Cosine,
            dimensions: dimensions as u32,
            family: key.family,
            code_width,
            primary_rows: vec![V23PageRow {
                canonical_record_id: b"row-0".to_vec().into_boxed_slice(),
                code: encoded[0].clone().into_boxed_slice(),
            }],
            replicated_rows: Vec::new(),
        };
        let page = encode_v23_page(&page_input).unwrap();
        assert_eq!(read_v23_u16(&page, 64), Some(code_width));
        let checksum = blake3::hash(&page).to_hex().to_string();
        let page_ref = V23PageRef {
            generation_checksum: page_input.generation_checksum,
            page_ordinal: 0,
            metric: VectorMetric::Cosine,
            dimensions: dimensions as u32,
            family: key.family,
            code_width,
            path: format!("pages/{checksum}"),
            checksum,
            encoded_bytes: page.len() as u64,
            primary_rows: 1,
            replicated_rows: 0,
        };
        let decoded = decode_v23_page(page.clone(), &page_ref).unwrap();
        assert_eq!(decoded.code(0), Some(encoded[0].as_slice()));

        let mut non_finite_page = page.to_vec();
        let code_start = usize::try_from(V23_PAGE_HEADER_BYTES).unwrap() + 8 + b"row-0".len();
        non_finite_page[code_start..code_start + 2]
            .copy_from_slice(&half::f16::NAN.to_bits().to_le_bytes());
        let non_finite_page = bytes::Bytes::from(non_finite_page);
        let mut non_finite_ref = page_ref;
        non_finite_ref.checksum = blake3::hash(&non_finite_page).to_hex().to_string();
        non_finite_ref.path = format!("pages/{}", non_finite_ref.checksum);
        assert!(decode_v23_page(non_finite_page, &non_finite_ref).is_err());
    }
}
