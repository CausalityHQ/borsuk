use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    path::Path,
    time::Instant,
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    BorsukError, Result,
    centroid_hnsw::{CatalogRouter, CatalogRoutingStrategy},
    global_pq_sidecar::{GlobalScanQuantizer, GlobalScanQuantizerState},
    logical_cell_catalog::LogicalCellCatalog,
    metric::VectorMetric,
    rotated_product_quantizer::{ProductQuantizerConfig, ProductRotation, RotatedProductQuantizer},
    turboquant::{FastTurboQuantMseScanQuantizer, FastTurboQuantProdScanQuantizer},
    v22_feasibility::{V22_MAX_EXACT_PREFIX_ROWS, V22StageLQueryPrefix, V22StageLSpill},
};

#[allow(dead_code, reason = "consumed by the planned D2 page-codec slice")]
pub(crate) const V23_PAGE_MAX_ENCODED_BYTES: u64 = 245_760;
#[allow(dead_code, reason = "consumed by the planned D2 and D3 slices")]
pub(crate) const V23_WAVE_MAX_PAGES: usize = 4;
pub(crate) const V23_WAVE_MAX_BYTES: u64 = 983_040;
#[allow(dead_code, reason = "consumed by the planned D2 RAM projection")]
pub(crate) const V23_PROCESS_MAX_BYTES: u64 = 3 * 1024 * 1024 * 1024;
pub(crate) const V23_DIAGNOSTIC_QUERIES: usize = 32;
const V23_PAGE_HEADER_BYTES: u64 = 96;
const V23_PAGE_MAGIC: &[u8; 4] = b"BVP1";
const V23_PAGE_VERSION: u8 = 1;
const V23_PROJECTED_ROWS: u64 = 100_000_000;
const V23_PROJECTED_ROOT_HEADER_BYTES: u64 = 96;
const V23_PROJECTED_ROOT_FIXED_BYTES_PER_PAGE: u64 = 96;
const V23_PROJECTED_CATALOG_FIXED_BYTES_PER_PAGE: u64 = 32;
// Production HNSW caps each tower at 17 layers, with 32 base and 16 upper
// neighbours. 4 KiB/page exceeds Vec headers, maximum adjacency capacity, and
// allocator rounding for that bounded topology.
const V23_PROJECTED_ROUTER_BYTES_PER_PAGE: u64 = 4_096;
const V23_PROJECTED_FIXED_RUNTIME_BYTES: u64 = 512 * 1024 * 1024;
const V23_D2_EVALUATED_ARMS: u64 = 3 * 3 * V23_WAVE_MAX_PAGES as u64;
const V23_D2_LIGHTWEIGHT_SAMPLE_SLACK_BYTES: u64 = 4_096;
const V23_D1_PROJECTED_PAGE_ROWS: u64 = 2_048;
const V23_SCALAR_SIMD_MAX_DISTANCE_DELTA_PPM: u64 = 10;
#[allow(dead_code, reason = "consumed by the planned D3 benchmark slice")]
pub(crate) const V23_D3_WAVES: usize = 1_000;
const V23_D1_CPU_MAX_NS: u64 = 15_000_000;

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Canonical identity of one D1 quantizer arm.
pub struct V23D1ArmKey {
    /// Production quantizer family.
    pub family: V23QuantizerFamily,
    /// Fixed encoded bytes carried by every row.
    pub code_width_bytes: u8,
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
    /// Recomputed ground-truth hits in `oracle`.
    pub oracle_hits: u8,
    /// Recomputed ground-truth hits in `routed`.
    pub routed_hits: u8,
    /// Query preparation plus both production SIMD scans.
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
    /// Conservative encoded-byte projection for four maximum pages.
    pub four_page_projected_bytes: u64,
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
    pub metric: VectorMetric,
    /// Exact dense-vector dimensionality.
    pub dimensions: u32,
    /// Exact production quantizer family.
    pub family: V23QuantizerFamily,
    /// Fixed encoded bytes carried by each row.
    pub code_width: u8,
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
    /// Full-dimensional routing centroid.
    pub centroid: Vec<f32>,
}

pub(crate) type V23PageSink<'a> = dyn FnMut(&V23PageRef, &Bytes) -> Result<()> + 'a;

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
    pub(crate) code_width: u8,
    pub(crate) primary_rows: Vec<V23PageRow>,
    pub(crate) replicated_rows: Vec<V23PageRow>,
}

#[derive(Debug, Clone)]
pub(crate) struct V23DecodedPage {
    bytes: Bytes,
    offsets: Box<[u32]>,
    id_start: usize,
    code_start: usize,
    primary_rows: usize,
    replicated_rows: usize,
    code_width: usize,
}

impl V23DecodedPage {
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
    }
}

fn read_v23_u32(bytes: &[u8], start: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        bytes.get(start..start + 4)?.try_into().ok()?,
    ))
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
        || !valid_diagnostic_code_width(V23D1ArmKey {
            family: input.family,
            code_width_bytes: input.code_width,
        })
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
    encoded[7] = input.code_width;
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
        || !valid_diagnostic_code_width(V23D1ArmKey {
            family: page_ref.family,
            code_width_bytes: page_ref.code_width,
        })
        || !valid_checksum(&page_ref.checksum)
        || page_ref.path != expected_path
        || blake3::hash(&bytes).to_hex().as_str() != page_ref.checksum
        || bytes.get(0..4) != Some(V23_PAGE_MAGIC.as_slice())
        || bytes[4] != V23_PAGE_VERSION
        || v23_metric_tag(&page_ref.metric) != Some(bytes[5])
        || v23_family_tag(page_ref.family) != bytes[6]
        || page_ref.code_width != bytes[7]
        || read_v23_u32(&bytes, 8) != Some(page_ref.dimensions)
        || read_v23_u32(&bytes, 12) != Some(page_ref.page_ordinal)
        || bytes.get(32..64) != Some(page_ref.generation_checksum.as_slice())
        || bytes[64..96].iter().any(|byte| *byte != 0)
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
    Ok(V23DecodedPage {
        bytes,
        offsets: offsets.into_boxed_slice(),
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
    /// Sum of complete selected page lengths.
    pub encoded_bytes: u64,
    /// Rows scanned before replica deduplication.
    pub candidate_rows: u64,
    /// Exact ground-truth top-ten record IDs.
    pub ground_truth_ids: Vec<Vec<u8>>,
    /// Code-ranked, replica-deduplicated top-ten result.
    pub ranked: V23RankedResult,
    /// Ground-truth rows physically covered by selected pages.
    pub gt_page_hits: u8,
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
    /// Query-scoped S3 Standard response bytes.
    pub backing_bytes: u64,
    /// Query preparation, decode, and SIMD ranking time.
    pub cpu_ns: u64,
    /// Complete measured cold-query wall time.
    pub elapsed_ns: u64,
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
        || sample.backing_bytes != sample.encoded_bytes
        || sample.cpu_ns == 0
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
        && key.code_width_bytes <= 64
        && (!matches!(key.family, V23QuantizerFamily::SrhtPq)
            || [8, 16, 32, 64].contains(&key.code_width_bytes))
}

pub(crate) fn fit_v23_diagnostic_quantizer(
    family: V23QuantizerFamily,
    code_width_bytes: u8,
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
    }
}

pub(crate) fn restore_v23_diagnostic_quantizer(arm: &V23D1Arm) -> Result<GlobalScanQuantizer> {
    if !valid_diagnostic_code_width(arm.key) || !valid_checksum(&arm.quantizer_checksum) {
        return Err(BorsukError::InvalidStorage(
            "V23 D1 quantizer authority differs".to_string(),
        ));
    }
    let state_bytes = serde_json::to_vec(&arm.quantizer_state).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 D1 quantizer state cannot be serialized: {error}"
        ))
    })?;
    if blake3::hash(&state_bytes).to_hex().as_str() != arm.quantizer_checksum {
        return Err(BorsukError::InvalidStorage(
            "V23 D1 quantizer state checksum differs".to_string(),
        ));
    }
    let state: GlobalScanQuantizerState = serde_json::from_value(arm.quantizer_state.clone())
        .map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "V23 D1 quantizer state cannot be decoded: {error}"
            ))
        })?;
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
    for code_width_bytes in [8_u8, 16, 32, 64] {
        if usize::from(code_width_bytes) <= dimensions {
            keys.insert(V23D1ArmKey {
                family: V23QuantizerFamily::SrhtPq,
                code_width_bytes,
            });
        }
    }
    for bits in 1_u8..=8 {
        if let Ok(quantizer) = FastTurboQuantMseScanQuantizer::new(23, dimensions, bits, 1)
            && let Ok(code_width_bytes) = u8::try_from(quantizer.packed_code_len())
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
            && let Ok(code_width_bytes) = u8::try_from(quantizer.packed_code_len())
            && code_width_bytes <= 64
        {
            keys.insert(V23D1ArmKey {
                family: V23QuantizerFamily::FastTurboQuantProd,
                code_width_bytes,
            });
        }
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
        let quantizer = fit_v23_diagnostic_quantizer(
            key.family,
            key.code_width_bytes,
            dimensions,
            &sample_vectors,
        )?;
        let quantizer_state = serde_json::to_value(quantizer.state()).map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "V23 D1 quantizer state cannot be canonicalized: {error}"
            ))
        })?;
        let quantizer_state_bytes = serde_json::to_vec(&quantizer_state).map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "V23 D1 quantizer state cannot be serialized: {error}"
            ))
        })?;
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
                    let started = Instant::now();
                    let distances = quantizer
                        .score_prepared_contiguous_codes(&prepared[query_index], &codes)?;
                    cpu_ns[query_index] = cpu_ns[query_index]
                        .saturating_add(started.elapsed().as_nanos().max(1) as u64);
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
        let mut scalar_simd_max_distance_delta_ppm = 0_u64;
        for query_index in 0..authority.queries.len() {
            if oracle_ids[query_index].len() != 2_048
                || routed_rows[query_index] != authority.routing_gates[query_index].1
            {
                return Err(BorsukError::InvalidStorage(
                    "V23 D1 candidate pools conflict with authority".to_string(),
                ));
            }
            let oracle_distances = quantizer.score_prepared_contiguous_codes(
                &prepared[query_index],
                &oracle_codes[query_index],
            )?;
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
        let projected_page_bytes = V23_PAGE_HEADER_BYTES
            .checked_add(4 * (V23_D1_PROJECTED_PAGE_ROWS + 1))
            .and_then(|bytes| {
                bytes.checked_add(
                    V23_D1_PROJECTED_PAGE_ROWS
                        * (u64::from(key.code_width_bytes) + u64::from(maximum_record_id_bytes)),
                )
            })
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V23 D1 page projection overflows".to_string())
            })?;
        let four_page_projected_bytes = projected_page_bytes.checked_mul(4).ok_or_else(|| {
            BorsukError::InvalidStorage("V23 D1 wave projection overflows".to_string())
        })?;
        arms.push(V23D1Arm {
            key,
            quantizer_checksum: blake3::hash(&quantizer_state_bytes).to_hex().to_string(),
            quantizer_state,
            query_samples,
            oracle_recall_ppm,
            routed_recall_ppm,
            scalar_simd_ids_equal,
            scalar_simd_max_distance_delta_ppm,
            cpu_p99_ns,
            four_page_projected_bytes,
            passed: oracle_recall_ppm >= 990_000
                && routed_recall_ppm >= 975_000
                && scalar_simd_ids_equal
                && scalar_simd_max_distance_delta_ppm <= V23_SCALAR_SIMD_MAX_DISTANCE_DELTA_PPM
                && cpu_p99_ns <= V23_D1_CPU_MAX_NS
                && projected_page_bytes <= V23_PAGE_MAX_ENCODED_BYTES
                && four_page_projected_bytes <= V23_WAVE_MAX_BYTES,
        });
    }
    let report = V23D1Report {
        schema: "borsuk-v23-d1-v3".to_string(),
        v20_root_checksum: authority.root_checksum.to_string(),
        v20_codebook_checksum: authority.codebook_checksum.to_string(),
        sample_ordinals_checksum: ordinal_hasher.finalize().to_hex().to_string(),
        query_vectors_checksum: v23_query_vectors_checksum(
            authority.query_ordinals,
            authority.queries,
        )?,
        query_ordinals: authority.query_ordinals.to_vec(),
        rows: authority.rows,
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
        || code_width > 64
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

fn v23_d2_projected_memory(
    unique_rows: u64,
    page_count: usize,
    dimensions: usize,
) -> Result<(u64, u64)> {
    if unique_rows == 0 || page_count == 0 || dimensions == 0 {
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
        .checked_mul(
            V23_PROJECTED_ROOT_FIXED_BYTES_PER_PAGE
                .checked_add(centroid_bytes)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("V23 projected root row overflows".to_string())
                })?,
        )
        .and_then(|bytes| bytes.checked_add(V23_PROJECTED_ROOT_HEADER_BYTES))
        .ok_or_else(|| BorsukError::InvalidStorage("V23 projected root overflows".to_string()))?;
    let projected_catalog_bytes = projected_pages
        .checked_mul(
            V23_PROJECTED_CATALOG_FIXED_BYTES_PER_PAGE
                .checked_add(centroid_bytes)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("V23 projected catalog row overflows".to_string())
                })?,
        )
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 projected catalog overflows".to_string())
        })?;
    let projected_router_bytes = projected_pages
        .checked_mul(V23_PROJECTED_ROUTER_BYTES_PER_PAGE)
        .ok_or_else(|| BorsukError::InvalidStorage("V23 projected router overflows".to_string()))?;
    let projected_ram_bytes = projected_root_bytes
        .checked_add(projected_catalog_bytes)
        .and_then(|bytes| bytes.checked_add(projected_router_bytes))
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
    code_width: u8,
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
        .and_then(|bytes| bytes.checked_mul(6))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 planner index projection overflows".to_string())
        })?;
    let decoded_and_planner = rows
        .checked_mul(
            decoded_row_bytes
                .checked_add(candidate_bytes)
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
    let dimensions = arm
        .pages
        .first()
        .map(|page| page.centroid.len())
        .filter(|dimensions| *dimensions > 0)
        .ok_or_else(|| BorsukError::InvalidStorage("V23 D2 page authority is empty".to_string()))?;
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

fn v23_d2_arm_dominates(left: &V23D2Arm, right: &V23D2Arm) -> bool {
    let left_max_bytes = left
        .query_samples
        .iter()
        .map(|sample| sample.encoded_bytes)
        .max()
        .unwrap_or(u64::MAX);
    let right_max_bytes = right
        .query_samples
        .iter()
        .map(|sample| sample.encoded_bytes)
        .max()
        .unwrap_or(u64::MAX);
    let left_max_pages = left
        .query_samples
        .iter()
        .map(|sample| sample.page_ordinals.len())
        .max()
        .unwrap_or(usize::MAX);
    let right_max_pages = right
        .query_samples
        .iter()
        .map(|sample| sample.page_ordinals.len())
        .max()
        .unwrap_or(usize::MAX);
    let no_worse = left.aggregate_recall_ppm >= right.aggregate_recall_ppm
        && left_max_bytes <= right_max_bytes
        && left_max_pages <= right_max_pages
        && left.storage_amplification_ppm <= right.storage_amplification_ppm
        && left.projected_ram_bytes <= right.projected_ram_bytes;
    let strictly_better = left.aggregate_recall_ppm > right.aggregate_recall_ppm
        || left_max_bytes < right_max_bytes
        || left_max_pages < right_max_pages
        || left.storage_amplification_ppm < right.storage_amplification_ppm
        || left.projected_ram_bytes < right.projected_ram_bytes;
    no_worse && strictly_better
}

fn v23_d2_arm_objective_cmp(left: &V23D2Arm, right: &V23D2Arm) -> Ordering {
    let maximum_bytes = |arm: &V23D2Arm| {
        arm.query_samples
            .iter()
            .map(|sample| sample.encoded_bytes)
            .max()
            .unwrap_or(u64::MAX)
    };
    let maximum_pages = |arm: &V23D2Arm| {
        arm.query_samples
            .iter()
            .map(|sample| sample.page_ordinals.len())
            .max()
            .unwrap_or(usize::MAX)
    };
    right
        .aggregate_recall_ppm
        .cmp(&left.aggregate_recall_ppm)
        .then_with(|| maximum_bytes(left).cmp(&maximum_bytes(right)))
        .then_with(|| maximum_pages(left).cmp(&maximum_pages(right)))
        .then_with(|| {
            left.storage_amplification_ppm
                .cmp(&right.storage_amplification_ppm)
        })
        .then_with(|| left.projected_ram_bytes.cmp(&right.projected_ram_bytes))
        .then_with(|| d2_arm_key(left).cmp(&d2_arm_key(right)))
}

fn select_v23_d2_frontier(arms: &[V23D2Arm]) -> Result<Vec<V23D2Arm>> {
    if arms.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "V23 D2 evaluated-arm authority is empty".to_string(),
        ));
    }
    let passing = arms.iter().filter(|arm| arm.passed).collect::<Vec<_>>();
    let candidates = if passing.is_empty() {
        arms.iter().collect::<Vec<_>>()
    } else {
        passing
    };
    let mut nondominated = candidates
        .iter()
        .enumerate()
        .filter(|(candidate, arm)| {
            !candidates.iter().enumerate().any(|(other, other_arm)| {
                other != *candidate && v23_d2_arm_dominates(other_arm, arm)
            })
        })
        .map(|(_, arm)| (*arm).clone())
        .collect::<Vec<_>>();
    nondominated.sort_by(v23_d2_arm_objective_cmp);
    nondominated.truncate(3);
    Ok(nondominated)
}

fn build_v23_d2_arms(
    authority: &V23D2CorpusAuthority<'_>,
    quantizer: &GlobalScanQuantizer,
    planning_rows: &[V23PlanningRow],
    primary_target_rows: u16,
    maximum_assignments_per_row: u8,
    materialized_page_budgets: Option<&BTreeSet<u8>>,
    page_sink: Option<&mut V23PageSink<'_>>,
) -> Result<Vec<V23D2Arm>> {
    let planning = plan_v23_pages_for_metric(
        planning_rows,
        primary_target_rows,
        maximum_assignments_per_row,
        &authority.metric,
    )?;
    let dimensions = authority.scratch.dimensions();
    let catalog = LogicalCellCatalog::from_centroids(
        23,
        dimensions,
        authority.metric.clone(),
        planning
            .pages
            .iter()
            .map(|page| page.centroid.to_vec())
            .collect(),
    )?;
    let page_centroids = catalog.centroids.clone();
    let router = CatalogRouter::build(
        std::sync::Arc::new(catalog),
        authority.metric.clone(),
        CatalogRoutingStrategy::production(&authority.metric, planning.pages.len()),
    )?;
    let generation_checksum = *blake3::Hash::from_hex(&authority.d1_report.v20_root_checksum)
        .map_err(|_| {
            BorsukError::InvalidStorage("V23 source generation checksum differs".to_string())
        })?
        .as_bytes();
    let encoded_pages = planning
        .pages
        .iter()
        .zip(page_centroids)
        .map(|(page, centroid)| {
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
                    centroid,
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
    let (projected_root_bytes, projected_ram_bytes) =
        v23_d2_projected_memory(unique_rows, pages.len(), dimensions)?;
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

    let mut built_arms = Vec::with_capacity(V23_WAVE_MAX_PAGES);
    for maximum_query_pages in 1_u8..=V23_WAVE_MAX_PAGES as u8 {
        let mut query_samples = Vec::with_capacity(authority.queries.len());
        for (query_index, query) in authority.queries.iter().enumerate() {
            let prepared_query = if authority.normalize {
                crate::metric::unit_l2_normalized(query)
            } else {
                query.clone()
            };
            let started = Instant::now();
            let mut page_ordinals =
                router.nearest(&prepared_query, usize::from(maximum_query_pages))?;
            page_ordinals.sort_unstable();
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
                encoded_bytes,
                candidate_rows,
                ground_truth_ids,
                ranked,
                gt_page_hits,
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
        let mut cpu = query_samples
            .iter()
            .map(|sample| sample.cpu_ns)
            .collect::<Vec<_>>();
        cpu.sort_unstable();
        let cpu_p99_ns = cpu[cpu.len() - 1];
        let passed = aggregate_recall_ppm >= 975_000
            && minimum_query_recall_ppm >= 800_000
            && storage_amplification_ppm <= 2_000_000
            && projected_ram_bytes <= V23_PROCESS_MAX_BYTES
            && cpu_p99_ns <= V23_D1_CPU_MAX_NS;
        if materialized_page_budgets.is_some_and(|budgets| !budgets.contains(&maximum_query_pages))
        {
            continue;
        }
        built_arms.push(V23D2Arm {
            d1_key: authority.d1_key,
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
            cpu_p99_ns,
            passed,
        });
    }
    Ok(built_arms)
}

pub(crate) fn build_v23_d2_report(authority: V23D2CorpusAuthority<'_>) -> Result<V23D2Report> {
    build_v23_d2_report_inner(authority, None)
}

pub(crate) fn build_v23_d2_report_with_page_sink(
    authority: V23D2CorpusAuthority<'_>,
    sink: &mut V23PageSink<'_>,
) -> Result<V23D2Report> {
    build_v23_d2_report_inner(authority, Some(sink))
}

fn build_v23_d2_report_inner(
    authority: V23D2CorpusAuthority<'_>,
    mut page_sink: Option<&mut V23PageSink<'_>>,
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
    let mut evaluated_arms = Vec::with_capacity(V23_D2_EVALUATED_ARMS as usize);
    for primary_target_rows in [512_u16, 1_024, 2_048] {
        for maximum_assignments_per_row in [1_u8, 2, 3] {
            evaluated_arms.extend(build_v23_d2_arms(
                &authority,
                &quantizer,
                &planning_rows,
                primary_target_rows,
                maximum_assignments_per_row,
                None,
                None,
            )?);
        }
    }
    let selected = select_v23_d2_frontier(&evaluated_arms)?;
    let mut selected_budgets = BTreeMap::<(u16, u8), BTreeSet<u8>>::new();
    for arm in &selected {
        selected_budgets
            .entry((arm.primary_target_rows, arm.maximum_assignments_per_row))
            .or_default()
            .insert(arm.maximum_query_pages);
    }
    let mut nondominated = Vec::with_capacity(selected.len());
    let mut emitted_page_paths = BTreeSet::new();
    for ((primary_target_rows, maximum_assignments_per_row), budgets) in selected_budgets {
        let rehydrated = match page_sink.as_deref_mut() {
            Some(sink) => {
                let mut unique_sink = |page: &V23PageRef, bytes: &Bytes| {
                    if emitted_page_paths.insert(page.path.clone()) {
                        sink(page, bytes)
                    } else {
                        Ok(())
                    }
                };
                build_v23_d2_arms(
                    &authority,
                    &quantizer,
                    &planning_rows,
                    primary_target_rows,
                    maximum_assignments_per_row,
                    Some(&budgets),
                    Some(&mut unique_sink),
                )
            }
            None => build_v23_d2_arms(
                &authority,
                &quantizer,
                &planning_rows,
                primary_target_rows,
                maximum_assignments_per_row,
                Some(&budgets),
                None,
            ),
        }?;
        for materialized in rehydrated {
            let key = d2_arm_key(&materialized);
            let mut evaluated = selected
                .iter()
                .find(|arm| d2_arm_key(arm) == key)
                .cloned()
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V23 D2 selected-arm rehydration is absent".to_string(),
                    )
                })?;
            evaluated.pages = materialized.pages;
            evaluated.projected_build_bytes = materialized.projected_build_bytes;
            nondominated.push(evaluated);
        }
    }
    nondominated.sort_by(v23_d2_arm_objective_cmp);
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
    let d1_report_bytes = serde_json::to_vec(authority.d1_report).map_err(|error| {
        BorsukError::InvalidStorage(format!("V23 D1 report cannot be canonicalized: {error}"))
    })?;
    let report = V23D2Report {
        schema: "borsuk-v23-d2-v3".to_string(),
        d1_report_checksum: blake3::hash(&d1_report_bytes).to_hex().to_string(),
        query_ordinals: authority.query_ordinals.to_vec(),
        rows: authority.scratch.total_rows(),
        arms: nondominated,
    };
    validate_d2_report(&report)?;
    Ok(report)
}

pub(crate) fn validate_d1_report(report: &V23D1Report) -> Result<()> {
    if report.schema != "borsuk-v23-d1-v3"
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
        let expected_page_projection = V23_PAGE_HEADER_BYTES
            + 4 * (V23_D1_PROJECTED_PAGE_ROWS + 1)
            + V23_D1_PROJECTED_PAGE_ROWS
                * (u64::from(arm.key.code_width_bytes) + u64::from(report.maximum_record_id_bytes));
        let expected_wave_projection = expected_page_projection.saturating_mul(4);
        if !valid_diagnostic_code_width(arm.key)
            || !valid_checksum(&arm.quantizer_checksum)
            || restore_v23_diagnostic_quantizer(arm).is_err()
            || arm.query_samples.len() != V23_DIAGNOSTIC_QUERIES
            || arm.four_page_projected_bytes != expected_wave_projection
        {
            return Err(BorsukError::InvalidStorage(
                "V23 D1 arm authority differs".to_string(),
            ));
        }
        let mut oracle_hits = 0_u64;
        let mut routed_hits = 0_u64;
        let mut cpu = Vec::with_capacity(V23_DIAGNOSTIC_QUERIES);
        let mut scalar_simd_ids_equal = true;
        let mut scalar_simd_max_distance_delta_ppm = 0_u64;
        for (expected_index, sample) in arm.query_samples.iter().enumerate() {
            let truth = sample.ground_truth_ids.iter().collect::<BTreeSet<_>>();
            validate_ranked_result(&sample.oracle)?;
            validate_ranked_result(&sample.scalar_oracle)?;
            validate_ranked_result(&sample.routed)?;
            scalar_simd_ids_equal &= sample.oracle.ids == sample.scalar_oracle.ids;
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
            && scalar_simd_ids_equal
            && scalar_simd_max_distance_delta_ppm <= V23_SCALAR_SIMD_MAX_DISTANCE_DELTA_PPM
            && expected_cpu_p99 <= V23_D1_CPU_MAX_NS
            && expected_page_projection <= V23_PAGE_MAX_ENCODED_BYTES
            && arm.four_page_projected_bytes <= V23_WAVE_MAX_BYTES;
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

fn d2_arm_key(arm: &V23D2Arm) -> (V23D1ArmKey, u16, u8, u8) {
    (
        arm.d1_key,
        arm.primary_target_rows,
        arm.maximum_assignments_per_row,
        arm.maximum_query_pages,
    )
}

pub(crate) fn validate_d2_report(report: &V23D2Report) -> Result<()> {
    if report.schema != "borsuk-v23-d2-v3"
        || !valid_checksum(&report.d1_report_checksum)
        || report.query_ordinals.len() != V23_DIAGNOSTIC_QUERIES
        || report
            .query_ordinals
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || report.rows == 0
        || report.arms.is_empty()
        || report.arms.len() > 3
        || (report.arms.iter().any(|arm| arm.passed) && report.arms.iter().any(|arm| !arm.passed))
        || report
            .arms
            .windows(2)
            .any(|pair| v23_d2_arm_objective_cmp(&pair[0], &pair[1]) != Ordering::Less)
        || report.arms.iter().enumerate().any(|(candidate, arm)| {
            report.arms.iter().enumerate().any(|(other, other_arm)| {
                other != candidate && v23_d2_arm_dominates(other_arm, arm)
            })
        })
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
            || ![512, 1_024, 2_048].contains(&arm.primary_target_rows)
            || !(1..=3).contains(&arm.maximum_assignments_per_row)
            || !(1..=V23_WAVE_MAX_PAGES as u8).contains(&arm.maximum_query_pages)
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
        let centroid_dimensions = arm.pages[0].centroid.len();
        let generation_checksum = arm.pages[0].generation_checksum;
        let metric = &arm.pages[0].metric;
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
                || usize::try_from(page.dimensions).ok() != Some(centroid_dimensions)
                || page.family != arm.d1_key.family
                || page.code_width != arm.d1_key.code_width_bytes
                || !valid_checksum(&page.checksum)
                || page.path != expected_path
                || page.encoded_bytes == 0
                || page.encoded_bytes > V23_PAGE_MAX_ENCODED_BYTES
                || page.primary_rows == 0
                || centroid_dimensions == 0
                || page.centroid.len() != centroid_dimensions
                || page.centroid.iter().any(|value| !value.is_finite())
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
            v23_d2_projected_memory(arm.unique_rows, arm.pages.len(), centroid_dimensions)?;
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
        let mut minimum_recall = 1_000_000_u64;
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
            if usize::try_from(sample.query_index).ok() != Some(expected_index)
                || sample.page_ordinals.is_empty()
                || sample.page_ordinals.len() > V23_WAVE_MAX_PAGES
                || sample.page_ordinals.len() > usize::from(arm.maximum_query_pages)
                || sample
                    .page_ordinals
                    .windows(2)
                    .any(|pair| pair[0] >= pair[1])
                || expected_bytes != Some(sample.encoded_bytes)
                || sample.encoded_bytes > V23_WAVE_MAX_BYTES
                || expected_rows != Some(sample.candidate_rows)
                || truth.len() != 10
                || sample.ground_truth_ids.iter().any(Vec::is_empty)
                || sample.gt_page_hits > 10
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
            minimum_recall = minimum_recall.min(sample.recall_ppm);
            cpu.push(sample.cpu_ns);
        }
        cpu.sort_unstable();
        let expected_aggregate = total_hits.saturating_mul(1_000_000)
            / ((V23_DIAGNOSTIC_QUERIES as u64).saturating_mul(10));
        let expected_cpu_p99 = cpu[V23_DIAGNOSTIC_QUERIES - 1];
        let expected_maximum_pages = usize::from(arm.maximum_query_pages).min(arm.pages.len());
        let expected_passed = expected_aggregate >= 975_000
            && minimum_recall >= 800_000
            && arm.storage_amplification_ppm <= 2_000_000
            && arm.projected_ram_bytes <= V23_PROCESS_MAX_BYTES
            && expected_cpu_p99 <= V23_D1_CPU_MAX_NS;
        if arm.aggregate_recall_ppm != expected_aggregate
            || arm.minimum_query_recall_ppm != minimum_recall
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
    use std::collections::BTreeSet;

    use super::{
        V23_DIAGNOSTIC_QUERIES, V23_WAVE_MAX_BYTES, V23D1Arm, V23D1ArmKey, V23D1QuerySample,
        V23D1Report, V23D2Arm, V23D2QuerySample, V23D2Report, V23PageInput, V23PageRef, V23PageRow,
        V23PlanningRow, V23QuantizerFamily, V23RankedResult, V23ReplicaCandidate, V23WaveSample,
        decode_v23_page, encode_v23_page, fit_v23_diagnostic_quantizer, plan_v23_pages,
        plan_v23_pages_for_metric, restore_v23_diagnostic_quantizer, select_v23_d2_frontier,
        stream_v23_materialized_pages, v23_d2_projected_build_memory, v23_d2_projected_memory,
        validate_d1_report, validate_d2_report, validate_v23_d2_query_binding,
        validate_v23_d2_query_prefixes, validate_wave_sample,
    };
    use crate::metric::VectorMetric;
    use crate::v22_feasibility::V22StageLQueryPrefix;

    fn canonical_wave() -> V23WaveSample {
        V23WaveSample {
            query_index: 7,
            page_ordinals: vec![3, 9, 12, 18],
            encoded_bytes: 983_040,
            candidate_rows: 8_192,
            backing_gets: 4,
            backing_bytes: 983_040,
            cpu_ns: 2_000_000,
            elapsed_ns: 40_000_000,
        }
    }

    fn ranked_top_ten() -> V23RankedResult {
        V23RankedResult {
            ids: (0_u8..10).map(|value| vec![b'i', value]).collect(),
            distances: (0_u8..10).map(f32::from).collect(),
        }
    }

    fn serialized_test_quantizer(
        family: V23QuantizerFamily,
        code_width_bytes: u8,
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
                oracle_hits: 10,
                routed_hits: 10,
                cpu_ns: 1_000_000,
            })
            .collect();
        V23D1Report {
            schema: "borsuk-v23-d1-v3".to_string(),
            v20_root_checksum: "a".repeat(64),
            v20_codebook_checksum: "b".repeat(64),
            sample_ordinals_checksum: "c".repeat(64),
            query_vectors_checksum: "9".repeat(64),
            query_ordinals: (0_u64..32).collect(),
            rows: 9_990_000,
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
                four_page_projected_bytes: 4 * (96 + 4 * 2_049 + 2_048 * (64 + 32)),
                passed: true,
            }],
        }
    }

    fn canonical_d2_report() -> V23D2Report {
        let query_samples = (0_u32..32)
            .map(|query_index| V23D2QuerySample {
                query_index,
                page_ordinals: vec![0],
                encoded_bytes: 120_000,
                candidate_rows: 1_000,
                ground_truth_ids: ranked_top_ten().ids,
                ranked: ranked_top_ten(),
                gt_page_hits: 10,
                hits: 10,
                recall_ppm: 1_000_000,
                cpu_ns: 1_000_000,
            })
            .collect();
        V23D2Report {
            schema: "borsuk-v23-d2-v3".to_string(),
            d1_report_checksum: "e".repeat(64),
            query_ordinals: (0_u64..32).collect(),
            rows: 1_000,
            arms: vec![V23D2Arm {
                d1_key: V23D1ArmKey {
                    family: V23QuantizerFamily::SrhtPq,
                    code_width_bytes: 64,
                },
                primary_target_rows: 1_024,
                maximum_assignments_per_row: 1,
                maximum_query_pages: 1,
                maximum_record_id_bytes: 32,
                pages: vec![V23PageRef {
                    generation_checksum: [1; 32],
                    page_ordinal: 0,
                    metric: VectorMetric::SquaredEuclidean,
                    dimensions: 4,
                    family: V23QuantizerFamily::SrhtPq,
                    code_width: 64,
                    path: format!("pages/{}", "f".repeat(64)),
                    checksum: "f".repeat(64),
                    encoded_bytes: 120_000,
                    primary_rows: 1_000,
                    replicated_rows: 0,
                    centroid: vec![0.0, 0.0, 0.0, 0.0],
                }],
                unique_rows: 1_000,
                total_assignments: 1_000,
                storage_amplification_ppm: 1_000_000,
                projected_root_bytes: 96 + 100_000 * (96 + 4 * 4),
                projected_ram_bytes: 96
                    + 100_000 * (96 + 4 * 4)
                    + 100_000 * (32 + 4 * 4)
                    + 100_000 * 4_096
                    + 512 * 1024 * 1024
                    + 2 * V23_WAVE_MAX_BYTES,
                projected_build_bytes: v23_d2_projected_build_memory(
                    1_000, 1, 120_000, 4, 32, 64, 1,
                )
                .unwrap(),
                query_samples,
                aggregate_recall_ppm: 1_000_000,
                minimum_query_recall_ppm: 1_000_000,
                cpu_p99_ns: 1_000_000,
                passed: true,
            }],
        }
    }

    #[test]
    fn v23_contract_rejects_a_fifth_page() {
        let sample = canonical_wave();
        validate_wave_sample(&sample).unwrap();

        let mut overflow = sample;
        overflow.page_ordinals.push(21);
        overflow.backing_gets = 5;
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

        let mut backing_bytes_differ = canonical.clone();
        backing_bytes_differ.backing_bytes -= 1;
        assert!(validate_wave_sample(&backing_bytes_differ).is_err());

        let mut no_cpu = canonical.clone();
        no_cpu.cpu_ns = 0;
        assert!(validate_wave_sample(&no_cpu).is_err());

        let mut no_elapsed = canonical;
        no_elapsed.elapsed_ns = 0;
        assert!(validate_wave_sample(&no_elapsed).is_err());
    }

    #[test]
    fn v23_d1_contract_recomputes_gates_and_rejects_wide_codes() {
        let canonical = canonical_d1_report();
        validate_d1_report(&canonical).unwrap();

        let mut wide = canonical.clone();
        wide.arms[0].key.code_width_bytes = 65;
        assert!(validate_d1_report(&wide).is_err());

        let mut aggregate_drift = canonical.clone();
        aggregate_drift.arms[0].routed_recall_ppm -= 1;
        assert!(validate_d1_report(&aggregate_drift).is_err());

        let mut non_finite = canonical.clone();
        non_finite.arms[0].query_samples[0].routed.distances[0] = f32::NAN;
        assert!(validate_d1_report(&non_finite).is_err());

        let mut projection_drift = canonical.clone();
        projection_drift.arms[0].four_page_projected_bytes += 1;
        assert!(validate_d1_report(&projection_drift).is_err());

        let mut oversized_projection = canonical.clone();
        oversized_projection.maximum_record_id_bytes = 64;
        oversized_projection.arms[0].four_page_projected_bytes =
            4 * (96 + 4 * 2_049 + 2_048 * (64 + 64));
        oversized_projection.arms[0].passed = false;
        validate_d1_report(&oversized_projection).unwrap();
        oversized_projection.arms[0].passed = true;
        assert!(validate_d1_report(&oversized_projection).is_err());

        let mut duplicate_source_query = canonical.clone();
        duplicate_source_query.query_ordinals[31] = 30;
        assert!(validate_d1_report(&duplicate_source_query).is_err());

        let mut scalar_identity_drift = canonical.clone();
        scalar_identity_drift.arms[0].query_samples[0]
            .scalar_oracle
            .ids[0] = vec![b'z'];
        assert!(validate_d1_report(&scalar_identity_drift).is_err());

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
        report.arms[0].quantizer_state = state;
        report.arms[0].quantizer_checksum = checksum;
        report.arms[0].four_page_projected_bytes = 4 * (96 + 4 * 2_049 + 2_048 * (52 + 32));
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
        report.arms[0].key = V23D1ArmKey {
            family: V23QuantizerFamily::SrhtPq,
            code_width_bytes: 8,
        };
        report.arms[0].quantizer_checksum = blake3::hash(&serde_json::to_vec(&state).unwrap())
            .to_hex()
            .to_string();
        report.arms[0].quantizer_state = state;
        report.arms[0].four_page_projected_bytes = 4 * (96 + 4 * 2_049 + 2_048 * (8 + 32));
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
        assert!(serialized_arm.get("projected_root_bytes").is_some());
        assert!(serialized_arm.get("root_bytes").is_none());
        assert!(serialized_arm.get("projected_build_bytes").is_some());
        assert!(serialized_arm.get("build_peak_rss_bytes").is_none());
        let projected_pages = 100_000_u64;
        let projected_root_bytes = 96 + projected_pages * (96 + 4 * 4);
        let projected_catalog_bytes = projected_pages * (32 + 4 * 4);
        let projected_router_bytes = projected_pages * 4_096;
        let projected_ram_bytes = projected_root_bytes
            + projected_catalog_bytes
            + projected_router_bytes
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
                centroid: vec![0.0; 4],
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

        let mut ram_overflow = canonical.clone();
        ram_overflow.arms[0].pages[0].dimensions = 10_000;
        ram_overflow.arms[0].pages[0].centroid = vec![0.0; 10_000];
        let (projected_root_bytes, projected_ram_bytes) = v23_d2_projected_memory(
            ram_overflow.arms[0].unique_rows,
            ram_overflow.arms[0].pages.len(),
            10_000,
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
            10_000,
            ram_overflow.arms[0].maximum_record_id_bytes,
            ram_overflow.arms[0].d1_key.code_width_bytes,
            ram_overflow.arms[0].maximum_assignments_per_row,
        )
        .unwrap();
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
    fn v23_d2_within_gate_cpu_jitter_does_not_change_frontier_ordering() {
        let canonical = canonical_d2_report();
        let mut report = canonical.clone();
        let mut jittered = canonical.arms[0].clone();
        jittered.primary_target_rows = 512;
        jittered
            .query_samples
            .iter_mut()
            .for_each(|sample| sample.cpu_ns += 1);
        jittered.cpu_p99_ns += 1;
        report.arms = vec![jittered, canonical.arms[0].clone()];
        validate_d2_report(&report).unwrap();
    }

    #[test]
    fn v23_d2_frontier_never_discards_a_passing_arm_for_failing_recall_leaders() {
        let canonical = canonical_d2_report().arms.remove(0);
        let mut passing = canonical.clone();
        passing.primary_target_rows = 2_048;
        passing.aggregate_recall_ppm = 978_000;

        let mut evaluated = vec![passing.clone()];
        for (target, recall) in [(512_u16, 990_000_u64), (1_024, 985_000), (2_048, 980_000)] {
            let mut failing = canonical.clone();
            failing.primary_target_rows = target;
            failing.maximum_assignments_per_row = 2;
            failing.aggregate_recall_ppm = recall;
            failing.cpu_p99_ns = super::V23_D1_CPU_MAX_NS + 1;
            failing.passed = false;
            evaluated.push(failing);
        }

        let frontier = select_v23_d2_frontier(&evaluated).unwrap();
        assert_eq!(frontier, vec![passing]);
    }

    #[test]
    fn v23_d2_builder_projection_covers_replica_and_index_transients() {
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
        let index_vectors = rows * 6 * std::mem::size_of::<usize>() as u64;
        assert!(projected >= decoded_rows + replica_candidates + index_vectors);
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
            centroid: vec![0.5; 4],
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
            (4_usize, 2_u8),
            (6, 2),
            (7, 16),
            (8, 5),
            (12, 4),
            (16, 1),
            (20, 0),
            (24, 1),
            (28, 1),
            (32, 9),
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
        bad_reserved_header[64] ^= 1;
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
            centroid: vec![0.0; 2],
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
            (V23QuantizerFamily::SrhtPq, 8_u8),
            (V23QuantizerFamily::SrhtPq, 16_u8),
            (V23QuantizerFamily::FastTurboQuantMse, 8_u8),
            (V23QuantizerFamily::FastTurboQuantProd, 16_u8),
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
}
