use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io::Cursor,
    sync::Arc,
};

use crate::{
    BorsukError, Result, V35ArtifactIdentity, V35ProjectedQuery, V35RemoteDirectoryBinding,
    V35RoutePrefix, simd_control::f32x8, v35_route::v35_artifact_authority_digest,
};
use arrow_array::{
    Array, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, RecordBatch, StringArray,
    UInt32Array, UInt64Array,
};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use half::f16;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MIB: u64 = 1_048_576;
const MAX_ENCODED_CHUNK_BYTES: u64 = MIB;
const MAX_DECODED_CHUNK_BYTES: u64 = 2 * MIB;
const MAX_QUERY_WORKSPACE_BYTES: u64 = 32 * MIB;
const MAX_RETRIES: u8 = 2;
const MAX_CODE_GETS: u64 = 24;
const DISPATCH_WINDOW: usize = 4;
const MAX_CANDIDATES: usize = 12_288;
const MAX_MUTATION_ENTRIES: usize = 1_000_000;
const MAX_DIRECTORY_BLOCK_BYTES: u64 = MIB;
const MAX_DIRECTORY_CHUNKS: usize = 64;
const DIRECTORY_FORMAT: &str = "borsuk-v35-remote-directory-block-v1";
const CODE_FORMAT: &str = "borsuk-v35-remote-code-arrow-v1";
const CODE_MANIFEST_KEY: &str = "borsuk.v35.remote-code.manifest";
const PAGE_FORMAT: &str = "borsuk-v35-exact-page-parquet-v1";
const PAGE_MANIFEST_KEY: &str = "borsuk.v35.exact-page.manifest";

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn digest_hex(value: [u8; 32]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Logical identity of the one incompatible V35 Arrow code schema.
pub fn v35_remote_code_schema_digest() -> [u8; 32] {
    Sha256::digest(
        b"borsuk-v35-remote-code-schema-v1\nid:u64\nsequence:u64\nprimary_page:u32\nreplica_page:u32?\ncode:fixed-size-binary\n",
    )
    .into()
}

fn validate_object(identity: &V35ArtifactIdentity) -> Result<()> {
    if identity.role != "remote-code-object"
        || identity.digest_algorithm != "sha256"
        || !is_digest(&identity.digest)
        || identity.length == 0
        || !identity.uri.starts_with("s3://")
        || identity.uri.contains("/corpus/")
    {
        return Err(invalid("V35 remote code object authority differs"));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Latest visible sequence and live/tombstone state for one vector ID.
pub struct V35SnapshotEntry {
    id: u64,
    sequence: u64,
    live: bool,
}

impl V35SnapshotEntry {
    /// Construct one nonzero-sequence snapshot entry.
    pub fn new(id: u64, sequence: u64, live: bool) -> Result<Self> {
        if sequence == 0 {
            return Err(invalid("V35 snapshot sequence differs"));
        }
        Ok(Self { id, sequence, live })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Complete sorted visibility authority pinned for one query.
pub struct V35SnapshotVisibility {
    digest: [u8; 32],
    entries: Vec<V35SnapshotEntry>,
}

impl V35SnapshotVisibility {
    /// Construct a strict ID-ordered visibility snapshot.
    pub fn new(digest: [u8; 32], entries: Vec<V35SnapshotEntry>) -> Result<Self> {
        if digest == [0; 32]
            || entries.len() > MAX_MUTATION_ENTRIES
            || entries.windows(2).any(|pair| pair[0].id >= pair[1].id)
        {
            return Err(invalid("V35 snapshot visibility authority differs"));
        }
        Ok(Self { digest, entries })
    }

    fn admits(&self, id: u64, sequence: u64) -> bool {
        self.entries
            .binary_search_by_key(&id, |entry| entry.id)
            .map_or(true, |position| {
                let entry = self.entries[position];
                entry.live && entry.sequence == sequence
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
/// One decoded approximate-distance row before bounded heap reduction.
pub struct V35ScannedCandidate {
    distance: f64,
    row_ordinal: u64,
    id: u64,
    sequence: u64,
    primary_page: u32,
    replica_page: Option<u32>,
}

impl V35ScannedCandidate {
    /// Construct one finite candidate with distinct optional page references.
    pub fn new(
        distance: f64,
        row_ordinal: u64,
        id: u64,
        sequence: u64,
        primary_page: u32,
        replica_page: Option<u32>,
    ) -> Result<Self> {
        if !distance.is_finite() || sequence == 0 || replica_page == Some(primary_page) {
            return Err(invalid("V35 scanned candidate differs"));
        }
        Ok(Self {
            distance,
            row_ordinal,
            id,
            sequence,
            primary_page,
            replica_page,
        })
    }
    /// Approximate full-source SQ distance.
    pub fn distance(self) -> f64 {
        self.distance
    }
    /// Stable source row ordinal.
    pub fn row_ordinal(self) -> u64 {
        self.row_ordinal
    }
    /// Vector identifier.
    pub fn id(self) -> u64 {
        self.id
    }
    /// Visible sequence number.
    pub fn sequence(self) -> u64 {
        self.sequence
    }
}

fn candidate_order(left: &V35ScannedCandidate, right: &V35ScannedCandidate) -> std::cmp::Ordering {
    left.distance
        .total_cmp(&right.distance)
        .then_with(|| left.row_ordinal.cmp(&right.row_ordinal))
        .then_with(|| left.id.cmp(&right.id))
}

fn heap_swap(
    heap: &mut [V35ScannedCandidate],
    positions: &mut HashMap<u64, usize>,
    left: usize,
    right: usize,
) {
    heap.swap(left, right);
    positions.insert(heap[left].id, left);
    positions.insert(heap[right].id, right);
}

fn heap_sift_up(
    heap: &mut [V35ScannedCandidate],
    positions: &mut HashMap<u64, usize>,
    mut position: usize,
) {
    while position > 0 {
        let parent = (position - 1) / 2;
        if !candidate_order(&heap[position], &heap[parent]).is_gt() {
            break;
        }
        heap_swap(heap, positions, position, parent);
        position = parent;
    }
}

fn heap_sift_down(
    heap: &mut [V35ScannedCandidate],
    positions: &mut HashMap<u64, usize>,
    mut position: usize,
) {
    loop {
        let left = 2 * position + 1;
        if left >= heap.len() {
            break;
        }
        let right = left + 1;
        let child = if right < heap.len() && candidate_order(&heap[right], &heap[left]).is_gt() {
            right
        } else {
            left
        };
        if !candidate_order(&heap[child], &heap[position]).is_gt() {
            break;
        }
        heap_swap(heap, positions, position, child);
        position = child;
    }
}

/// Filter snapshot visibility before keeping the exact bounded candidate prefix.
pub fn reduce_v35_scanned_candidates(
    scanned: &[V35ScannedCandidate],
    visibility: &V35SnapshotVisibility,
) -> Result<Vec<V35ScannedCandidate>> {
    let mut heap = Vec::<V35ScannedCandidate>::with_capacity(MAX_CANDIDATES);
    let mut positions = HashMap::<u64, usize>::with_capacity(MAX_CANDIDATES);
    for candidate in scanned.iter().copied() {
        admit_candidate(&mut heap, &mut positions, visibility, candidate);
    }
    heap.sort_by(candidate_order);
    Ok(heap)
}

fn admit_candidate(
    heap: &mut Vec<V35ScannedCandidate>,
    positions: &mut HashMap<u64, usize>,
    visibility: &V35SnapshotVisibility,
    candidate: V35ScannedCandidate,
) {
    if !visibility.admits(candidate.id, candidate.sequence) {
        return;
    }
    if let Some(position) = positions.get(&candidate.id).copied() {
        if !candidate_order(&candidate, &heap[position]).is_lt() {
            return;
        }
        heap[position] = candidate;
        heap_sift_down(heap, positions, position);
    } else if heap.len() < MAX_CANDIDATES {
        let position = heap.len();
        heap.push(candidate);
        positions.insert(candidate.id, position);
        heap_sift_up(heap, positions, position);
    } else if candidate_order(&candidate, &heap[0]).is_lt() {
        positions.remove(&heap[0].id);
        heap[0] = candidate;
        positions.insert(candidate.id, 0);
        heap_sift_down(heap, positions, 0);
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Sorted bounded candidates carrying the query/generation/snapshot authority used to scan them.
pub struct V35CandidateSet {
    candidates: Vec<V35ScannedCandidate>,
    generation_digest: [u8; 32],
    query_digest: [u8; 32],
    snapshot_digest: [u8; 32],
}

impl V35CandidateSet {
    /// Candidate rows ordered by approximate distance and source ordinal.
    pub fn candidates(&self) -> &[V35ScannedCandidate] {
        &self.candidates
    }
    /// Routing generation used by the scan.
    pub fn generation_digest(&self) -> [u8; 32] {
        self.generation_digest
    }
    /// Query identity used by the scan.
    pub fn query_digest(&self) -> [u8; 32] {
        self.query_digest
    }
    /// Visibility snapshot used before heap admission.
    pub fn snapshot_digest(&self) -> [u8; 32] {
        self.snapshot_digest
    }
}

/// One streaming bounded candidate heap pinned to the plan's visibility snapshot.
pub struct V35CandidateAccumulator<'a> {
    visibility: &'a V35SnapshotVisibility,
    heap: Vec<V35ScannedCandidate>,
    positions: HashMap<u64, usize>,
    generation_digest: [u8; 32],
    query_digest: [u8; 32],
}

impl<'a> V35CandidateAccumulator<'a> {
    /// Admit a streaming accumulator only under the exact plan snapshot.
    pub fn new(plan: &V35RemotePlan, visibility: &'a V35SnapshotVisibility) -> Result<Self> {
        if visibility.digest != plan.directory_binding.snapshot_digest {
            return Err(invalid("V35 candidate visibility snapshot differs"));
        }
        Ok(Self {
            visibility,
            heap: Vec::with_capacity(MAX_CANDIDATES),
            positions: HashMap::with_capacity(MAX_CANDIDATES),
            generation_digest: plan.generation_digest,
            query_digest: plan.query_digest,
        })
    }

    /// Filter and admit one decoded candidate without materializing a scanned-row vector.
    pub fn admit(&mut self, candidate: V35ScannedCandidate) {
        admit_candidate(
            &mut self.heap,
            &mut self.positions,
            self.visibility,
            candidate,
        );
    }

    /// Seal the deterministic sorted candidate set and retain all authority bindings.
    pub fn finish(mut self) -> V35CandidateSet {
        self.heap.sort_by(candidate_order);
        V35CandidateSet {
            candidates: self.heap,
            generation_digest: self.generation_digest,
            query_digest: self.query_digest,
            snapshot_digest: self.visibility.digest,
        }
    }
}

/// Select at most eight coverage-greedy exact primary pages in candidate order.
pub fn select_v35_exact_pages(candidates: &[V35ScannedCandidate]) -> Vec<u32> {
    let mut selected = BTreeSet::new();
    let mut pages = Vec::with_capacity(8);
    for candidate in candidates {
        if selected.contains(&candidate.primary_page)
            || candidate
                .replica_page
                .is_some_and(|page| selected.contains(&page))
        {
            continue;
        }
        selected.insert(candidate.primary_page);
        pages.push(candidate.primary_page);
        if pages.len() == 8 {
            break;
        }
    }
    pages
}

#[derive(Debug, Clone, PartialEq)]
/// Per-group affine residual scalar-quantization descriptor and packed rows.
pub struct V35ResidualSqDescriptor {
    rows: u64,
    dimensions: u32,
    bits_per_dimension: u8,
    bytes_per_row: u32,
    center_f16_bits: Vec<u16>,
    scales: Vec<f32>,
    codes: Vec<u8>,
}

impl V35ResidualSqDescriptor {
    /// Source rows encoded by this descriptor.
    pub fn rows(&self) -> u64 {
        self.rows
    }
    /// Full source dimension retained remotely.
    pub fn dimensions(&self) -> u32 {
        self.dimensions
    }
    /// Packed bytes for one row, including odd SQ4 padding.
    pub fn bytes_per_row(&self) -> u32 {
        self.bytes_per_row
    }
    /// Stored scalar-quantization bits per source dimension.
    pub fn bits_per_dimension(&self) -> u8 {
        self.bits_per_dimension
    }
    /// Source-order f16 center bit patterns.
    pub fn center_f16_bits(&self) -> &[u16] {
        &self.center_f16_bits
    }
    /// Source-order decoded f32 affine scales.
    pub fn scales(&self) -> &[f32] {
        &self.scales
    }
    /// Packed source-order row codes.
    pub fn codes(&self) -> &[u8] {
        &self.codes
    }
}

/// Reusable allocation-free SIMD scorer for one bounded residual-SQ group.
pub struct V35ResidualSqScorer<'a> {
    descriptor: &'a V35ResidualSqDescriptor,
    query: &'a [f32],
    levels: f32,
}

impl<'a> V35ResidualSqScorer<'a> {
    /// Bind one full-source query to one validated SQ4/SQ8 descriptor.
    pub fn new(descriptor: &'a V35ResidualSqDescriptor, query: &'a [f32]) -> Result<Self> {
        let dimensions = usize::try_from(descriptor.dimensions)
            .map_err(|_| invalid("V35 residual SQ dimensions overflow"))?;
        let width = usize::try_from(descriptor.bytes_per_row)
            .map_err(|_| invalid("V35 residual SQ row bytes overflow"))?;
        let rows = usize::try_from(descriptor.rows)
            .map_err(|_| invalid("V35 residual SQ rows overflow"))?;
        if query.len() != dimensions
            || query.iter().any(|value| !value.is_finite())
            || descriptor.center_f16_bits.len() != dimensions
            || descriptor.scales.len() != dimensions
            || descriptor
                .scales
                .iter()
                .any(|scale| !scale.is_finite() || *scale < 0.0)
            || descriptor.codes.len() != rows.saturating_mul(width)
            || !matches!(descriptor.bits_per_dimension, 4 | 8)
        {
            return Err(invalid("V35 residual SQ scorer authority differs"));
        }
        Ok(Self {
            descriptor,
            query,
            levels: if descriptor.bits_per_dimension == 4 {
                15.0
            } else {
                255.0
            },
        })
    }

    fn decoded(&self, codes: &[u8], dimension: usize) -> f32 {
        let code = if self.descriptor.bits_per_dimension == 8 {
            codes[dimension]
        } else if dimension.is_multiple_of(2) {
            codes[dimension / 2] >> 4
        } else {
            codes[dimension / 2] & 0x0f
        };
        let center = f16::from_bits(self.descriptor.center_f16_bits[dimension]).to_f32();
        let scale = self.descriptor.scales[dimension];
        center - self.levels * scale / 2.0 + f32::from(code) * scale
    }

    /// Score one row with eight source dimensions per SIMD step and a scalar tail.
    pub fn score(&self, row: u64) -> Result<f32> {
        if row >= self.descriptor.rows {
            return Err(invalid("V35 residual SQ scorer row differs"));
        }
        let width = self.descriptor.bytes_per_row as usize;
        let start = usize::try_from(row)
            .ok()
            .and_then(|row| row.checked_mul(width))
            .ok_or_else(|| invalid("V35 residual SQ scorer row overflows"))?;
        let codes = self
            .descriptor
            .codes
            .get(start..start + width)
            .ok_or_else(|| invalid("V35 residual SQ scorer code extent differs"))?;
        let dimensions = self.query.len();
        let bulk = dimensions / 8 * 8;
        let mut accumulator = f32x8::ZERO;
        for base in (0..bulk).step_by(8) {
            let decoded = std::array::from_fn(|lane| self.decoded(codes, base + lane));
            let query = std::array::from_fn(|lane| self.query[base + lane]);
            let delta = f32x8::from(query) - f32x8::from(decoded);
            accumulator += delta * delta;
        }
        let mut score = accumulator.reduce_add();
        for dimension in bulk..dimensions {
            let delta = self.query[dimension] - self.decoded(codes, dimension);
            score = delta.mul_add(delta, score);
        }
        if !score.is_finite() {
            return Err(invalid("V35 residual SQ score is nonfinite"));
        }
        Ok(if score == 0.0 { 0.0 } else { score })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Immutable identity, visibility sequence, and exact-page references for one code row.
pub struct V35RemoteCodeRow {
    id: u64,
    sequence: u64,
    primary_page: u32,
    replica_page: Option<u32>,
}

impl V35RemoteCodeRow {
    /// Construct one row; a replica must differ from its primary page.
    pub fn new(
        id: u64,
        sequence: u64,
        primary_page: u32,
        replica_page: Option<u32>,
    ) -> Result<Self> {
        if sequence == 0 || replica_page == Some(primary_page) {
            return Err(invalid("V35 remote code row authority differs"));
        }
        Ok(Self {
            id,
            sequence,
            primary_page,
            replica_page,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V35RemoteCodeManifest {
    bits_per_dimension: u8,
    bytes_per_row: u32,
    center_f16_bits: Vec<u16>,
    code_schema_sha256: String,
    dimensions: u32,
    format: String,
    group_ordinal: u32,
    logical_start: u64,
    rows: u64,
    scales_f32_bits: Vec<u32>,
}

fn code_manifest_json(manifest: &V35RemoteCodeManifest) -> Result<String> {
    serde_json::to_string(manifest)
        .map_err(|_| invalid("V35 remote code manifest cannot be serialized"))
}

fn remote_code_schema(bytes_per_row: u32, manifest_json: String) -> Result<Arc<Schema>> {
    let width =
        i32::try_from(bytes_per_row).map_err(|_| invalid("V35 remote code row width overflows"))?;
    Ok(Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("sequence", DataType::UInt64, false),
            Field::new("primary_page", DataType::UInt32, false),
            Field::new("replica_page", DataType::UInt32, true),
            Field::new("code", DataType::FixedSizeBinary(width), false),
        ],
        HashMap::from([(CODE_MANIFEST_KEY.to_owned(), manifest_json)]),
    )))
}

fn projected_remote_code_decoded_bytes(
    rows: u64,
    dimensions: u32,
    bytes_per_row: u32,
) -> Result<u64> {
    let row_bytes = u64::from(bytes_per_row)
        .checked_add(24)
        .ok_or_else(|| invalid("V35 remote decoded row bytes overflow"))?;
    rows.checked_mul(row_bytes)
        .and_then(|bytes| bytes.checked_add(u64::from(dimensions) * 6))
        .and_then(|bytes| bytes.checked_add(65_536))
        .ok_or_else(|| invalid("V35 remote decoded bytes overflow"))
}

fn validate_remote_code_descriptor(descriptor: &V35ResidualSqDescriptor) -> Result<usize> {
    let dimensions = usize::try_from(descriptor.dimensions)
        .map_err(|_| invalid("V35 remote code dimensions overflow"))?;
    let width = usize::try_from(descriptor.bytes_per_row)
        .map_err(|_| invalid("V35 remote code row width overflows"))?;
    let expected_width = match descriptor.bits_per_dimension {
        4 => dimensions.div_ceil(2),
        8 => dimensions,
        _ => return Err(invalid("V35 remote code quantization differs")),
    };
    if dimensions == 0
        || width != expected_width
        || descriptor.center_f16_bits.len() != dimensions
        || descriptor.scales.len() != dimensions
        || descriptor
            .center_f16_bits
            .iter()
            .any(|bits| !f16::from_bits(*bits).to_f32().is_finite())
        || descriptor
            .scales
            .iter()
            .any(|scale| !scale.is_finite() || *scale < 0.0)
    {
        return Err(invalid("V35 remote code descriptor authority differs"));
    }
    Ok(width)
}

/// Encode one independently decodable, uncompressed Arrow code chunk.
pub fn encode_v35_remote_code_arrow(
    group_ordinal: u32,
    logical_start: u64,
    descriptor: &V35ResidualSqDescriptor,
    rows: &[V35RemoteCodeRow],
) -> Result<(Vec<u8>, u64)> {
    let row_count =
        u64::try_from(rows.len()).map_err(|_| invalid("V35 remote code rows overflow"))?;
    let width = validate_remote_code_descriptor(descriptor)?;
    let encoded_code_bytes = rows
        .len()
        .checked_mul(width)
        .ok_or_else(|| invalid("V35 remote code extent overflows"))?;
    if rows.is_empty()
        || descriptor.rows != row_count
        || descriptor.codes.len() != encoded_code_bytes
        || rows.windows(2).any(|pair| pair[0].id >= pair[1].id)
        || logical_start.checked_add(row_count).is_none()
    {
        return Err(invalid("V35 remote code chunk authority differs"));
    }
    let manifest = V35RemoteCodeManifest {
        bits_per_dimension: descriptor.bits_per_dimension,
        bytes_per_row: descriptor.bytes_per_row,
        center_f16_bits: descriptor.center_f16_bits.clone(),
        code_schema_sha256: digest_hex(v35_remote_code_schema_digest()),
        dimensions: descriptor.dimensions,
        format: CODE_FORMAT.to_owned(),
        group_ordinal,
        logical_start,
        rows: row_count,
        scales_f32_bits: descriptor
            .scales
            .iter()
            .map(|scale| scale.to_bits())
            .collect(),
    };
    let schema = remote_code_schema(descriptor.bytes_per_row, code_manifest_json(&manifest)?)?;
    let codes = FixedSizeBinaryArray::try_from_iter(descriptor.codes.chunks_exact(width))?;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.sequence).collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                rows.iter().map(|row| row.primary_page).collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                rows.iter().map(|row| row.replica_page).collect::<Vec<_>>(),
            )),
            Arc::new(codes),
        ],
    )?;
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut bytes = Vec::new();
    let mut writer = FileWriter::try_new_with_options(&mut bytes, schema.as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    if bytes.len() as u64 > MAX_ENCODED_CHUNK_BYTES {
        return Err(invalid("V35 remote code chunk exceeds encoded admission"));
    }
    let decoded = projected_remote_code_decoded_bytes(
        row_count,
        descriptor.dimensions,
        descriptor.bytes_per_row,
    )?;
    if decoded > MAX_DECODED_CHUNK_BYTES {
        return Err(invalid("V35 remote code chunk exceeds decoded admission"));
    }
    Ok((bytes, decoded))
}

fn parse_remote_code_manifest(
    chunk: &V35RemoteChunk,
    bytes: &[u8],
) -> Result<(V35ResidualSqDescriptor, Vec<V35RemoteCodeRow>)> {
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.num_batches() != 1 {
        return Err(invalid("V35 remote code Arrow batch count differs"));
    }
    let schema = reader.schema();
    let metadata = schema.metadata();
    if metadata.len() != 1 {
        return Err(invalid("V35 remote code metadata differs"));
    }
    let manifest_json = metadata
        .get(CODE_MANIFEST_KEY)
        .ok_or_else(|| invalid("V35 remote code manifest is missing"))?;
    let manifest: V35RemoteCodeManifest = serde_json::from_str(manifest_json)
        .map_err(|_| invalid("V35 remote code manifest differs"))?;
    if code_manifest_json(&manifest)? != *manifest_json
        || manifest.format != CODE_FORMAT
        || manifest.code_schema_sha256 != digest_hex(v35_remote_code_schema_digest())
        || manifest.group_ordinal != chunk.group_ordinal
        || manifest.logical_start != chunk.logical_start
        || manifest.rows != chunk.rows
    {
        return Err(invalid("V35 remote code manifest authority differs"));
    }
    let scales = manifest
        .scales_f32_bits
        .iter()
        .map(|bits| f32::from_bits(*bits))
        .collect::<Vec<_>>();
    let descriptor_without_codes = V35ResidualSqDescriptor {
        rows: manifest.rows,
        dimensions: manifest.dimensions,
        bits_per_dimension: manifest.bits_per_dimension,
        bytes_per_row: manifest.bytes_per_row,
        center_f16_bits: manifest.center_f16_bits,
        scales,
        codes: Vec::new(),
    };
    let width = validate_remote_code_descriptor(&descriptor_without_codes)?;
    let projected = projected_remote_code_decoded_bytes(
        manifest.rows,
        manifest.dimensions,
        manifest.bytes_per_row,
    )?;
    let expected_schema = remote_code_schema(manifest.bytes_per_row, manifest_json.clone())?;
    if projected != chunk.decoded_length || schema.as_ref() != expected_schema.as_ref() {
        return Err(invalid("V35 remote code Arrow schema differs"));
    }
    let batch = reader
        .next()
        .transpose()?
        .ok_or_else(|| invalid("V35 remote code Arrow batch is missing"))?;
    if reader.next().is_some() || batch.num_rows() as u64 != manifest.rows {
        return Err(invalid("V35 remote code Arrow rows differ"));
    }
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V35 remote code id column differs"))?;
    let sequences = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V35 remote code sequence column differs"))?;
    let primary_pages = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| invalid("V35 remote code primary-page column differs"))?;
    let replica_pages = batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| invalid("V35 remote code replica-page column differs"))?;
    let codes = batch
        .column(4)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| invalid("V35 remote code payload column differs"))?;
    if ids.null_count() != 0
        || sequences.null_count() != 0
        || primary_pages.null_count() != 0
        || codes.null_count() != 0
        || codes.value_length() != i32::try_from(width).unwrap_or(-1)
    {
        return Err(invalid("V35 remote code Arrow nullability differs"));
    }
    let mut rows = Vec::with_capacity(batch.num_rows());
    let mut packed_codes = Vec::with_capacity(
        batch
            .num_rows()
            .checked_mul(width)
            .ok_or_else(|| invalid("V35 remote code allocation overflows"))?,
    );
    for row in 0..batch.num_rows() {
        rows.push(V35RemoteCodeRow::new(
            ids.value(row),
            sequences.value(row),
            primary_pages.value(row),
            (!replica_pages.is_null(row)).then(|| replica_pages.value(row)),
        )?);
        packed_codes.extend_from_slice(codes.value(row));
    }
    if rows.windows(2).any(|pair| pair[0].id >= pair[1].id) {
        return Err(invalid("V35 remote code row order differs"));
    }
    Ok((
        V35ResidualSqDescriptor {
            codes: packed_codes,
            ..descriptor_without_codes
        },
        rows,
    ))
}

/// Execute fixed-format authenticated Arrow code ranges into a bounded candidate heap.
pub fn scan_v35_code_ranges<T: V35VersionedRangeTransport>(
    plan: &V35RemotePlan,
    query: &V35ProjectedQuery,
    visibility: &V35SnapshotVisibility,
    transport: &mut T,
) -> std::result::Result<(V35CandidateSet, V35RemoteReadReceipt), V35RemoteExecutionFailure> {
    if plan.query_digest != query.source_digest()
        || plan.directory_binding.code_schema_digest != v35_remote_code_schema_digest()
    {
        return Err(execution_failure(
            V35RemoteFailureKind::Authority,
            V35RemoteReadReceipt::default(),
        ));
    }
    let mut candidates = V35CandidateAccumulator::new(plan, visibility).map_err(|_| {
        execution_failure(
            V35RemoteFailureKind::Authority,
            V35RemoteReadReceipt::default(),
        )
    })?;
    let receipt = execute_v35_remote_plan(plan, transport, |chunk, bytes| {
        let (descriptor, rows) = parse_remote_code_manifest(chunk, bytes)?;
        let scorer = V35ResidualSqScorer::new(&descriptor, query.source_query())?;
        for (row, authority) in rows.iter().enumerate() {
            let row_ordinal = chunk
                .logical_start
                .checked_add(row as u64)
                .ok_or_else(|| invalid("V35 remote code row ordinal overflows"))?;
            candidates.admit(V35ScannedCandidate::new(
                f64::from(scorer.score(row as u64)?),
                row_ordinal,
                authority.id,
                authority.sequence,
                authority.primary_page,
                authority.replica_page,
            )?);
        }
        Ok(chunk.decoded_length)
    })?;
    Ok((candidates.finish(), receipt))
}

#[derive(Debug, Clone, PartialEq)]
/// One immutable exact-vector page row.
pub struct V35ExactPageRow {
    id: u64,
    sequence: u64,
    vector: Vec<f32>,
}

impl V35ExactPageRow {
    /// Construct one finite, non-empty exact row.
    pub fn new(id: u64, sequence: u64, vector: Vec<f32>) -> Result<Self> {
        if sequence == 0 || vector.is_empty() || vector.iter().any(|value| !value.is_finite()) {
            return Err(invalid("V35 exact page row differs"));
        }
        Ok(Self {
            id,
            sequence,
            vector,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Exact immutable identity and bounds for one Parquet page.
pub struct V35ExactPageIdentity {
    page_ordinal: u32,
    generation_digest: [u8; 32],
    dimensions: u32,
    rows: u16,
    decoded_length: u64,
    object: V35ArtifactIdentity,
    version_id: String,
}

impl V35ExactPageIdentity {
    /// Exact immutable object URI.
    pub fn uri(&self) -> &str {
        &self.object.uri
    }
    /// Exact encoded object bytes expected from storage.
    pub fn encoded_bytes(&self) -> u64 {
        self.object.length
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Completed whole-page response identity returned into caller-owned storage.
pub struct V35ExactPageResponse {
    uri: String,
    version_id: String,
    returned_bytes: u64,
    complete: bool,
}

impl V35ExactPageResponse {
    /// Construct a response; reranking independently compares it to the selected page.
    pub fn new(page: &V35ExactPageIdentity, returned_bytes: u64, complete: bool) -> Self {
        Self {
            uri: page.object.uri.clone(),
            version_id: page.version_id.clone(),
            returned_bytes,
            complete,
        }
    }
}

/// Eight-wide immutable-page transport with caller-owned completion buffers.
pub trait V35ExactPageTransport {
    /// Dispatch one selected immutable page without returning its body.
    fn dispatch(
        &mut self,
        page: &V35ExactPageIdentity,
    ) -> std::result::Result<V35RemoteDispatch, V35TransportFailure>;

    /// Complete one dispatch into the exact caller-owned page buffer.
    fn complete(
        &mut self,
        dispatch: V35RemoteDispatch,
        page: &V35ExactPageIdentity,
        destination: &mut [u8],
    ) -> std::result::Result<V35ExactPageResponse, V35TransportFailure>;

    /// Cancel or drain one outstanding page request and report returned bytes.
    fn cancel(&mut self, dispatch: V35RemoteDispatch) -> u64;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V35ExactPageManifest {
    dimensions: u32,
    format: String,
    generation_sha256: String,
    page_ordinal: u32,
    rows: u16,
}

fn exact_page_manifest_json(manifest: &V35ExactPageManifest) -> Result<String> {
    serde_json::to_string(manifest)
        .map_err(|_| invalid("V35 exact page manifest cannot be serialized"))
}

fn exact_page_schema(dimensions: u32, manifest_json: String) -> Result<Arc<Schema>> {
    let width =
        i32::try_from(dimensions).map_err(|_| invalid("V35 exact page dimensions overflow"))?;
    Ok(Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("sequence", DataType::UInt64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float32, false)),
                    width,
                ),
                false,
            ),
        ],
        HashMap::from([(PAGE_MANIFEST_KEY.to_owned(), manifest_json)]),
    )))
}

fn projected_exact_page_decoded_bytes(rows: usize, dimensions: usize) -> Result<u64> {
    let row_bytes = dimensions
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(16))
        .ok_or_else(|| invalid("V35 exact page row bytes overflow"))?;
    rows.checked_mul(row_bytes)
        .and_then(|bytes| bytes.checked_add(65_536))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| invalid("V35 exact page decoded bytes overflow"))
}

/// Encode one strict, independently authenticated full-vector Parquet page.
pub fn encode_v35_exact_page_parquet(
    page_ordinal: u32,
    generation_digest: [u8; 32],
    uri: &str,
    version_id: &str,
    rows: &[V35ExactPageRow],
) -> Result<(V35ExactPageIdentity, Vec<u8>)> {
    let dimensions = rows.first().map_or(0, |row| row.vector.len());
    if rows.is_empty()
        || rows.len() > 256
        || dimensions == 0
        || generation_digest == [0; 32]
        || version_id.is_empty()
        || !uri.starts_with("s3://")
        || uri.contains("/corpus/")
        || rows.iter().any(|row| row.vector.len() != dimensions)
    {
        return Err(invalid("V35 exact page authority differs"));
    }
    let mut row_identities = BTreeSet::new();
    if rows
        .iter()
        .any(|row| !row_identities.insert((row.id, row.sequence)))
    {
        return Err(invalid("V35 exact page row is duplicated"));
    }
    let rows_u16 =
        u16::try_from(rows.len()).map_err(|_| invalid("V35 exact page rows overflow"))?;
    let dimensions_u32 =
        u32::try_from(dimensions).map_err(|_| invalid("V35 exact page dimensions overflow"))?;
    let manifest = V35ExactPageManifest {
        dimensions: dimensions_u32,
        format: PAGE_FORMAT.to_owned(),
        generation_sha256: digest_hex(generation_digest),
        page_ordinal,
        rows: rows_u16,
    };
    let schema = exact_page_schema(dimensions_u32, exact_page_manifest_json(&manifest)?)?;
    let values = rows
        .iter()
        .flat_map(|row| row.vector.iter().copied())
        .collect::<Vec<_>>();
    let vectors = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        i32::try_from(dimensions).map_err(|_| invalid("V35 exact page dimensions overflow"))?,
        Arc::new(Float32Array::from(values)),
        None,
    )?;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.sequence).collect::<Vec<_>>(),
            )),
            Arc::new(vectors),
        ],
    )?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    if bytes.is_empty() || bytes.len() as u64 > 4 * MIB {
        return Err(invalid("V35 exact page encoded admission differs"));
    }
    let decoded_length = projected_exact_page_decoded_bytes(rows.len(), dimensions)?;
    if decoded_length > 4 * MIB {
        return Err(invalid("V35 exact page decoded admission differs"));
    }
    let object = V35ArtifactIdentity {
        digest: format!("{:x}", Sha256::digest(&bytes)),
        digest_algorithm: "sha256".to_owned(),
        length: bytes.len() as u64,
        role: "exact-vector-page".to_owned(),
        uri: uri.to_owned(),
    };
    Ok((
        V35ExactPageIdentity {
            page_ordinal,
            generation_digest,
            dimensions: dimensions_u32,
            rows: rows_u16,
            decoded_length,
            object,
            version_id: version_id.to_owned(),
        },
        bytes,
    ))
}

#[derive(Debug, Clone, Copy, PartialEq)]
/// One exact full-dimensional neighbor.
pub struct V35Match {
    id: u64,
    squared_distance: f64,
}

impl V35Match {
    /// Stable vector identifier.
    pub fn id(self) -> u64 {
        self.id
    }
    /// Exact full-source squared L2 distance.
    pub fn squared_distance(self) -> f64 {
        self.squared_distance
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Bounded exact result plus decoded page work.
pub struct V35SearchResult {
    matches: Vec<V35Match>,
    pages_read: usize,
    decoded_rows: usize,
    unique_visible_rows: usize,
}

impl V35SearchResult {
    /// Exact neighbors in `(distance,id)` order.
    pub fn matches(&self) -> &[V35Match] {
        &self.matches
    }
    /// Exact authenticated pages consumed.
    pub fn pages_read(&self) -> usize {
        self.pages_read
    }
    /// Rows decoded before visibility and sequence reduction.
    pub fn decoded_rows(&self) -> usize {
        self.decoded_rows
    }
    /// Unique rows remaining after snapshot and greatest-sequence reduction.
    pub fn unique_visible_rows(&self) -> usize {
        self.unique_visible_rows
    }
}

fn exact_squared_distance(vector: &[f32], query: &[f32]) -> Result<f64> {
    if vector.len() != query.len() {
        return Err(invalid("V35 exact page vector dimension differs"));
    }
    let mut score = 0.0_f64;
    let bulk = vector.len() / 4 * 4;
    for base in (0..bulk).step_by(4) {
        let left = crate::simd_control::f64x4::from(std::array::from_fn(|lane| {
            f64::from(vector[base + lane])
        }));
        let right = crate::simd_control::f64x4::from(std::array::from_fn(|lane| {
            f64::from(query[base + lane])
        }));
        for contribution in ((left - right) * (left - right)).to_array() {
            score += contribution;
        }
    }
    for dimension in bulk..vector.len() {
        let delta = f64::from(vector[dimension]) - f64::from(query[dimension]);
        score = delta.mul_add(delta, score);
    }
    if !score.is_finite() {
        return Err(invalid("V35 exact rerank distance is nonfinite"));
    }
    Ok(if score == 0.0 { 0.0 } else { score })
}

/// Authenticate and exactly rerank the frozen first-eight candidate pages.
pub fn rerank_v35_exact_pages<T: V35ExactPageTransport>(
    plan: &V35RemotePlan,
    query: &V35ProjectedQuery,
    visibility: &V35SnapshotVisibility,
    candidates: &V35CandidateSet,
    pages: &[V35ExactPageIdentity],
    transport: &mut T,
    k: usize,
) -> Result<V35SearchResult> {
    let selected = select_v35_exact_pages(candidates.candidates());
    let dimensions = query.source_query().len();
    if plan.query_digest != query.source_digest()
        || plan.directory_binding.snapshot_digest != visibility.digest
        || candidates.generation_digest != plan.generation_digest
        || candidates.query_digest != plan.query_digest
        || candidates.snapshot_digest != visibility.digest
        || selected.is_empty()
        || pages.len() != selected.len()
        || k == 0
        || k > MAX_CANDIDATES
    {
        return Err(invalid("V35 exact rerank authority differs"));
    }
    let mut page_objects = BTreeSet::new();
    for (expected_page, identity) in selected.iter().zip(pages) {
        if identity.page_ordinal != *expected_page
            || identity.generation_digest != plan.generation_digest
            || identity.dimensions as usize != dimensions
            || identity.rows == 0
            || identity.rows > 256
            || identity.decoded_length > 4 * MIB
            || identity.object.role != "exact-vector-page"
            || identity.object.digest_algorithm != "sha256"
            || !is_digest(&identity.object.digest)
            || identity.object.length == 0
            || identity.object.length > 4 * MIB
            || identity.version_id.is_empty()
            || !identity.object.uri.starts_with("s3://")
            || identity.object.uri.contains("/corpus/")
            || !page_objects.insert((identity.object.uri.as_str(), identity.version_id.as_str()))
        {
            return Err(invalid("V35 exact page authority differs before dispatch"));
        }
    }
    let mut pending = Vec::with_capacity(pages.len());
    for page in pages {
        match transport.dispatch(page) {
            Ok(dispatch) => pending.push(dispatch),
            Err(_) => {
                for dispatch in pending.drain(..) {
                    transport.cancel(dispatch);
                }
                return Err(invalid("V35 exact page dispatch failed"));
            }
        }
    }
    let mut best = BTreeMap::<u64, (u64, f64)>::new();
    let mut decoded_rows = 0_usize;
    let mut body = Vec::new();
    for (index, (expected_page, identity)) in selected.iter().zip(pages).enumerate() {
        let encoded_bytes = usize::try_from(identity.object.length)
            .map_err(|_| invalid("V35 exact page encoded length overflows"))?;
        body.resize(encoded_bytes, 0);
        let response = match transport.complete(pending[index], identity, &mut body) {
            Ok(response) => response,
            Err(_) => {
                for dispatch in pending.iter().skip(index + 1).copied() {
                    transport.cancel(dispatch);
                }
                return Err(invalid("V35 exact page completion failed"));
            }
        };
        if identity.page_ordinal != *expected_page
            || identity.generation_digest != plan.generation_digest
            || identity.dimensions as usize != dimensions
            || identity.rows == 0
            || identity.rows > 256
            || identity.decoded_length > 4 * MIB
            || identity.object.role != "exact-vector-page"
            || identity.object.digest_algorithm != "sha256"
            || identity.object.length != body.len() as u64
            || identity.object.digest != format!("{:x}", Sha256::digest(&body))
            || identity.version_id.is_empty()
            || !identity.object.uri.starts_with("s3://")
            || identity.object.uri.contains("/corpus/")
            || response.uri != identity.object.uri
            || response.version_id != identity.version_id
            || response.returned_bytes != identity.object.length
            || !response.complete
        {
            for dispatch in pending.iter().skip(index + 1).copied() {
                transport.cancel(dispatch);
            }
            return Err(invalid("V35 exact page identity differs"));
        }
        let page_result = (|| -> Result<usize> {
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(&body))?;
            let manifest_json = builder
                .schema()
                .metadata()
                .get(PAGE_MANIFEST_KEY)
                .ok_or_else(|| invalid("V35 exact page manifest is missing"))?;
            if builder.schema().metadata().len() != 1 {
                return Err(invalid("V35 exact page metadata differs"));
            }
            let manifest: V35ExactPageManifest = serde_json::from_str(manifest_json)
                .map_err(|_| invalid("V35 exact page manifest differs"))?;
            if exact_page_manifest_json(&manifest)? != *manifest_json
                || manifest.format != PAGE_FORMAT
                || manifest.generation_sha256 != digest_hex(plan.generation_digest)
                || manifest.page_ordinal != *expected_page
                || manifest.dimensions as usize != dimensions
                || manifest.rows != identity.rows
                || builder.schema().as_ref()
                    != exact_page_schema(manifest.dimensions, manifest_json.clone())?.as_ref()
            {
                return Err(invalid("V35 exact page schema authority differs"));
            }
            let mut reader = builder.with_batch_size(256).build()?;
            let mut page_rows = 0_usize;
            for batch in &mut reader {
                let batch = batch?;
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| invalid("V35 exact page id column differs"))?;
                let sequences = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| invalid("V35 exact page sequence column differs"))?;
                let vectors = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| invalid("V35 exact page vector column differs"))?;
                let values = vectors
                    .values()
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| invalid("V35 exact page vector values differ"))?;
                if ids.null_count() != 0
                    || sequences.null_count() != 0
                    || vectors.null_count() != 0
                    || values.null_count() != 0
                    || vectors.value_length() as usize != dimensions
                {
                    return Err(invalid("V35 exact page nullability differs"));
                }
                for row in 0..batch.num_rows() {
                    let id = ids.value(row);
                    let sequence = sequences.value(row);
                    let start = row * dimensions;
                    let vector = &values.values()[start..start + dimensions];
                    if sequence == 0 || vector.iter().any(|value| !value.is_finite()) {
                        return Err(invalid("V35 exact page row authority differs"));
                    }
                    if visibility.admits(id, sequence) {
                        let distance = exact_squared_distance(vector, query.source_query())?;
                        match best.get(&id).copied() {
                            None => {
                                best.insert(id, (sequence, distance));
                            }
                            Some((old_sequence, _)) if sequence > old_sequence => {
                                best.insert(id, (sequence, distance));
                            }
                            Some((old_sequence, old_distance)) if sequence == old_sequence => {
                                if distance.to_bits() != old_distance.to_bits() {
                                    return Err(invalid("V35 exact replica vector differs"));
                                }
                            }
                            Some(_) => {}
                        }
                    }
                }
                page_rows = page_rows
                    .checked_add(batch.num_rows())
                    .ok_or_else(|| invalid("V35 exact page rows overflow"))?;
            }
            if page_rows != usize::from(identity.rows)
                || projected_exact_page_decoded_bytes(page_rows, dimensions)?
                    != identity.decoded_length
            {
                return Err(invalid("V35 exact page decoded extent differs"));
            }
            Ok(page_rows)
        })();
        let page_rows = match page_result {
            Ok(page_rows) => page_rows,
            Err(error) => {
                for dispatch in pending.iter().skip(index + 1).copied() {
                    transport.cancel(dispatch);
                }
                return Err(error);
            }
        };
        decoded_rows = decoded_rows
            .checked_add(page_rows)
            .ok_or_else(|| invalid("V35 exact rerank rows overflow"))?;
    }
    if decoded_rows > 8 * 256 {
        return Err(invalid("V35 exact rerank work differs"));
    }
    let unique_visible_rows = best.len();
    let mut matches = best
        .into_iter()
        .map(|(id, (_, squared_distance))| V35Match {
            id,
            squared_distance,
        })
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        left.squared_distance
            .total_cmp(&right.squared_distance)
            .then(left.id.cmp(&right.id))
    });
    matches.truncate(k);
    Ok(V35SearchResult {
        matches,
        pages_read: pages.len(),
        decoded_rows,
        unique_visible_rows,
    })
}

/// Build an exact per-group SQ4 or SQ8 descriptor from a bounded row group.
pub fn build_v35_residual_sq_descriptor(
    rows: &[Vec<f32>],
    bits_per_dimension: u8,
) -> Result<V35ResidualSqDescriptor> {
    let dimensions = rows.first().map_or(0, Vec::len);
    if rows.is_empty()
        || dimensions == 0
        || !matches!(bits_per_dimension, 4 | 8)
        || rows
            .iter()
            .any(|row| row.len() != dimensions || row.iter().any(|value| !value.is_finite()))
    {
        return Err(invalid("V35 residual SQ source differs"));
    }
    let row_count = rows.len() as f64;
    let mut center_f16_bits = Vec::with_capacity(dimensions);
    let mut centers = Vec::with_capacity(dimensions);
    let mut scales = Vec::with_capacity(dimensions);
    let levels = (1_u16 << bits_per_dimension) - 1;
    for dimension in 0..dimensions {
        let mean = rows
            .iter()
            .fold(0.0_f64, |sum, row| sum + f64::from(row[dimension]))
            / row_count;
        let center = f16::from_f64(mean);
        let decoded_center = f64::from(center.to_f32());
        if !decoded_center.is_finite() {
            return Err(invalid("V35 residual SQ f16 center differs"));
        }
        let variance = rows.iter().fold(0.0_f64, |sum, row| {
            let residual = f64::from(row[dimension]) - decoded_center;
            residual.mul_add(residual, sum)
        }) / row_count;
        let sigma = variance.sqrt();
        let scale = if sigma == 0.0 {
            0.0
        } else {
            (8.0 * sigma / f64::from(levels)) as f32
        };
        if !scale.is_finite() || (sigma > 0.0 && scale == 0.0) {
            return Err(invalid("V35 residual SQ scale differs"));
        }
        center_f16_bits.push(center.to_bits());
        centers.push(decoded_center);
        scales.push(scale);
    }

    let bytes_per_row = match bits_per_dimension {
        4 => dimensions.div_ceil(2),
        8 => dimensions,
        _ => unreachable!(),
    };
    let capacity = rows
        .len()
        .checked_mul(bytes_per_row)
        .ok_or_else(|| invalid("V35 residual SQ payload overflows"))?;
    let mut codes = Vec::with_capacity(capacity);
    for row in rows {
        if bits_per_dimension == 8 {
            for dimension in 0..dimensions {
                codes.push(quantize_residual(
                    row[dimension],
                    centers[dimension],
                    scales[dimension],
                    levels,
                ));
            }
        } else {
            for dimension in (0..dimensions).step_by(2) {
                let high = quantize_residual(
                    row[dimension],
                    centers[dimension],
                    scales[dimension],
                    levels,
                );
                let low = if dimension + 1 < dimensions {
                    quantize_residual(
                        row[dimension + 1],
                        centers[dimension + 1],
                        scales[dimension + 1],
                        levels,
                    )
                } else {
                    0
                };
                codes.push((high << 4) | low);
            }
        }
    }
    Ok(V35ResidualSqDescriptor {
        rows: u64::try_from(rows.len()).map_err(|_| invalid("V35 residual SQ rows overflow"))?,
        dimensions: u32::try_from(dimensions)
            .map_err(|_| invalid("V35 residual SQ dimensions overflow"))?,
        bits_per_dimension,
        bytes_per_row: u32::try_from(bytes_per_row)
            .map_err(|_| invalid("V35 residual SQ row bytes overflow"))?,
        center_f16_bits,
        scales,
        codes,
    })
}

fn quantize_residual(value: f32, center: f64, scale: f32, levels: u16) -> u8 {
    if scale == 0.0 {
        return 0;
    }
    let scale = f64::from(scale);
    let lower = center - f64::from(levels) * scale / 2.0;
    ((f64::from(value) - lower) / scale)
        .round_ties_even()
        .clamp(0.0, f64::from(levels)) as u8
}

#[cfg(test)]
mod tests {
    use super::quantize_residual;

    #[test]
    fn v35_remote_sq_quantization_uses_ties_to_even() {
        // Break caught: `round` uses ties away from zero and makes the code
        // stream depend on an unregistered rounding convention.
        assert_eq!(quantize_residual(-7.0, 0.0, 1.0, 15), 0);
        assert_eq!(quantize_residual(-6.0, 0.0, 1.0, 15), 2);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One independently authenticated logical code chunk in a versioned object.
pub struct V35RemoteChunk {
    group_ordinal: u32,
    logical_start: u64,
    rows: u64,
    object: V35ArtifactIdentity,
    version_id: String,
    offset: u64,
    encoded_length: u64,
    decoded_length: u64,
    digest: String,
}

impl V35RemoteChunk {
    /// Construct a bounded code chunk. Whole-object and non-S3 capabilities are rejected.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        group_ordinal: u32,
        logical_start: u64,
        rows: u64,
        object: V35ArtifactIdentity,
        version_id: &str,
        offset: u64,
        encoded_length: u64,
        decoded_length: u64,
        digest: String,
    ) -> Result<Self> {
        validate_object(&object)?;
        let end = offset
            .checked_add(encoded_length)
            .ok_or_else(|| invalid("V35 remote chunk interval overflows"))?;
        if rows == 0
            || version_id.is_empty()
            || encoded_length == 0
            || encoded_length > MAX_ENCODED_CHUNK_BYTES
            || decoded_length == 0
            || decoded_length > MAX_DECODED_CHUNK_BYTES
            || end > object.length
            || (offset == 0 && encoded_length == object.length)
            || !is_digest(&digest)
        {
            return Err(invalid("V35 remote chunk authority differs"));
        }
        Ok(Self {
            group_ordinal,
            logical_start,
            rows,
            object,
            version_id: version_id.to_owned(),
            offset,
            encoded_length,
            decoded_length,
            digest,
        })
    }
    /// Complete storage group containing this chunk.
    pub fn group_ordinal(&self) -> u32 {
        self.group_ordinal
    }
    /// First logical source row in this independently authenticated chunk.
    pub fn logical_start(&self) -> u64 {
        self.logical_start
    }
    /// Maximum decoded allocation declared by the chunk envelope.
    pub fn decoded_length(&self) -> u64 {
        self.decoded_length
    }
}

fn directory_metadata(
    binding: V35RemoteDirectoryBinding,
    uri: &str,
) -> Result<HashMap<String, String>> {
    let manifest = BTreeMap::from([
        (
            "code_schema_sha256".to_owned(),
            digest_hex(binding.code_schema_digest),
        ),
        ("format".to_owned(), DIRECTORY_FORMAT.to_owned()),
        (
            "root_sha256".to_owned(),
            digest_hex(binding.directory_root_digest),
        ),
        (
            "snapshot_sha256".to_owned(),
            digest_hex(binding.snapshot_digest),
        ),
        ("uri".to_owned(), uri.to_owned()),
    ]);
    let manifest = serde_json::to_string(&manifest)
        .map_err(|_| invalid("V35 code-directory manifest cannot be serialized"))?;
    Ok(HashMap::from([(
        "borsuk.v35.remote-directory.manifest".to_owned(),
        manifest,
    )]))
}

fn directory_schema(binding: V35RemoteDirectoryBinding, uri: &str) -> Result<Arc<Schema>> {
    Ok(Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("group_ordinal", DataType::UInt32, false),
            Field::new("logical_start", DataType::UInt64, false),
            Field::new("rows", DataType::UInt64, false),
            Field::new("object_uri", DataType::Utf8, false),
            Field::new("object_sha256", DataType::Utf8, false),
            Field::new("object_length", DataType::UInt64, false),
            Field::new("version_id", DataType::Utf8, false),
            Field::new("offset", DataType::UInt64, false),
            Field::new("encoded_length", DataType::UInt64, false),
            Field::new("decoded_length", DataType::UInt64, false),
            Field::new("chunk_sha256", DataType::Utf8, false),
        ],
        directory_metadata(binding, uri)?,
    )))
}

fn validate_directory_chunks(chunks: &[V35RemoteChunk]) -> Result<()> {
    let group = chunks
        .first()
        .map(V35RemoteChunk::group_ordinal)
        .ok_or_else(|| invalid("V35 code-directory block is empty"))?;
    if chunks.len() > MAX_DIRECTORY_CHUNKS {
        return Err(invalid("V35 code-directory block chunk count differs"));
    }
    let mut next_logical = chunks[0].logical_start;
    for chunk in chunks {
        validate_object(&chunk.object)?;
        if chunk.group_ordinal != group
            || chunk.logical_start != next_logical
            || !is_digest(&chunk.digest)
        {
            return Err(invalid("V35 code-directory block chunk authority differs"));
        }
        next_logical = next_logical
            .checked_add(chunk.rows)
            .ok_or_else(|| invalid("V35 code-directory block rows overflow"))?;
    }
    Ok(())
}

/// Encode one strict, independently authenticated Arrow directory block.
pub fn encode_v35_remote_directory_arrow(
    binding: V35RemoteDirectoryBinding,
    chunks: &[V35RemoteChunk],
    uri: &str,
) -> Result<(Vec<u8>, V35ArtifactIdentity)> {
    if !uri.starts_with("s3://") || uri.contains("/corpus/") {
        return Err(invalid("V35 code-directory block URI differs"));
    }
    validate_directory_chunks(chunks)?;
    let schema = directory_schema(binding, uri)?;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from(
                chunks
                    .iter()
                    .map(|chunk| chunk.group_ordinal)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                chunks
                    .iter()
                    .map(|chunk| chunk.logical_start)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                chunks.iter().map(|chunk| chunk.rows).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                chunks
                    .iter()
                    .map(|chunk| chunk.object.uri.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                chunks
                    .iter()
                    .map(|chunk| chunk.object.digest.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                chunks
                    .iter()
                    .map(|chunk| chunk.object.length)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                chunks
                    .iter()
                    .map(|chunk| chunk.version_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                chunks.iter().map(|chunk| chunk.offset).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                chunks
                    .iter()
                    .map(|chunk| chunk.encoded_length)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                chunks
                    .iter()
                    .map(|chunk| chunk.decoded_length)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                chunks
                    .iter()
                    .map(|chunk| chunk.digest.as_str())
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut bytes = Vec::new();
    let mut writer = FileWriter::try_new_with_options(&mut bytes, schema.as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    if bytes.len() as u64 > MAX_DIRECTORY_BLOCK_BYTES {
        return Err(invalid("V35 code-directory block exceeds admission"));
    }
    let identity = V35ArtifactIdentity {
        digest: format!("{:x}", Sha256::digest(&bytes)),
        digest_algorithm: "sha256".to_owned(),
        length: bytes.len() as u64,
        role: "code-directory-block".to_owned(),
        uri: uri.to_owned(),
    };
    Ok((bytes, identity))
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One selectively loaded directory block for a single storage group.
pub struct V35RemoteDirectoryBlock {
    binding: V35RemoteDirectoryBinding,
    identity: V35ArtifactIdentity,
    chunks: Vec<V35RemoteChunk>,
}

impl V35RemoteDirectoryBlock {
    fn new(
        binding: V35RemoteDirectoryBinding,
        identity: V35ArtifactIdentity,
        chunks: Vec<V35RemoteChunk>,
    ) -> Result<Self> {
        v35_artifact_authority_digest(&identity)?;
        let group = chunks
            .first()
            .map(V35RemoteChunk::group_ordinal)
            .ok_or_else(|| invalid("V35 code-directory block is empty"))?;
        if chunks.iter().any(|chunk| chunk.group_ordinal != group) {
            return Err(invalid("V35 code-directory block group differs"));
        }
        Ok(Self {
            binding,
            identity,
            chunks,
        })
    }
    /// Registered complete identity for this selectively loaded block.
    pub fn identity(&self) -> &V35ArtifactIdentity {
        &self.identity
    }
    /// Independently authenticated chunks in the block.
    pub fn chunks(&self) -> &[V35RemoteChunk] {
        &self.chunks
    }
}

/// Authenticate and decode one strict Arrow directory block before planning.
pub fn decode_v35_remote_directory_arrow(
    bytes: &[u8],
    registered: &V35ArtifactIdentity,
    expected_binding: V35RemoteDirectoryBinding,
) -> Result<V35RemoteDirectoryBlock> {
    v35_artifact_authority_digest(registered)?;
    if bytes.is_empty()
        || bytes.len() as u64 > MAX_DIRECTORY_BLOCK_BYTES
        || registered.length != bytes.len() as u64
        || registered.digest != format!("{:x}", Sha256::digest(bytes))
    {
        return Err(invalid("V35 code-directory block identity differs"));
    }
    let expected_schema = directory_schema(expected_binding, &registered.uri)?;
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != expected_schema.as_ref() {
        return Err(invalid("V35 code-directory block Arrow schema differs"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("V35 code-directory block Arrow batch is missing"))??;
    if reader.next().is_some()
        || batch.num_rows() == 0
        || batch.num_rows() > MAX_DIRECTORY_CHUNKS
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V35 code-directory block Arrow batches differ"));
    }
    let groups = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| invalid("V35 code-directory block group column differs"))?;
    let logical_starts = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V35 code-directory block logical column differs"))?;
    let rows = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V35 code-directory block rows column differs"))?;
    let object_uris = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid("V35 code-directory block object URI column differs"))?;
    let object_digests = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid("V35 code-directory block object digest column differs"))?;
    let object_lengths = batch
        .column(5)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V35 code-directory block object length column differs"))?;
    let versions = batch
        .column(6)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid("V35 code-directory block version column differs"))?;
    let offsets = batch
        .column(7)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V35 code-directory block offset column differs"))?;
    let encoded_lengths = batch
        .column(8)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V35 code-directory block encoded column differs"))?;
    let decoded_lengths = batch
        .column(9)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V35 code-directory block decoded column differs"))?;
    let chunk_digests = batch
        .column(10)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid("V35 code-directory block chunk digest column differs"))?;

    let mut chunks = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let object = V35ArtifactIdentity {
            digest: object_digests.value(row).to_owned(),
            digest_algorithm: "sha256".to_owned(),
            length: object_lengths.value(row),
            role: "remote-code-object".to_owned(),
            uri: object_uris.value(row).to_owned(),
        };
        chunks.push(V35RemoteChunk::new(
            groups.value(row),
            logical_starts.value(row),
            rows.value(row),
            object,
            versions.value(row),
            offsets.value(row),
            encoded_lengths.value(row),
            decoded_lengths.value(row),
            chunk_digests.value(row).to_owned(),
        )?);
    }
    V35RemoteDirectoryBlock::new(expected_binding, registered.clone(), chunks)
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One bounded range request, optionally covering adjacent authenticated chunks.
pub struct V35RemoteRange {
    uri: String,
    version_id: String,
    start: u64,
    end: u64,
    chunks: Vec<V35RemoteChunk>,
}

impl V35RemoteRange {
    /// Immutable S3 object URI.
    pub fn uri(&self) -> &str {
        &self.uri
    }
    /// Required immutable object version.
    pub fn version_id(&self) -> &str {
        &self.version_id
    }
    /// Inclusive byte-range start.
    pub fn start(&self) -> u64 {
        self.start
    }
    /// Exclusive byte-range end.
    pub fn end(&self) -> u64 {
        self.end
    }
    /// Independently authenticated chunks contained by this request.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One versioned range response returned by the narrow transport boundary.
pub struct V35RemoteRangeResponse {
    uri: String,
    version_id: String,
    start: u64,
    end: u64,
    returned_bytes: u64,
    complete: bool,
}

impl V35RemoteRangeResponse {
    /// Construct a response. The executor independently checks it against the opaque plan.
    pub fn new(
        uri: &str,
        version_id: &str,
        start: u64,
        end: u64,
        returned_bytes: u64,
        complete: bool,
    ) -> Result<Self> {
        if !uri.starts_with("s3://")
            || version_id.is_empty()
            || end <= start
            || returned_bytes > MAX_ENCODED_CHUNK_BYTES + 1
        {
            return Err(invalid("V35 remote range response differs"));
        }
        Ok(Self {
            uri: uri.to_owned(),
            version_id: version_id.to_owned(),
            start,
            end,
            returned_bytes,
            complete,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Transport failure classification plus bytes received before failure.
pub struct V35TransportFailure {
    retryable: bool,
    returned_bytes: u64,
}

impl V35TransportFailure {
    /// A timeout, throttle, or transient transport/5xx failure.
    pub fn retryable(returned_bytes: u64) -> Self {
        Self {
            retryable: true,
            returned_bytes,
        }
    }
    /// A non-retryable transport failure.
    pub fn terminal(returned_bytes: u64) -> Self {
        Self {
            retryable: false,
            returned_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Fixed-size handle for one dispatched range request; it never owns response bytes.
pub struct V35RemoteDispatch {
    id: u64,
}

impl V35RemoteDispatch {
    /// Construct a nonzero transport-local dispatch handle.
    pub fn new(id: u64) -> Result<Self> {
        if id == 0 {
            return Err(invalid("V35 remote dispatch handle differs"));
        }
        Ok(Self { id })
    }

    /// Transport-local handle identity.
    pub fn id(self) -> u64 {
        self.id
    }
}

/// Version-pinned range transport with fixed-size handles and caller-owned bodies.
pub trait V35VersionedRangeTransport {
    /// Dispatch one opaque range capability without returning body bytes.
    fn dispatch(
        &mut self,
        range: &V35RemoteRange,
    ) -> std::result::Result<V35RemoteDispatch, V35TransportFailure>;

    /// Complete one prior dispatch into the exact caller-owned range buffer.
    fn complete(
        &mut self,
        dispatch: V35RemoteDispatch,
        range: &V35RemoteRange,
        destination: &mut [u8],
    ) -> std::result::Result<V35RemoteRangeResponse, V35TransportFailure>;

    /// Cancel or drain one outstanding dispatch and report bytes already received.
    fn cancel(&mut self, dispatch: V35RemoteDispatch) -> u64;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Stable terminal class for a remote execution failure.
pub enum V35RemoteFailureKind {
    /// Response identity differs from the planned capability.
    Authority,
    /// Response length differs from the planned inclusive/exclusive interval.
    Length,
    /// An independently registered chunk digest differs.
    Integrity,
    /// Transport failed terminally or exhausted its retry allowance.
    Transport,
    /// A physical GET or byte counter crossed a registered execution ceiling.
    Budget,
    /// Authenticated bytes could not be decoded by the caller.
    Decode,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// Exact physical and logical byte accounting for one remote execution.
pub struct V35RemoteReadReceipt {
    physical_get_attempts: u64,
    requested_bytes: u64,
    returned_bytes: u64,
    unique_logical_bytes: u64,
    authenticated_bytes: u64,
    decoded_bytes: u64,
    retry_attempts: u64,
    retry_requested_bytes: u64,
    retry_returned_bytes: u64,
}

impl V35RemoteReadReceipt {
    /// Physical range attempts, including retries.
    pub fn physical_get_attempts(&self) -> u64 {
        self.physical_get_attempts
    }
    /// Bytes requested across all physical attempts.
    pub fn requested_bytes(&self) -> u64 {
        self.requested_bytes
    }
    /// Bytes returned across successful and failed physical attempts.
    pub fn returned_bytes(&self) -> u64 {
        self.returned_bytes
    }
    /// Unique planned logical bytes, excluding retries.
    pub fn unique_logical_bytes(&self) -> u64 {
        self.unique_logical_bytes
    }
    /// Unique bytes authenticated before decoding.
    pub fn authenticated_bytes(&self) -> u64 {
        self.authenticated_bytes
    }
    /// Bytes reported decoded by the bounded caller callback.
    pub fn decoded_bytes(&self) -> u64 {
        self.decoded_bytes
    }
    /// Physical attempts after the first attempt for a range.
    pub fn retry_attempts(&self) -> u64 {
        self.retry_attempts
    }
    /// Bytes requested by retry attempts.
    pub fn retry_requested_bytes(&self) -> u64 {
        self.retry_requested_bytes
    }
    /// Bytes returned by retry attempts.
    pub fn retry_returned_bytes(&self) -> u64 {
        self.retry_returned_bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Failure that preserves the complete receipt accumulated before termination.
pub struct V35RemoteExecutionFailure {
    kind: V35RemoteFailureKind,
    receipt: V35RemoteReadReceipt,
}

impl V35RemoteExecutionFailure {
    /// Stable failure class.
    pub fn kind(&self) -> V35RemoteFailureKind {
        self.kind
    }
    /// Counters accumulated by the original execution.
    pub fn receipt(&self) -> &V35RemoteReadReceipt {
        &self.receipt
    }
}

fn execution_failure(
    kind: V35RemoteFailureKind,
    receipt: V35RemoteReadReceipt,
) -> V35RemoteExecutionFailure {
    V35RemoteExecutionFailure { kind, receipt }
}

fn add_counter(counter: &mut u64, value: u64) -> bool {
    if let Some(total) = counter.checked_add(value) {
        *counter = total;
        true
    } else {
        *counter = u64::MAX;
        false
    }
}

fn record_returned_bytes(receipt: &mut V35RemoteReadReceipt, returned: u64, retry: bool) -> bool {
    let exact = add_counter(&mut receipt.returned_bytes, returned);
    let retry_exact = !retry || add_counter(&mut receipt.retry_returned_bytes, returned);
    exact && retry_exact && receipt.returned_bytes <= 8 * MIB
}

fn validate_remote_plan(plan: &V35RemotePlan) -> Result<()> {
    if plan.ranges.is_empty()
        || plan.ranges.len() as u64 > MAX_CODE_GETS
        || plan.selected_groups == 0
        || plan.selected_rows == 0
        || plan.requested_code_bytes == 0
        || plan.requested_code_bytes > 8 * MIB
        || plan.generation_digest == [0; 32]
        || plan.query_digest == [0; 32]
    {
        return Err(invalid("V35 remote execution plan differs"));
    }
    let mut groups = BTreeSet::new();
    let mut rows = 0_u64;
    let mut bytes = 0_u64;
    for range in &plan.ranges {
        let range_bytes = range
            .end
            .checked_sub(range.start)
            .ok_or_else(|| invalid("V35 remote execution range differs"))?;
        if range_bytes == 0 || range_bytes > MAX_ENCODED_CHUNK_BYTES || range.chunks.is_empty() {
            return Err(invalid("V35 remote execution range differs"));
        }
        bytes = bytes
            .checked_add(range_bytes)
            .ok_or_else(|| invalid("V35 remote execution bytes overflow"))?;
        let mut next = range.start;
        for chunk in &range.chunks {
            validate_object(&chunk.object)?;
            if chunk.object.uri != range.uri
                || chunk.version_id != range.version_id
                || chunk.offset != next
            {
                return Err(invalid("V35 remote execution capability differs"));
            }
            next = next
                .checked_add(chunk.encoded_length)
                .ok_or_else(|| invalid("V35 remote execution chunk overflows"))?;
            rows = rows
                .checked_add(chunk.rows)
                .ok_or_else(|| invalid("V35 remote execution rows overflow"))?;
            groups.insert(chunk.group_ordinal);
        }
        if next != range.end {
            return Err(invalid("V35 remote execution range coverage differs"));
        }
    }
    if groups.len() != plan.selected_groups
        || rows != plan.selected_rows
        || bytes != plan.requested_code_bytes
    {
        return Err(invalid("V35 remote execution totals differ"));
    }
    Ok(())
}

fn cancel_dispatches<T: V35VersionedRangeTransport>(
    transport: &mut T,
    pending: &mut [Option<V35RemoteDispatch>],
    attempts: &[u64],
    receipt: &mut V35RemoteReadReceipt,
) -> bool {
    let mut within_budget = true;
    for (index, dispatch) in pending.iter_mut().enumerate() {
        if let Some(dispatch) = dispatch.take() {
            within_budget &=
                record_returned_bytes(receipt, transport.cancel(dispatch), attempts[index] > 0);
        }
    }
    within_budget
}

fn abort_execution<T: V35VersionedRangeTransport>(
    kind: V35RemoteFailureKind,
    transport: &mut T,
    pending: &mut [Option<V35RemoteDispatch>],
    attempts: &[u64],
    mut receipt: V35RemoteReadReceipt,
) -> V35RemoteExecutionFailure {
    let within_budget = cancel_dispatches(transport, pending, attempts, &mut receipt);
    execution_failure(
        if within_budget {
            kind
        } else {
            V35RemoteFailureKind::Budget
        },
        receipt,
    )
}

fn abort_failure<T: V35VersionedRangeTransport>(
    transport: &mut T,
    pending: &mut [Option<V35RemoteDispatch>],
    attempts: &[u64],
    failure: V35RemoteExecutionFailure,
) -> V35RemoteExecutionFailure {
    abort_execution(failure.kind, transport, pending, attempts, failure.receipt)
}

fn dispatch_range<T: V35VersionedRangeTransport>(
    transport: &mut T,
    range: &V35RemoteRange,
    attempt: &mut u64,
    receipt: &mut V35RemoteReadReceipt,
) -> std::result::Result<V35RemoteDispatch, V35RemoteExecutionFailure> {
    let requested = range.end - range.start;
    loop {
        if receipt.physical_get_attempts == MAX_CODE_GETS {
            return Err(execution_failure(
                V35RemoteFailureKind::Budget,
                receipt.clone(),
            ));
        }
        receipt.physical_get_attempts += 1;
        if !add_counter(&mut receipt.requested_bytes, requested)
            || (*attempt > 0 && !add_counter(&mut receipt.retry_requested_bytes, requested))
        {
            return Err(execution_failure(
                V35RemoteFailureKind::Budget,
                receipt.clone(),
            ));
        }
        match transport.dispatch(range) {
            Ok(dispatch) => return Ok(dispatch),
            Err(failure) => {
                if !record_returned_bytes(receipt, failure.returned_bytes, *attempt > 0) {
                    return Err(execution_failure(
                        V35RemoteFailureKind::Budget,
                        receipt.clone(),
                    ));
                }
                if !failure.retryable || *attempt >= u64::from(MAX_RETRIES) {
                    return Err(execution_failure(
                        V35RemoteFailureKind::Transport,
                        receipt.clone(),
                    ));
                }
                if !add_counter(&mut receipt.retry_attempts, 1) {
                    return Err(execution_failure(
                        V35RemoteFailureKind::Budget,
                        receipt.clone(),
                    ));
                }
                *attempt += 1;
            }
        }
    }
}

/// Execute an authenticated plan with pipelined dispatch and plan-order delivery.
pub fn execute_v35_remote_plan<T, F>(
    plan: &V35RemotePlan,
    transport: &mut T,
    mut decode: F,
) -> std::result::Result<V35RemoteReadReceipt, V35RemoteExecutionFailure>
where
    T: V35VersionedRangeTransport,
    F: FnMut(&V35RemoteChunk, &[u8]) -> Result<u64>,
{
    if validate_remote_plan(plan).is_err() {
        return Err(execution_failure(
            V35RemoteFailureKind::Authority,
            V35RemoteReadReceipt::default(),
        ));
    }
    let mut receipt = V35RemoteReadReceipt {
        unique_logical_bytes: plan.requested_code_bytes,
        ..V35RemoteReadReceipt::default()
    };
    let mut attempts = vec![0_u64; plan.ranges.len()];
    let mut pending = vec![None; plan.ranges.len()];
    for index in 0..plan.ranges.len().min(DISPATCH_WINDOW) {
        match dispatch_range(
            transport,
            &plan.ranges[index],
            &mut attempts[index],
            &mut receipt,
        ) {
            Ok(dispatch) => pending[index] = Some(dispatch),
            Err(failure) => {
                return Err(abort_failure(transport, &mut pending, &attempts, failure));
            }
        }
    }
    let mut encoded = Vec::<u8>::new();
    for index in 0..plan.ranges.len() {
        let range = &plan.ranges[index];
        let requested = range.end - range.start;
        let requested_usize = match usize::try_from(requested) {
            Ok(value) => value,
            Err(_) => {
                return Err(abort_execution(
                    V35RemoteFailureKind::Budget,
                    transport,
                    &mut pending,
                    &attempts,
                    receipt,
                ));
            }
        };
        encoded.resize(requested_usize, 0);
        let mut dispatch = pending[index]
            .take()
            .expect("validated dispatch window covers current range");
        let response = loop {
            match transport.complete(dispatch, range, &mut encoded) {
                Ok(response) => {
                    if !record_returned_bytes(
                        &mut receipt,
                        response.returned_bytes,
                        attempts[index] > 0,
                    ) {
                        return Err(abort_execution(
                            V35RemoteFailureKind::Budget,
                            transport,
                            &mut pending,
                            &attempts,
                            receipt,
                        ));
                    }
                    break response;
                }
                Err(failure) => {
                    if !record_returned_bytes(
                        &mut receipt,
                        failure.returned_bytes,
                        attempts[index] > 0,
                    ) {
                        return Err(abort_execution(
                            V35RemoteFailureKind::Budget,
                            transport,
                            &mut pending,
                            &attempts,
                            receipt,
                        ));
                    }
                    if !failure.retryable || attempts[index] >= u64::from(MAX_RETRIES) {
                        return Err(abort_execution(
                            V35RemoteFailureKind::Transport,
                            transport,
                            &mut pending,
                            &attempts,
                            receipt,
                        ));
                    }
                    if !add_counter(&mut receipt.retry_attempts, 1) {
                        return Err(abort_execution(
                            V35RemoteFailureKind::Budget,
                            transport,
                            &mut pending,
                            &attempts,
                            receipt,
                        ));
                    }
                    attempts[index] += 1;
                    match dispatch_range(transport, range, &mut attempts[index], &mut receipt) {
                        Ok(retry) => dispatch = retry,
                        Err(failure) => {
                            return Err(abort_failure(transport, &mut pending, &attempts, failure));
                        }
                    }
                }
            }
        };
        if response.uri != range.uri
            || response.version_id != range.version_id
            || response.start != range.start
            || response.end != range.end
        {
            return Err(abort_execution(
                V35RemoteFailureKind::Authority,
                transport,
                &mut pending,
                &attempts,
                receipt,
            ));
        }
        if !response.complete || response.returned_bytes != requested {
            return Err(abort_execution(
                V35RemoteFailureKind::Length,
                transport,
                &mut pending,
                &attempts,
                receipt,
            ));
        }
        let mut authenticated = Vec::with_capacity(range.chunks.len());
        for chunk in &range.chunks {
            let relative = chunk.offset - range.start;
            let Ok(start) = usize::try_from(relative) else {
                return Err(abort_execution(
                    V35RemoteFailureKind::Length,
                    transport,
                    &mut pending,
                    &attempts,
                    receipt,
                ));
            };
            let Ok(length) = usize::try_from(chunk.encoded_length) else {
                return Err(abort_execution(
                    V35RemoteFailureKind::Length,
                    transport,
                    &mut pending,
                    &attempts,
                    receipt,
                ));
            };
            let Some(end) = start.checked_add(length) else {
                return Err(abort_execution(
                    V35RemoteFailureKind::Length,
                    transport,
                    &mut pending,
                    &attempts,
                    receipt,
                ));
            };
            let Some(bytes) = encoded.get(start..end) else {
                return Err(abort_execution(
                    V35RemoteFailureKind::Length,
                    transport,
                    &mut pending,
                    &attempts,
                    receipt,
                ));
            };
            if format!("{:x}", Sha256::digest(bytes)) != chunk.digest {
                return Err(abort_execution(
                    V35RemoteFailureKind::Integrity,
                    transport,
                    &mut pending,
                    &attempts,
                    receipt,
                ));
            }
            authenticated.push((chunk, bytes));
        }
        if !add_counter(&mut receipt.authenticated_bytes, requested) {
            return Err(abort_execution(
                V35RemoteFailureKind::Budget,
                transport,
                &mut pending,
                &attempts,
                receipt,
            ));
        }
        for (chunk, bytes) in authenticated {
            let decoded = match decode(chunk, bytes) {
                Ok(decoded) => decoded,
                Err(_) => {
                    return Err(abort_execution(
                        V35RemoteFailureKind::Decode,
                        transport,
                        &mut pending,
                        &attempts,
                        receipt,
                    ));
                }
            };
            if decoded != chunk.decoded_length {
                return Err(abort_execution(
                    V35RemoteFailureKind::Decode,
                    transport,
                    &mut pending,
                    &attempts,
                    receipt,
                ));
            }
            if !add_counter(&mut receipt.decoded_bytes, decoded) {
                return Err(abort_execution(
                    V35RemoteFailureKind::Budget,
                    transport,
                    &mut pending,
                    &attempts,
                    receipt,
                ));
            }
        }
        let next = index + DISPATCH_WINDOW;
        if next < plan.ranges.len() {
            match dispatch_range(
                transport,
                &plan.ranges[next],
                &mut attempts[next],
                &mut receipt,
            ) {
                Ok(dispatch) => pending[next] = Some(dispatch),
                Err(failure) => {
                    return Err(abort_failure(transport, &mut pending, &attempts, failure));
                }
            }
        }
    }
    Ok(receipt)
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Complete selected-only remote range plan for one query.
pub struct V35RemotePlan {
    ranges: Vec<V35RemoteRange>,
    selected_groups: usize,
    selected_rows: u64,
    requested_code_bytes: u64,
    directory_binding: V35RemoteDirectoryBinding,
    generation_digest: [u8; 32],
    query_digest: [u8; 32],
}

impl V35RemotePlan {
    /// Versioned ranges in selected-group/logical-row order.
    pub fn ranges(&self) -> &[V35RemoteRange] {
        &self.ranges
    }
    /// Complete selected group count.
    pub fn selected_groups(&self) -> usize {
        self.selected_groups
    }
    /// Complete selected logical row count.
    pub fn selected_rows(&self) -> u64 {
        self.selected_rows
    }
    /// Encoded code bytes requested before transport retries.
    pub fn requested_code_bytes(&self) -> u64 {
        self.requested_code_bytes
    }
    /// Per-chunk encoded allocation ceiling.
    pub fn maximum_encoded_chunk_bytes(&self) -> u64 {
        MAX_ENCODED_CHUNK_BYTES
    }
    /// Per-chunk decoded allocation ceiling.
    pub fn maximum_decoded_chunk_bytes(&self) -> u64 {
        MAX_DECODED_CHUNK_BYTES
    }
    /// Complete per-query resident workspace ceiling.
    pub fn maximum_query_workspace_bytes(&self) -> u64 {
        MAX_QUERY_WORKSPACE_BYTES
    }
    /// Retry count after the first exact range attempt.
    pub fn maximum_retries(&self) -> u8 {
        MAX_RETRIES
    }
    /// Authenticated directory root, snapshot, and code-schema authority.
    pub fn directory_binding(&self) -> V35RemoteDirectoryBinding {
        self.directory_binding
    }
    /// Exact query-content digest inherited from routing.
    pub fn query_digest(&self) -> [u8; 32] {
        self.query_digest
    }
    /// Routing-generation authority inherited from the exact route prefix.
    pub fn generation_digest(&self) -> [u8; 32] {
        self.generation_digest
    }
}

/// Plan only authenticated chunks belonging to the exact selected group prefix.
pub fn plan_v35_remote_reads(
    route: &V35RoutePrefix,
    directory: &[V35RemoteDirectoryBlock],
) -> Result<V35RemotePlan> {
    if route.selected_groups().is_empty() || directory.is_empty() {
        return Err(invalid("V35 remote plan authority differs"));
    }
    let binding = route
        .remote_binding()
        .ok_or_else(|| invalid("V35 route has no authenticated remote directory binding"))?;
    if directory.iter().any(|block| block.binding != binding) {
        return Err(invalid("V35 remote directory root or snapshot differs"));
    }
    let mut block_digests = BTreeSet::new();
    for block in directory {
        if !block_digests.insert(v35_artifact_authority_digest(&block.identity)?) {
            return Err(invalid("V35 code-directory block is duplicated"));
        }
    }
    let mut identities = BTreeSet::new();
    for chunk in directory.iter().flat_map(|block| &block.chunks) {
        let identity = (
            chunk.object.uri.as_str(),
            chunk.version_id.as_str(),
            chunk.offset,
            chunk.encoded_length,
            chunk.digest.as_str(),
        );
        if !identities.insert(identity) {
            return Err(invalid("V35 remote chunk is duplicated"));
        }
    }

    let mut selected = Vec::new();
    for group in route.selected_groups() {
        let blocks = directory
            .iter()
            .filter(|block| {
                block
                    .chunks
                    .first()
                    .is_some_and(|chunk| chunk.group_ordinal == group.group_ordinal())
            })
            .collect::<Vec<_>>();
        if blocks.len() != 1
            || v35_artifact_authority_digest(&blocks[0].identity)?
                != group.directory_block_authority_digest()
        {
            return Err(invalid("V35 selected code-directory block differs"));
        }
        let mut chunks = blocks[0].chunks.iter().collect::<Vec<_>>();
        chunks.sort_by_key(|chunk| chunk.logical_start);
        let mut next_logical = chunks
            .first()
            .map(|chunk| chunk.logical_start)
            .ok_or_else(|| invalid("V35 selected group has no remote chunks"))?;
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        for chunk in chunks {
            if chunk.logical_start != next_logical {
                return Err(invalid("V35 remote chunks are not logically contiguous"));
            }
            next_logical = next_logical
                .checked_add(chunk.rows)
                .ok_or_else(|| invalid("V35 remote logical interval overflows"))?;
            rows = rows
                .checked_add(chunk.rows)
                .ok_or_else(|| invalid("V35 remote row total overflows"))?;
            bytes = bytes
                .checked_add(chunk.encoded_length)
                .ok_or_else(|| invalid("V35 remote byte total overflows"))?;
            selected.push(chunk);
        }
        if rows != group.rows() || bytes != group.code_bytes() {
            return Err(invalid("V35 selected group remote authority differs"));
        }
    }

    let mut ranges: Vec<V35RemoteRange> = Vec::new();
    for chunk in selected {
        let end = chunk
            .offset
            .checked_add(chunk.encoded_length)
            .ok_or_else(|| invalid("V35 remote range endpoint overflows"))?;
        if let Some(last) = ranges.last_mut()
            && last.uri == chunk.object.uri
            && last.version_id == chunk.version_id
            && last.end == chunk.offset
            && end - last.start <= MAX_ENCODED_CHUNK_BYTES
        {
            last.end = end;
            last.chunks.push(chunk.clone());
        } else {
            ranges.push(V35RemoteRange {
                uri: chunk.object.uri.clone(),
                version_id: chunk.version_id.clone(),
                start: chunk.offset,
                end,
                chunks: vec![chunk.clone()],
            });
        }
    }
    Ok(V35RemotePlan {
        ranges,
        selected_groups: route.selected_groups().len(),
        selected_rows: route.selected_rows(),
        requested_code_bytes: route.selected_code_bytes(),
        directory_binding: binding,
        generation_digest: route.generation_digest(),
        query_digest: route.query_digest(),
    })
}

#[cfg(test)]
mod remote_execution_limit_tests {
    use super::*;

    struct CountingReader {
        calls: usize,
    }

    impl V35VersionedRangeTransport for CountingReader {
        fn dispatch(
            &mut self,
            _range: &V35RemoteRange,
        ) -> std::result::Result<V35RemoteDispatch, V35TransportFailure> {
            self.calls += 1;
            V35RemoteDispatch::new(self.calls as u64).map_err(|_| V35TransportFailure::terminal(0))
        }

        fn complete(
            &mut self,
            _dispatch: V35RemoteDispatch,
            range: &V35RemoteRange,
            destination: &mut [u8],
        ) -> std::result::Result<V35RemoteRangeResponse, V35TransportFailure> {
            destination[0] = u8::try_from(self.calls).unwrap();
            V35RemoteRangeResponse::new(
                range.uri(),
                range.version_id(),
                range.start(),
                range.end(),
                1,
                true,
            )
            .map_err(|_| V35TransportFailure::terminal(0))
        }

        fn cancel(&mut self, _dispatch: V35RemoteDispatch) -> u64 {
            0
        }
    }

    fn over_get_limit_plan() -> V35RemotePlan {
        let ranges = (1..=25_u8)
            .map(|byte| {
                let body = [byte];
                let object = V35ArtifactIdentity {
                    digest: format!("{:x}", Sha256::digest(body)),
                    digest_algorithm: "sha256".to_owned(),
                    length: 2,
                    role: "remote-code-object".to_owned(),
                    uri: format!("s3://borsuk-index/generations/g01/codes/{byte:02}.arrow"),
                };
                let chunk = V35RemoteChunk::new(
                    u32::from(byte),
                    u64::from(byte - 1),
                    1,
                    object.clone(),
                    "v01",
                    0,
                    1,
                    1,
                    format!("{:x}", Sha256::digest(body)),
                )
                .unwrap();
                V35RemoteRange {
                    uri: object.uri,
                    version_id: "v01".to_owned(),
                    start: 0,
                    end: 1,
                    chunks: vec![chunk],
                }
            })
            .collect();
        V35RemotePlan {
            ranges,
            selected_groups: 25,
            selected_rows: 25,
            requested_code_bytes: 25,
            directory_binding: V35RemoteDirectoryBinding::new([1; 32], [2; 32], [3; 32]).unwrap(),
            generation_digest: [4; 32],
            query_digest: [5; 32],
        }
    }

    struct FullRetryReader {
        calls: usize,
        attempts: HashMap<String, u64>,
        failing_dispatches: BTreeSet<u64>,
    }

    impl V35VersionedRangeTransport for FullRetryReader {
        fn dispatch(
            &mut self,
            range: &V35RemoteRange,
        ) -> std::result::Result<V35RemoteDispatch, V35TransportFailure> {
            self.calls += 1;
            let count = self.attempts.entry(range.uri.clone()).or_default();
            if *count == 0 {
                self.failing_dispatches.insert(self.calls as u64);
            }
            *count += 1;
            V35RemoteDispatch::new(self.calls as u64).map_err(|_| V35TransportFailure::terminal(0))
        }

        fn complete(
            &mut self,
            dispatch: V35RemoteDispatch,
            range: &V35RemoteRange,
            destination: &mut [u8],
        ) -> std::result::Result<V35RemoteRangeResponse, V35TransportFailure> {
            if self.failing_dispatches.remove(&dispatch.id()) {
                Err(V35TransportFailure::retryable(MIB))
            } else {
                destination.fill(0);
                V35RemoteRangeResponse::new(
                    range.uri(),
                    range.version_id(),
                    range.start(),
                    range.end(),
                    MIB,
                    true,
                )
                .map_err(|_| V35TransportFailure::terminal(0))
            }
        }

        fn cancel(&mut self, _dispatch: V35RemoteDispatch) -> u64 {
            0
        }
    }

    fn maximum_byte_plan() -> V35RemotePlan {
        let chunk_digest = format!("{:x}", Sha256::digest(vec![0; MIB as usize]));
        let ranges = (0..8_u8)
            .map(|byte| {
                let object = V35ArtifactIdentity {
                    digest: chunk_digest.clone(),
                    digest_algorithm: "sha256".to_owned(),
                    length: MIB + 1,
                    role: "remote-code-object".to_owned(),
                    uri: format!("s3://borsuk-index/generations/g01/codes/{byte:02}.arrow"),
                };
                let chunk = V35RemoteChunk::new(
                    u32::from(byte),
                    u64::from(byte),
                    1,
                    object.clone(),
                    "v01",
                    0,
                    MIB,
                    MIB,
                    chunk_digest.clone(),
                )
                .unwrap();
                V35RemoteRange {
                    uri: object.uri,
                    version_id: "v01".to_owned(),
                    start: 0,
                    end: MIB,
                    chunks: vec![chunk],
                }
            })
            .collect();
        V35RemotePlan {
            ranges,
            selected_groups: 8,
            selected_rows: 8,
            requested_code_bytes: 8 * MIB,
            directory_binding: V35RemoteDirectoryBinding::new([1; 32], [2; 32], [3; 32]).unwrap(),
            generation_digest: [4; 32],
            query_digest: [5; 32],
        }
    }

    #[test]
    fn v35_remote_execution_rejects_more_than_twenty_four_gets_before_io() {
        // Break caught: a malformed or future planner turns the documented
        // hard GET ceiling into a receipt-only observation after remote I/O.
        let mut reader = CountingReader { calls: 0 };
        let failure =
            execute_v35_remote_plan(&over_get_limit_plan(), &mut reader, |_, _| Ok(1)).unwrap_err();
        assert_eq!(failure.kind(), V35RemoteFailureKind::Authority);
        assert_eq!(failure.receipt().physical_get_attempts(), 0);
        assert_eq!(reader.calls, 0);
    }

    #[test]
    fn v35_remote_execution_stops_when_retries_exhaust_returned_byte_budget() {
        // Break caught: retry bodies can silently double the admitted 8-MiB
        // code scan, or receipt counters wrap instead of terminating.
        let mut reader = FullRetryReader {
            calls: 0,
            attempts: HashMap::new(),
            failing_dispatches: BTreeSet::new(),
        };
        let failure =
            execute_v35_remote_plan(&maximum_byte_plan(), &mut reader, |_, _| Ok(MIB)).unwrap_err();
        assert_eq!(failure.kind(), V35RemoteFailureKind::Budget);
        assert_eq!(failure.receipt().physical_get_attempts(), 12);
        assert_eq!(failure.receipt().requested_bytes(), 12 * MIB);
        assert_eq!(failure.receipt().returned_bytes(), 9 * MIB);
        assert_eq!(failure.receipt().authenticated_bytes(), 4 * MIB);
        assert_eq!(reader.calls, 12);
    }
}
