use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io::Cursor,
    sync::Arc,
};

use crate::{
    BorsukError, Result, V35ArtifactIdentity, V35RemoteDirectoryBinding, V35RoutePrefix,
    v35_route::v35_artifact_authority_digest,
};
use arrow_array::{Array, RecordBatch, StringArray, UInt32Array, UInt64Array};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use half::f16;
use sha2::{Digest, Sha256};

const MIB: u64 = 1_048_576;
const MAX_ENCODED_CHUNK_BYTES: u64 = MIB;
const MAX_DECODED_CHUNK_BYTES: u64 = 2 * MIB;
const MAX_QUERY_WORKSPACE_BYTES: u64 = 32 * MIB;
const MAX_RETRIES: u8 = 2;
const MAX_CODE_GETS: u64 = 24;
const MAX_CANDIDATES: usize = 12_288;
const MAX_MUTATION_ENTRIES: usize = 1_000_000;
const MAX_DIRECTORY_BLOCK_BYTES: u64 = MIB;
const MAX_DIRECTORY_CHUNKS: usize = 64;
const DIRECTORY_FORMAT: &str = "borsuk-v35-remote-directory-block-v1";

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

/// Versioned range-only transport. It has no list, discovery, write, or endpoint surface.
pub trait V35VersionedRangeReader {
    /// Read exactly one opaque range capability emitted by the authenticated planner.
    fn read_range(
        &mut self,
        range: &V35RemoteRange,
        destination: &mut [u8],
    ) -> std::result::Result<V35RemoteRangeResponse, V35TransportFailure>;
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

/// Execute an authenticated plan with bounded retries and plan-order chunk delivery.
pub fn execute_v35_remote_plan<R, F>(
    plan: &V35RemotePlan,
    reader: &mut R,
    mut decode: F,
) -> std::result::Result<V35RemoteReadReceipt, V35RemoteExecutionFailure>
where
    R: V35VersionedRangeReader,
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
    let mut encoded = Vec::<u8>::new();
    for range in &plan.ranges {
        let requested = range.end - range.start;
        let requested_usize = usize::try_from(requested)
            .map_err(|_| execution_failure(V35RemoteFailureKind::Budget, receipt.clone()))?;
        encoded.resize(requested_usize, 0);
        let mut range_attempt = 0_u64;
        let response = loop {
            if receipt.physical_get_attempts == MAX_CODE_GETS {
                return Err(execution_failure(V35RemoteFailureKind::Transport, receipt));
            }
            receipt.physical_get_attempts += 1;
            if !add_counter(&mut receipt.requested_bytes, requested) {
                return Err(execution_failure(V35RemoteFailureKind::Budget, receipt));
            }
            if range_attempt > 0 {
                if !add_counter(&mut receipt.retry_requested_bytes, requested) {
                    return Err(execution_failure(V35RemoteFailureKind::Budget, receipt));
                }
            }
            match reader.read_range(range, &mut encoded) {
                Ok(response) => {
                    if !record_returned_bytes(
                        &mut receipt,
                        response.returned_bytes,
                        range_attempt > 0,
                    ) {
                        return Err(execution_failure(V35RemoteFailureKind::Budget, receipt));
                    }
                    break response;
                }
                Err(failure) => {
                    if !record_returned_bytes(
                        &mut receipt,
                        failure.returned_bytes,
                        range_attempt > 0,
                    ) {
                        return Err(execution_failure(V35RemoteFailureKind::Budget, receipt));
                    }
                    if !failure.retryable || range_attempt >= u64::from(MAX_RETRIES) {
                        return Err(execution_failure(V35RemoteFailureKind::Transport, receipt));
                    }
                    receipt.retry_attempts += 1;
                    range_attempt += 1;
                }
            }
        };
        if response.uri != range.uri
            || response.version_id != range.version_id
            || response.start != range.start
            || response.end != range.end
        {
            return Err(execution_failure(V35RemoteFailureKind::Authority, receipt));
        }
        if !response.complete || response.returned_bytes != requested {
            return Err(execution_failure(V35RemoteFailureKind::Length, receipt));
        }
        let mut authenticated = Vec::with_capacity(range.chunks.len());
        for chunk in &range.chunks {
            let relative = chunk.offset - range.start;
            let start = usize::try_from(relative)
                .map_err(|_| execution_failure(V35RemoteFailureKind::Length, receipt.clone()))?;
            let length = usize::try_from(chunk.encoded_length)
                .map_err(|_| execution_failure(V35RemoteFailureKind::Length, receipt.clone()))?;
            let end = start
                .checked_add(length)
                .ok_or_else(|| execution_failure(V35RemoteFailureKind::Length, receipt.clone()))?;
            let bytes = encoded
                .get(start..end)
                .ok_or_else(|| execution_failure(V35RemoteFailureKind::Length, receipt.clone()))?;
            if format!("{:x}", Sha256::digest(bytes)) != chunk.digest {
                return Err(execution_failure(V35RemoteFailureKind::Integrity, receipt));
            }
            authenticated.push((chunk, bytes));
        }
        if !add_counter(&mut receipt.authenticated_bytes, requested) {
            return Err(execution_failure(V35RemoteFailureKind::Budget, receipt));
        }
        for (chunk, bytes) in authenticated {
            let decoded = decode(chunk, bytes)
                .map_err(|_| execution_failure(V35RemoteFailureKind::Decode, receipt.clone()))?;
            if decoded != chunk.decoded_length {
                return Err(execution_failure(V35RemoteFailureKind::Decode, receipt));
            }
            if !add_counter(&mut receipt.decoded_bytes, decoded) {
                return Err(execution_failure(V35RemoteFailureKind::Budget, receipt));
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

    impl V35VersionedRangeReader for CountingReader {
        fn read_range(
            &mut self,
            range: &V35RemoteRange,
            destination: &mut [u8],
        ) -> std::result::Result<V35RemoteRangeResponse, V35TransportFailure> {
            self.calls += 1;
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
    }

    impl V35VersionedRangeReader for FullRetryReader {
        fn read_range(
            &mut self,
            range: &V35RemoteRange,
            destination: &mut [u8],
        ) -> std::result::Result<V35RemoteRangeResponse, V35TransportFailure> {
            self.calls += 1;
            if self.calls % 2 == 1 {
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
        let mut reader = FullRetryReader { calls: 0 };
        let failure =
            execute_v35_remote_plan(&maximum_byte_plan(), &mut reader, |_, _| Ok(MIB)).unwrap_err();
        assert_eq!(failure.kind(), V35RemoteFailureKind::Budget);
        assert_eq!(failure.receipt().physical_get_attempts(), 9);
        assert_eq!(failure.receipt().requested_bytes(), 9 * MIB);
        assert_eq!(failure.receipt().returned_bytes(), 9 * MIB);
        assert_eq!(failure.receipt().authenticated_bytes(), 4 * MIB);
        assert_eq!(reader.calls, 9);
    }
}
