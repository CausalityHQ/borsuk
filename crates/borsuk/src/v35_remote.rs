use std::collections::BTreeSet;

use crate::{
    BorsukError, Result, V35ArtifactIdentity, V35RemoteDirectoryBinding, V35RoutePrefix,
    v35_route::v35_artifact_authority_digest,
};
use half::f16;

const MIB: u64 = 1_048_576;
const MAX_ENCODED_CHUNK_BYTES: u64 = MIB;
const MAX_DECODED_CHUNK_BYTES: u64 = 2 * MIB;
const MAX_QUERY_WORKSPACE_BYTES: u64 = 32 * MIB;
const MAX_RETRIES: u8 = 2;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One selectively loaded directory block for a single storage group.
pub struct V35RemoteDirectoryBlock {
    binding: V35RemoteDirectoryBinding,
    identity: V35ArtifactIdentity,
    chunks: Vec<V35RemoteChunk>,
}

impl V35RemoteDirectoryBlock {
    /// Bind one nonempty group block to its immutable directory root and snapshot.
    pub fn new(
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

#[derive(Debug, Clone, PartialEq, Eq)]
/// One bounded range request, optionally covering adjacent authenticated chunks.
pub struct V35RemoteRange {
    uri: String,
    version_id: String,
    start: u64,
    end: u64,
    chunk_count: usize,
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
        self.chunk_count
    }
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
        {
            last.end = end;
            last.chunk_count += 1;
        } else {
            ranges.push(V35RemoteRange {
                uri: chunk.object.uri.clone(),
                version_id: chunk.version_id.clone(),
                start: chunk.offset,
                end,
                chunk_count: 1,
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
