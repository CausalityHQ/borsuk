use std::mem::size_of;

use chrono::{DateTime, Utc};

use crate::{
    error::Result, index::IndexConfig, metric::VectorMetric, record::LeafMode,
    segment::vector_signature,
};

pub(crate) const TABLE_EXTENSION: &str = "parquet";
pub(crate) const SEGMENT_ID_BLOOM_BYTES: usize = 128;
pub(crate) const SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES: usize = 256;
pub(crate) const ROUTING_PAGE_FANOUT: usize = 128;
const SEGMENT_ID_BLOOM_HASHES: usize = 4;
const SEGMENT_VECTOR_SIGNATURE_BLOOM_HASHES: usize = 4;

/// Published index metadata kept in memory while an index is open.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Manifest {
    /// Monotonic manifest version.
    pub version: u64,
    /// Index creation and search configuration.
    pub config: IndexConfig,
    /// Immutable segment summaries used for routing and lower-bound pruning.
    pub segments: Vec<SegmentSummary>,
    /// Global pivot/router rows kept resident with segment summaries.
    pub pivots: Vec<PivotSummary>,
    /// Next numeric id reserved for generated-id add paths.
    pub next_generated_id: u64,
    /// Manifest creation time.
    pub created_at: DateTime<Utc>,
}

impl Manifest {
    pub(crate) fn new(config: IndexConfig) -> Self {
        Self {
            version: 1,
            config,
            segments: Vec::new(),
            pivots: Vec::new(),
            next_generated_id: 0,
            created_at: Utc::now(),
        }
    }

    pub(crate) fn next_version(&self) -> Self {
        Self {
            version: self.version + 1,
            config: self.config.clone(),
            segments: self.segments.clone(),
            pivots: self.pivots.clone(),
            next_generated_id: self.next_generated_id,
            created_at: Utc::now(),
        }
    }

    pub(crate) fn rebuild_pivots(&mut self) {
        self.pivots = self
            .segments
            .iter()
            .enumerate()
            .map(|(ordinal, segment)| PivotSummary {
                id: segment.id.clone(),
                ordinal,
                vector: segment.centroid.clone(),
            })
            .collect();
    }

    pub(crate) fn file_name(&self) -> String {
        format!("manifests/manifest-{:020}.{TABLE_EXTENSION}", self.version)
    }

    pub(crate) fn file_name_for_version(version: u64) -> String {
        format!("manifests/manifest-{version:020}.{TABLE_EXTENSION}")
    }

    pub(crate) fn routing_file_name(&self) -> String {
        Self::routing_file_name_for_version(self.version)
    }

    pub(crate) fn routing_file_name_for_version(version: u64) -> String {
        format!("routing/segments-{version:020}.{TABLE_EXTENSION}")
    }

    pub(crate) fn pivots_file_name(&self) -> String {
        Self::pivots_file_name_for_version(self.version)
    }

    pub(crate) fn pivots_file_name_for_version(version: u64) -> String {
        format!("routing/pivots-{version:020}.{TABLE_EXTENSION}")
    }

    pub(crate) fn routing_layer_page_file_name(
        version: u64,
        routing_level: u8,
        page_ordinal: usize,
    ) -> String {
        format!(
            "routing/layers/{version:020}/L{routing_level}/page-{page_ordinal:020}.{TABLE_EXTENSION}"
        )
    }

    pub(crate) fn resident_bytes_estimate(&self) -> u64 {
        let config_bytes = size_of::<IndexConfig>() + self.config.uri.len();
        let segments_bytes = self
            .segments
            .iter()
            .map(SegmentSummary::resident_bytes_estimate)
            .sum::<usize>();
        let pivots_bytes = self
            .pivots
            .iter()
            .map(PivotSummary::resident_bytes_estimate)
            .sum::<usize>();
        (size_of::<Self>() + config_bytes + segments_bytes + pivots_bytes) as u64
    }
}

/// Global pivot/router row kept in memory for segment-level routing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PivotSummary {
    /// Stable pivot identifier. Current pivots are derived from segment centroids.
    pub id: String,
    /// Pivot order inside the published routing table.
    pub ordinal: usize,
    /// Pivot vector used by router implementations.
    pub vector: Vec<f32>,
}

impl PivotSummary {
    pub(crate) fn resident_bytes_estimate(&self) -> usize {
        size_of::<Self>() + self.id.len() + self.vector.len() * size_of::<f32>()
    }
}

/// Summary for an immutable segment. This is the routing layer kept in memory.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SegmentSummary {
    /// Segment identifier.
    pub id: String,
    /// LSM level, where zero is the fresh insert level.
    pub level: u8,
    /// Segment object path relative to the index root.
    pub path: String,
    /// Number of records inside the segment.
    pub object_count: usize,
    /// Vector dimensionality.
    pub dimensions: usize,
    /// Segment centroid used for coarse lower-bound pruning.
    pub centroid: Vec<f32>,
    /// Maximum distance from centroid to any vector in the segment.
    pub radius: f32,
    /// BLAKE3 checksum of the segment bytes.
    pub checksum: String,
    /// Stored segment size.
    pub size_bytes: u64,
    /// Segment-local graph object path relative to the index root.
    pub graph_path: String,
    /// BLAKE3 checksum of the graph bytes.
    pub graph_checksum: String,
    /// Stored graph size.
    pub graph_size_bytes: u64,
    /// Segment-local leaf engine represented by this summary.
    pub leaf_mode: LeafMode,
    /// Fixed-size bloom filter over record ids in this segment.
    pub id_bloom: Vec<u8>,
    /// Fixed-size bloom filter over quantized vector signatures in this segment.
    pub vector_signature_bloom: Vec<u8>,
    /// Segment creation time.
    pub created_at: DateTime<Utc>,
}

impl SegmentSummary {
    pub(crate) fn resident_bytes_estimate(&self) -> usize {
        size_of::<Self>()
            + self.id.len()
            + self.path.len()
            + self.checksum.len()
            + self.graph_path.len()
            + self.graph_checksum.len()
            + self.id_bloom.len()
            + self.vector_signature_bloom.len()
            + self.centroid.len() * size_of::<f32>()
    }

    pub(crate) fn might_contain_record_id(&self, id: &str) -> bool {
        if self.id_bloom.len() != SEGMENT_ID_BLOOM_BYTES {
            return true;
        }

        bloom_contains(&self.id_bloom, id)
    }

    pub(crate) fn might_contain_vector_signature(&self, signature: u64) -> bool {
        if self.vector_signature_bloom.len() != SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES {
            return false;
        }

        vector_signature_bloom_contains(&self.vector_signature_bloom, signature)
    }

    pub(crate) fn lower_bound(&self, query: &[f32], metric: &VectorMetric) -> Result<f32> {
        if !metric.supports_centroid_lower_bound() {
            return Ok(f32::NEG_INFINITY);
        }

        let center_distance = metric.distance(query, &self.centroid)?;
        Ok((center_distance - self.radius).max(0.0))
    }
}

pub(crate) fn segment_id_bloom<'a>(ids: impl IntoIterator<Item = &'a str>) -> Vec<u8> {
    let mut bloom = vec![0_u8; SEGMENT_ID_BLOOM_BYTES];
    for id in ids {
        for position in bloom_positions(id) {
            bloom[position / 8] |= 1_u8 << (position % 8);
        }
    }
    bloom
}

pub(crate) fn segment_vector_signature_bloom<'a>(
    vectors: impl IntoIterator<Item = &'a [f32]>,
) -> Vec<u8> {
    let mut bloom = vec![0_u8; SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES];
    for vector in vectors {
        for position in vector_signature_bloom_positions(vector_signature(vector)) {
            bloom[position / 8] |= 1_u8 << (position % 8);
        }
    }
    bloom
}

fn bloom_contains(bloom: &[u8], id: &str) -> bool {
    bloom_positions(id)
        .into_iter()
        .all(|position| bloom[position / 8] & (1_u8 << (position % 8)) != 0)
}

fn vector_signature_bloom_contains(bloom: &[u8], signature: u64) -> bool {
    vector_signature_bloom_positions(signature)
        .into_iter()
        .all(|position| bloom[position / 8] & (1_u8 << (position % 8)) != 0)
}

fn bloom_positions(id: &str) -> [usize; SEGMENT_ID_BLOOM_HASHES] {
    let hash = blake3::hash(id.as_bytes());
    let bytes = hash.as_bytes();
    let bit_count = SEGMENT_ID_BLOOM_BYTES * 8;
    let mut positions = [0_usize; SEGMENT_ID_BLOOM_HASHES];
    for (index, position) in positions.iter_mut().enumerate() {
        let start = index * 4;
        let value = u32::from_le_bytes([
            bytes[start],
            bytes[start + 1],
            bytes[start + 2],
            bytes[start + 3],
        ]);
        *position = value as usize % bit_count;
    }
    positions
}

fn vector_signature_bloom_positions(
    signature: u64,
) -> [usize; SEGMENT_VECTOR_SIGNATURE_BLOOM_HASHES] {
    let hash = blake3::hash(&signature.to_le_bytes());
    let bytes = hash.as_bytes();
    let bit_count = SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES * 8;
    let mut positions = [0_usize; SEGMENT_VECTOR_SIGNATURE_BLOOM_HASHES];
    for (index, position) in positions.iter_mut().enumerate() {
        let start = index * 4;
        let value = u32::from_le_bytes([
            bytes[start],
            bytes[start + 1],
            bytes[start + 2],
            bytes[start + 3],
        ]);
        *position = value as usize % bit_count;
    }
    positions
}
