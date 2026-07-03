use std::mem::size_of;

use chrono::{DateTime, Utc};

use crate::{
    error::{BorsukError, Result},
    index::IndexConfig,
    metric::VectorMetric,
    record::LeafMode,
    segment::vector_signature,
};

pub(crate) const TABLE_EXTENSION: &str = "parquet";
pub(crate) const SEGMENT_ID_BLOOM_BYTES: usize = 128;
pub(crate) const SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES: usize = 256;
/// Default number of routing page refs grouped into each routing parent page.
pub const DEFAULT_ROUTING_PAGE_FANOUT: usize = 128;
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
    /// Highest persisted routing layer for this manifest version.
    pub(crate) routing_max_level: u8,
    /// Number of routing page refs grouped into each routing parent page.
    pub(crate) routing_page_fanout: usize,
    /// Manifest creation time.
    pub created_at: DateTime<Utc>,
}

impl Manifest {
    pub(crate) fn new_with_routing_page_fanout(
        config: IndexConfig,
        routing_page_fanout: usize,
    ) -> Self {
        Self {
            version: 1,
            config,
            segments: Vec::new(),
            pivots: Vec::new(),
            next_generated_id: 0,
            routing_max_level: 0,
            routing_page_fanout,
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
            routing_max_level: self.routing_max_level,
            routing_page_fanout: self.routing_page_fanout,
            created_at: Utc::now(),
        }
    }

    pub(crate) fn set_routing_max_level_for_leaf_pages(
        &mut self,
        leaf_page_count: usize,
    ) -> Result<()> {
        let mut page_count = leaf_page_count;
        let mut routing_level = 0_u8;
        while page_count > 1 {
            page_count = page_count.div_ceil(self.routing_page_fanout);
            routing_level = routing_level.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage("routing layer depth exceeds u8".to_string())
            })?;
        }
        self.routing_max_level = routing_level;
        Ok(())
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

    pub(crate) fn routing_layer_page_index_file_name(version: u64, routing_level: u8) -> String {
        format!("routing/layers/{version:020}/L{routing_level}/pages.{TABLE_EXTENSION}")
    }

    pub(crate) fn routing_layer_page_content_file_name(
        routing_level: u8,
        checksum: &str,
    ) -> String {
        let prefix = &checksum[..2];
        format!("routing/pages/L{routing_level}/{prefix}/page-{checksum}.{TABLE_EXTENSION}")
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

/// Reference from a versioned routing layer to an immutable routing page object.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RoutingLayerPageRef {
    pub routing_level: u8,
    pub page_ordinal: usize,
    pub path: String,
    pub checksum: String,
    pub page_segments: usize,
    pub leaf_segments: usize,
    pub dimensions: usize,
    pub centroid: Vec<f32>,
    pub radius: f32,
    pub bounds_min: Vec<f32>,
    pub bounds_max: Vec<f32>,
    pub id_bloom: Vec<u8>,
    pub vector_signature_bloom: Vec<u8>,
    pub level_mask: u64,
    pub page_records: usize,
    pub page_segment_bytes: u64,
    pub page_graph_bytes: u64,
}

impl RoutingLayerPageRef {
    pub(crate) fn might_contain_record_id(&self, id: impl AsRef<[u8]>) -> bool {
        if self.id_bloom.len() != SEGMENT_ID_BLOOM_BYTES {
            return true;
        }

        bloom_contains(&self.id_bloom, id)
    }

    pub(crate) fn lower_bound(&self, query: &[f32], metric: &VectorMetric) -> Result<f32> {
        if !metric.supports_centroid_lower_bound() {
            return Ok(f32::NEG_INFINITY);
        }

        if let Some(lower_bound) =
            vector_bounds_lower_bound(query, metric, &self.bounds_min, &self.bounds_max)?
        {
            return Ok(lower_bound);
        }

        let center_distance = metric.distance(query, &self.centroid)?;
        Ok((center_distance - self.radius).max(0.0))
    }

    pub(crate) fn might_contain_vector_signature(&self, signature: u64) -> bool {
        if self.vector_signature_bloom.len() != SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES {
            return true;
        }

        vector_signature_bloom_contains(&self.vector_signature_bloom, signature)
    }

    pub(crate) fn might_contain_level(&self, level: u8) -> bool {
        if self.level_mask == u64::MAX || level >= u64::BITS as u8 {
            return true;
        }

        self.level_mask & (1_u64 << level) != 0
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
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
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
    /// Per-dimension minimum vector coordinates in this segment.
    pub bounds_min: Vec<f32>,
    /// Per-dimension maximum vector coordinates in this segment.
    pub bounds_max: Vec<f32>,
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
            + self.bounds_min.len() * size_of::<f32>()
            + self.bounds_max.len() * size_of::<f32>()
    }

    pub(crate) fn might_contain_record_id(&self, id: impl AsRef<[u8]>) -> bool {
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

        if let Some(lower_bound) =
            vector_bounds_lower_bound(query, metric, &self.bounds_min, &self.bounds_max)?
        {
            return Ok(lower_bound);
        }

        let center_distance = metric.distance(query, &self.centroid)?;
        Ok((center_distance - self.radius).max(0.0))
    }
}

pub(crate) fn segment_id_bloom(ids: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Vec<u8> {
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

fn bloom_contains(bloom: &[u8], id: impl AsRef<[u8]>) -> bool {
    bloom_positions(id)
        .into_iter()
        .all(|position| bloom[position / 8] & (1_u8 << (position % 8)) != 0)
}

fn vector_signature_bloom_contains(bloom: &[u8], signature: u64) -> bool {
    vector_signature_bloom_positions(signature)
        .into_iter()
        .all(|position| bloom[position / 8] & (1_u8 << (position % 8)) != 0)
}

fn vector_bounds_lower_bound(
    query: &[f32],
    metric: &VectorMetric,
    bounds_min: &[f32],
    bounds_max: &[f32],
) -> Result<Option<f32>> {
    if bounds_min.len() != query.len() || bounds_max.len() != query.len() {
        return Ok(None);
    }

    let outside_deltas = query
        .iter()
        .zip(bounds_min)
        .zip(bounds_max)
        .map(|((value, min), max)| {
            if !min.is_finite() || !max.is_finite() || min > max {
                return Err(BorsukError::InvalidStorage(
                    "routing vector bounds must contain finite min <= max values".to_string(),
                ));
            }
            if value < min {
                Ok(min - value)
            } else if value > max {
                Ok(value - max)
            } else {
                Ok(0.0)
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let lower_bound = match metric {
        VectorMetric::Euclidean => outside_deltas
            .iter()
            .map(|delta| delta * delta)
            .sum::<f32>()
            .sqrt(),
        VectorMetric::Manhattan => outside_deltas.iter().sum(),
        VectorMetric::Gower => outside_deltas.iter().sum::<f32>() / outside_deltas.len() as f32,
        VectorMetric::Chebyshev => outside_deltas.into_iter().fold(0.0_f32, f32::max),
        VectorMetric::Minkowski { p } => outside_deltas
            .iter()
            .map(|delta| delta.powf(*p))
            .sum::<f32>()
            .powf(1.0 / *p),
        _ => return Ok(None),
    };
    Ok(Some(lower_bound))
}

fn bloom_positions(id: impl AsRef<[u8]>) -> [usize; SEGMENT_ID_BLOOM_HASHES] {
    let hash = blake3::hash(id.as_ref());
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
