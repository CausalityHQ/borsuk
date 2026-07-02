use std::mem::size_of;

use chrono::{DateTime, Utc};

use crate::{error::Result, index::IndexConfig, metric::VectorMetric};

pub(crate) const TABLE_EXTENSION: &str = "parquet";

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
            created_at: Utc::now(),
        }
    }

    pub(crate) fn next_version(&self) -> Self {
        Self {
            version: self.version + 1,
            config: self.config.clone(),
            segments: self.segments.clone(),
            pivots: self.pivots.clone(),
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
            + self.centroid.len() * size_of::<f32>()
    }

    pub(crate) fn lower_bound(&self, query: &[f32], metric: &VectorMetric) -> Result<f32> {
        if !metric.supports_centroid_lower_bound() {
            return Ok(0.0);
        }

        let center_distance = metric.distance(query, &self.centroid)?;
        Ok((center_distance - self.radius).max(0.0))
    }
}
