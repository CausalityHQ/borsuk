use chrono::{DateTime, Utc};
use std::cmp::Ordering;

use crate::{
    error::{BorsukError, Result},
    metric::VectorMetric,
    record::VectorRecord,
};

/// Immutable segment stored as one local file or blob object.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct Segment {
    pub id: String,
    pub level: u8,
    pub metric: VectorMetric,
    pub dimensions: usize,
    pub centroid: Vec<f32>,
    pub radius: f32,
    pub records: Vec<VectorRecord>,
    pub routing_codes: Vec<f32>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct SegmentGraph {
    pub segment_id: String,
    pub level: u8,
    pub edges: Vec<GraphEdge>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct GraphEdge {
    pub source_record_id: String,
    pub neighbor_record_id: String,
    pub distance: f32,
}

impl Segment {
    pub(crate) fn from_records(
        id: String,
        level: u8,
        metric: VectorMetric,
        dimensions: usize,
        records: Vec<VectorRecord>,
    ) -> Result<Self> {
        if records.is_empty() {
            return Err(BorsukError::InvalidMetricInput(
                "segments must contain at least one record".to_string(),
            ));
        }

        let centroid = centroid(&records, dimensions)?;
        let radius = records
            .iter()
            .map(|record| metric.distance(&centroid, &record.vector))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .fold(0.0_f32, f32::max);
        let routing_codes = records
            .iter()
            .map(|record| routing_code(&record.vector))
            .collect::<Vec<_>>();

        Ok(Self {
            id,
            level,
            metric,
            dimensions,
            centroid,
            radius,
            records,
            routing_codes,
            created_at: Utc::now(),
        })
    }
}

impl SegmentGraph {
    pub(crate) fn from_segment(segment: &Segment, max_neighbors: usize) -> Result<Self> {
        let mut edges = Vec::new();
        if max_neighbors > 0 {
            for source in &segment.records {
                let mut neighbors = segment
                    .records
                    .iter()
                    .filter(|candidate| candidate.id != source.id)
                    .map(|candidate| {
                        Ok(GraphEdge {
                            source_record_id: source.id.clone(),
                            neighbor_record_id: candidate.id.clone(),
                            distance: segment.metric.distance(&source.vector, &candidate.vector)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                neighbors.sort_by(|left, right| {
                    left.distance
                        .partial_cmp(&right.distance)
                        .unwrap_or(Ordering::Equal)
                        .then_with(|| left.neighbor_record_id.cmp(&right.neighbor_record_id))
                });
                neighbors.truncate(max_neighbors);
                edges.extend(neighbors);
            }
        }

        Ok(Self {
            segment_id: segment.id.clone(),
            level: segment.level,
            edges,
            created_at: segment.created_at,
        })
    }
}

pub(crate) fn routing_code(vector: &[f32]) -> f32 {
    vector
        .iter()
        .enumerate()
        .map(
            |(index, value)| {
                if index % 2 == 0 { *value } else { -*value }
            },
        )
        .sum()
}

fn centroid(records: &[VectorRecord], dimensions: usize) -> Result<Vec<f32>> {
    let mut centroid = vec![0.0_f32; dimensions];
    for record in records {
        if record.vector.len() != dimensions {
            return Err(BorsukError::DimensionMismatch {
                expected: dimensions,
                actual: record.vector.len(),
            });
        }

        for (sum, value) in centroid.iter_mut().zip(&record.vector) {
            *sum += value;
        }
    }

    let count = records.len() as f32;
    for value in &mut centroid {
        *value /= count;
    }

    Ok(centroid)
}
