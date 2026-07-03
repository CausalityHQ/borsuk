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
    pub pq_codes: Vec<Vec<u8>>,
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
        let pq_codes = pq_codes_for_records(&records, dimensions)?;

        Ok(Self {
            id,
            level,
            metric,
            dimensions,
            centroid,
            radius,
            records,
            routing_codes,
            pq_codes,
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

pub(crate) fn vector_signature(vector: &[f32]) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(vector.len() as u64).to_le_bytes());
    for (coordinate_index, value) in vector.iter().copied().enumerate() {
        hasher.update(&(coordinate_index as u64).to_le_bytes());
        hasher.update(&signature_coordinate(value).to_le_bytes());
    }
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    u64::from_le_bytes(bytes)
}

fn signature_coordinate(value: f32) -> i32 {
    (quantized_coordinate_space(value) * 4096.0)
        .round()
        .clamp(i32::MIN as f32, i32::MAX as f32) as i32
}

pub(crate) fn pq_codes_for_records(
    records: &[VectorRecord],
    dimensions: usize,
) -> Result<Vec<Vec<u8>>> {
    let (mins, maxes) = pq_bounds(records, dimensions)?;
    Ok(records
        .iter()
        .map(|record| pq_code_for_vector(&record.vector, &mins, &maxes))
        .collect())
}

pub(crate) fn pq_code_for_query(segment: &Segment, query: &[f32]) -> Result<Vec<u8>> {
    let (mins, maxes) = pq_bounds(&segment.records, segment.dimensions)?;
    Ok(pq_code_for_vector(query, &mins, &maxes))
}

fn pq_bounds(records: &[VectorRecord], dimensions: usize) -> Result<(Vec<f32>, Vec<f32>)> {
    let mut mins = vec![f32::INFINITY; dimensions];
    let mut maxes = vec![f32::NEG_INFINITY; dimensions];
    for record in records {
        if record.vector.len() != dimensions {
            return Err(BorsukError::DimensionMismatch {
                expected: dimensions,
                actual: record.vector.len(),
            });
        }
        for ((min, max), value) in mins.iter_mut().zip(&mut maxes).zip(&record.vector) {
            let value = quantized_coordinate_space(*value);
            *min = min.min(value);
            *max = max.max(value);
        }
    }
    Ok((mins, maxes))
}

fn pq_code_for_vector(vector: &[f32], mins: &[f32], maxes: &[f32]) -> Vec<u8> {
    vector
        .iter()
        .zip(mins)
        .zip(maxes)
        .map(|((value, min), max)| quantize_coordinate(*value, *min, *max))
        .collect()
}

fn quantize_coordinate(value: f32, min: f32, max: f32) -> u8 {
    if max <= min {
        return 128;
    }
    let value = quantized_coordinate_space(value);
    let normalized = ((value - min) / (max - min)).clamp(0.0, 1.0);
    (normalized * 255.0).round() as u8
}

fn quantized_coordinate_space(value: f32) -> f32 {
    value.signum() * value.abs().ln_1p()
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
