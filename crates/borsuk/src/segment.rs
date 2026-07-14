use std::cmp::Ordering;

use chrono::{DateTime, Utc};

use crate::{
    error::{BorsukError, Result},
    metric::{VectorMetric, unit_l2_normalized},
    record::VectorRecord,
};

const VECTOR_LOCALITY_PROJECTIONS: usize = 16;
pub(crate) const VECTOR_LOCALITY_KEY_LEN: usize = VECTOR_LOCALITY_PROJECTIONS + 1;
const EXACT_GRAPH_RECORD_LIMIT: usize = 256;
const GRAPH_CANDIDATE_WINDOW: usize = 64;

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
    /// Per-dimension minimum used to quantize PQ codes. Persisting it lets a
    /// query be quantized without the segment's full vectors.
    pub pq_min: Vec<f32>,
    /// Per-dimension maximum used to quantize PQ codes.
    pub pq_max: Vec<f32>,
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
    pub source_record_index: usize,
    pub neighbor_record_index: usize,
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

        let (centroid, radius) = if metric.uses_normalized_euclidean_geometry() {
            let normalized_vectors = records
                .iter()
                .map(|record| unit_l2_normalized(&record.vector))
                .collect::<Vec<_>>();
            let centroid = centroid_from_vectors(&normalized_vectors, dimensions)?;
            let radius = normalized_vectors
                .iter()
                .map(|vector| metric.centroid_geometry_distance(&centroid, vector))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .fold(0.0_f32, f32::max);
            (centroid, radius)
        } else {
            let centroid = centroid(&records, dimensions)?;
            let radius = records
                .iter()
                .map(|record| metric.distance(&centroid, &record.vector))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .fold(0.0_f32, f32::max);
            (centroid, radius)
        };
        let routing_codes = records
            .iter()
            .map(|record| routing_code(&record.vector))
            .collect::<Vec<_>>();
        let (pq_min, pq_max) = pq_bounds(&records, dimensions)?;
        let pq_codes = records
            .iter()
            .map(|record| pq_code_for_vector(&record.vector, &pq_min, &pq_max))
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
            pq_codes,
            pq_min,
            pq_max,
            created_at: Utc::now(),
        })
    }
}

impl SegmentGraph {
    pub(crate) fn from_segment(segment: &Segment, max_neighbors: usize) -> Result<Self> {
        let edges = if max_neighbors == 0 {
            Vec::new()
        } else if segment.records.len() <= EXACT_GRAPH_RECORD_LIMIT {
            exact_graph_edges(segment, max_neighbors)?
        } else {
            bounded_graph_edges(segment, max_neighbors)?
        };

        Ok(Self {
            segment_id: segment.id.clone(),
            level: segment.level,
            edges,
            created_at: segment.created_at,
        })
    }
}

fn exact_graph_edges(segment: &Segment, max_neighbors: usize) -> Result<Vec<GraphEdge>> {
    let mut edges = Vec::new();
    for (source_index, source) in segment.records.iter().enumerate() {
        let mut neighbors = segment
            .records
            .iter()
            .enumerate()
            .filter(|(candidate_index, _)| *candidate_index != source_index)
            .map(|(candidate_index, candidate)| {
                Ok(GraphEdge {
                    source_record_index: source_index,
                    neighbor_record_index: candidate_index,
                    distance: segment.metric.distance(&source.vector, &candidate.vector)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        neighbors.sort_by(|left, right| {
            left.distance
                .partial_cmp(&right.distance)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.neighbor_record_index.cmp(&right.neighbor_record_index))
        });
        neighbors.truncate(max_neighbors);
        edges.extend(neighbors);
    }
    Ok(edges)
}

fn bounded_graph_edges(segment: &Segment, max_neighbors: usize) -> Result<Vec<GraphEdge>> {
    let locality_order = graph_locality_order(segment);
    let locality_positions = graph_positions_by_record_index(&locality_order);
    let routing_order = graph_routing_order(segment);
    let routing_positions = graph_positions_by_record_index(&routing_order);
    let mut edges = Vec::with_capacity(segment.records.len().saturating_mul(max_neighbors));

    for (source_index, source) in segment.records.iter().enumerate() {
        let candidates = graph_candidate_indices(
            source_index,
            &locality_order,
            &locality_positions,
            &routing_order,
            &routing_positions,
            max_neighbors,
        );
        let mut neighbors = candidates
            .into_iter()
            .map(|candidate_index| {
                Ok(GraphEdge {
                    source_record_index: source_index,
                    neighbor_record_index: candidate_index,
                    distance: segment
                        .metric
                        .distance(&source.vector, &segment.records[candidate_index].vector)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        neighbors.sort_by(|left, right| {
            left.distance
                .partial_cmp(&right.distance)
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    locality_positions[source_index]
                        .abs_diff(locality_positions[left.neighbor_record_index])
                        .cmp(
                            &locality_positions[source_index]
                                .abs_diff(locality_positions[right.neighbor_record_index]),
                        )
                })
                .then_with(|| left.neighbor_record_index.cmp(&right.neighbor_record_index))
        });
        neighbors.truncate(max_neighbors);
        edges.extend(neighbors);
    }

    Ok(edges)
}

fn graph_locality_order(segment: &Segment) -> Vec<usize> {
    let mut order = (0..segment.records.len()).collect::<Vec<_>>();
    order.sort_by(|left, right| {
        vector_locality_key(&segment.records[*left].vector)
            .cmp(&vector_locality_key(&segment.records[*right].vector))
            .then_with(|| segment.records[*left].id.cmp(&segment.records[*right].id))
            .then_with(|| left.cmp(right))
    });
    order
}

fn graph_routing_order(segment: &Segment) -> Vec<usize> {
    let mut order = (0..segment.records.len()).collect::<Vec<_>>();
    order.sort_by(|left, right| {
        segment_routing_code(segment, *left)
            .partial_cmp(&segment_routing_code(segment, *right))
            .unwrap_or(Ordering::Equal)
            .then_with(|| segment.records[*left].id.cmp(&segment.records[*right].id))
            .then_with(|| left.cmp(right))
    });
    order
}

fn graph_positions_by_record_index(order: &[usize]) -> Vec<usize> {
    let mut positions = vec![0_usize; order.len()];
    for (position, record_index) in order.iter().copied().enumerate() {
        positions[record_index] = position;
    }
    positions
}

fn graph_candidate_indices(
    source_index: usize,
    locality_order: &[usize],
    locality_positions: &[usize],
    routing_order: &[usize],
    routing_positions: &[usize],
    max_neighbors: usize,
) -> Vec<usize> {
    let max_possible_candidates = locality_order.len().saturating_sub(1);
    let window = GRAPH_CANDIDATE_WINDOW
        .max(max_neighbors.saturating_mul(8))
        .min(max_possible_candidates);
    let mut candidates = Vec::with_capacity(
        window
            .saturating_mul(4)
            .saturating_add(2)
            .min(max_possible_candidates),
    );
    push_graph_order_window(
        &mut candidates,
        source_index,
        locality_order,
        locality_positions[source_index],
        window,
    );
    push_graph_order_window(
        &mut candidates,
        source_index,
        routing_order,
        routing_positions[source_index],
        window,
    );
    candidates
}

fn push_graph_order_window(
    candidates: &mut Vec<usize>,
    source_index: usize,
    order: &[usize],
    source_position: usize,
    window: usize,
) {
    let start = source_position.saturating_sub(window);
    let end = source_position
        .saturating_add(window)
        .saturating_add(1)
        .min(order.len());
    for candidate_index in order[start..end].iter().copied() {
        if candidate_index == source_index || candidates.contains(&candidate_index) {
            continue;
        }
        candidates.push(candidate_index);
    }
}

fn segment_routing_code(segment: &Segment, record_index: usize) -> f32 {
    segment
        .routing_codes
        .get(record_index)
        .copied()
        .unwrap_or_else(|| routing_code(&segment.records[record_index].vector))
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

pub(crate) fn vector_locality_key(vector: &[f32]) -> [i32; VECTOR_LOCALITY_KEY_LEN] {
    let mut key = [0_i32; VECTOR_LOCALITY_KEY_LEN];
    let squared_norm = vector
        .iter()
        .map(|value| {
            let value = quantized_coordinate_space(*value);
            value * value
        })
        .sum::<f32>();
    key[0] = locality_bucket(squared_norm, 16.0);

    for projection in 0..VECTOR_LOCALITY_PROJECTIONS {
        let projected = vector
            .iter()
            .enumerate()
            .map(|(coordinate, value)| {
                let sign = if projection_sign(projection, coordinate) {
                    1.0
                } else {
                    -1.0
                };
                sign * quantized_coordinate_space(*value)
            })
            .sum::<f32>();
        key[projection + 1] = locality_bucket(projected, 16.0);
    }

    key
}

pub(crate) fn vector_bounds(
    records: &[VectorRecord],
    dimensions: usize,
    metric: &VectorMetric,
) -> Result<(Vec<f32>, Vec<f32>)> {
    let mut mins = vec![f32::INFINITY; dimensions];
    let mut maxes = vec![f32::NEG_INFINITY; dimensions];
    for record in records {
        if record.vector.len() != dimensions {
            return Err(BorsukError::DimensionMismatch {
                expected: dimensions,
                actual: record.vector.len(),
            });
        }
        let normalized;
        let vector = if metric.uses_normalized_euclidean_geometry() {
            normalized = unit_l2_normalized(&record.vector);
            normalized.as_slice()
        } else {
            &record.vector
        };
        for ((min, max), value) in mins.iter_mut().zip(&mut maxes).zip(vector) {
            *min = min.min(*value);
            *max = max.max(*value);
        }
    }
    Ok((mins, maxes))
}

fn projection_sign(projection: usize, coordinate: usize) -> bool {
    let mut value = ((projection as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15))
        ^ ((coordinate as u64 + 1).wrapping_mul(0xbf58_476d_1ce4_e5b9));
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^= value >> 31;
    value & 1 == 1
}

fn locality_bucket(value: f32, scale: f32) -> i32 {
    (value * scale)
        .round()
        .clamp(i32::MIN as f32, i32::MAX as f32) as i32
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
    if segment.pq_min.len() != segment.dimensions || segment.pq_max.len() != segment.dimensions {
        // Fall back to bounds derived from the resident vectors (older segments
        // without persisted PQ bounds always carry full vectors).
        let (mins, maxes) = pq_bounds(&segment.records, segment.dimensions)?;
        return Ok(pq_code_for_vector(query, &mins, &maxes));
    }
    Ok(pq_code_for_vector(query, &segment.pq_min, &segment.pq_max))
}

/// Per-dimension PQ quantization bounds derived from a segment's vectors.
pub(crate) fn pq_bounds_for_records(
    records: &[VectorRecord],
    dimensions: usize,
) -> Result<(Vec<f32>, Vec<f32>)> {
    pq_bounds(records, dimensions)
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

fn centroid_from_vectors(vectors: &[Vec<f32>], dimensions: usize) -> Result<Vec<f32>> {
    let mut centroid = vec![0.0_f32; dimensions];
    for vector in vectors {
        if vector.len() != dimensions {
            return Err(BorsukError::DimensionMismatch {
                expected: dimensions,
                actual: vector.len(),
            });
        }

        for (sum, value) in centroid.iter_mut().zip(vector) {
            *sum += value;
        }
    }

    let count = vectors.len() as f32;
    for value in &mut centroid {
        *value /= count;
    }

    Ok(centroid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_segment_graph_tie_breaks_duplicate_vectors_locally() {
        let records = (0..300)
            .map(|idx| VectorRecord::new(format!("doc-{idx:03}"), vec![1.0, 0.0]))
            .collect::<Vec<_>>();
        let segment =
            Segment::from_records("seg".to_string(), 0, VectorMetric::Euclidean, 2, records)
                .unwrap();

        let graph = SegmentGraph::from_segment(&segment, 8).unwrap();
        let tail_neighbors = graph
            .edges
            .iter()
            .filter(|edge| edge.source_record_index == 299)
            .map(|edge| edge.neighbor_record_index)
            .collect::<Vec<_>>();

        assert_eq!(tail_neighbors.len(), 8);
        assert!(
            tail_neighbors.iter().all(|neighbor| *neighbor >= 291),
            "large duplicate-vector graph should use local equivalent neighbors, got {tail_neighbors:?}"
        );
    }
}
