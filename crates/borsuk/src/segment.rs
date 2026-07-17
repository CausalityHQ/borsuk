use std::cmp::Ordering;

use chrono::{DateTime, Utc};

use crate::{
    error::{BorsukError, Result},
    metric::{VectorMetric, unit_l2_normalized},
    record::{QuantizerKind, VectorRecord},
    turboquant::TurboQuantizer,
};

const VECTOR_LOCALITY_PROJECTIONS: usize = 16;
pub(crate) const VECTOR_LOCALITY_KEY_LEN: usize = VECTOR_LOCALITY_PROJECTIONS + 1;
const EXACT_GRAPH_RECORD_LIMIT: usize = 256;
const GRAPH_CANDIDATE_WINDOW: usize = 64;
/// Below this record count, computing PQ codes serially is cheaper than paying
/// thread-spawn overhead. Above it, the per-record encoding (which dominates
/// compaction wall-clock) is split across threads. The value only affects
/// scheduling, never the produced bytes.
const PQ_PARALLEL_RECORD_THRESHOLD: usize = 2048;

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

impl Segment {
    /// The width of the coarse-code columns (`pq_codes`/`pq_min`/`pq_max`).
    ///
    /// For [`QuantizerKind::ScalarBounds`](crate::record::QuantizerKind::ScalarBounds)
    /// this equals `dimensions` (one code per raw coordinate). For
    /// [`QuantizerKind::TurboQuant`](crate::record::QuantizerKind::TurboQuant) the
    /// SRHT rotation pads to the next power of two, so the codes and bounds live at
    /// `padded_len(dimensions)`. The persisted per-coordinate bounds and every
    /// code carry that same length, so it is read straight off `pq_min` (falling
    /// back to `dimensions` only for the degenerate empty-bounds case).
    pub(crate) fn coarse_code_len(&self) -> usize {
        if self.pq_min.is_empty() {
            self.dimensions
        } else {
            self.pq_min.len()
        }
    }
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
    /// Build a segment with the default ([`QuantizerKind::ScalarBounds`]) coarse
    /// quantizer. Preserves the historical behavior byte-for-byte; test and
    /// synthetic call sites use this.
    pub(crate) fn from_records(
        id: String,
        level: u8,
        metric: VectorMetric,
        dimensions: usize,
        records: Vec<VectorRecord>,
    ) -> Result<Self> {
        Self::from_records_with_quantizer(
            id,
            level,
            metric,
            dimensions,
            records,
            QuantizerKind::ScalarBounds,
        )
    }

    /// Build a segment with an explicit coarse [`QuantizerKind`]. Production build
    /// and compaction paths pass the index's persisted choice. Only the coarse
    /// codes (`pq_min`/`pq_max`/`pq_codes`) differ between quantizers; every other
    /// field, the routing summary, the sidecar, and the exact rerank are
    /// identical, so recall is protected regardless of the choice.
    pub(crate) fn from_records_with_quantizer(
        id: String,
        level: u8,
        metric: VectorMetric,
        dimensions: usize,
        records: Vec<VectorRecord>,
        quantizer: QuantizerKind,
    ) -> Result<Self> {
        if records.is_empty() {
            return Err(BorsukError::InvalidMetricInput(
                "segments must contain at least one record".to_string(),
            ));
        }

        let (centroid, radius) =
            crate::build_timing::timed(crate::build_timing::Phase::SegmentCentroidRadius, || {
                if metric.uses_normalized_euclidean_geometry() {
                    let normalized_vectors = records
                        .iter()
                        .map(|record| unit_l2_normalized(&record.vector))
                        .collect::<Vec<_>>();
                    let centroid = centroid_from_vectors(&normalized_vectors, dimensions)?;
                    // Stored (already-validated) vectors and a centroid derived
                    // from them: skip the redundant finite/dim re-scan on this O(n)
                    // radius pass (the metric's own degeneracy check is preserved).
                    let radius = normalized_vectors
                        .iter()
                        .map(|vector| {
                            metric.centroid_geometry_distance_unchecked(&centroid, vector)
                        })
                        .collect::<Result<Vec<_>>>()?
                        .into_iter()
                        .fold(0.0_f32, f32::max);
                    Ok::<_, BorsukError>((centroid, radius))
                } else {
                    let centroid = centroid(&records, dimensions)?;
                    let radius = records
                        .iter()
                        .map(|record| metric.distance_unchecked(&centroid, &record.vector))
                        .collect::<Result<Vec<_>>>()?
                        .into_iter()
                        .fold(0.0_f32, f32::max);
                    Ok((centroid, radius))
                }
            })?;
        let routing_codes =
            crate::build_timing::timed(crate::build_timing::Phase::SegmentRoutingCodes, || {
                records
                    .iter()
                    .map(|record| routing_code(&record.vector))
                    .collect::<Vec<_>>()
            });
        // TurboQuant reuses the pq_min/pq_max/pq_codes slots to store the ROTATED
        // per-coordinate bounds and codes. Its SRHT pads to the next power of two,
        // so those three columns hold `padded_len(dimensions)` entries; the segment
        // schema now sizes them to the segment's actual coarse-code width (see
        // `Segment::coarse_code_len`), so TurboQuant applies at every dimensionality
        // — no power-of-two fallback.
        let (pq_min, pq_max, pq_codes) = match quantizer.effective_for_dimensions(dimensions) {
            QuantizerKind::ScalarBounds => {
                let (pq_min, pq_max) = crate::build_timing::timed(
                    crate::build_timing::Phase::SegmentPqBounds,
                    || pq_bounds(&records, dimensions),
                )?;
                let pq_codes =
                    crate::build_timing::timed(crate::build_timing::Phase::SegmentPqEncode, || {
                        encode_pq_codes(&records, &pq_min, &pq_max)
                    });
                (pq_min, pq_max, pq_codes)
            }
            QuantizerKind::TurboQuant { seed, bits } => {
                turboquant_bounds_and_codes(&records, dimensions, seed, bits)?
            }
        };

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
        let edges = crate::build_timing::timed(crate::build_timing::Phase::GraphBuild, || {
            if max_neighbors == 0 {
                Ok(Vec::new())
            } else if segment.records.len() <= EXACT_GRAPH_RECORD_LIMIT {
                exact_graph_edges(segment, max_neighbors)
            } else {
                bounded_graph_edges(segment, max_neighbors)
            }
        })?;

        Ok(Self {
            segment_id: segment.id.clone(),
            level: segment.level,
            edges,
            created_at: segment.created_at,
        })
    }
}

/// Below this source-node count, the per-node graph search is cheap enough that
/// thread-spawn overhead is not worth paying. Above it, the independent per-node
/// work is split across threads. Only affects scheduling, never the bytes.
const GRAPH_PARALLEL_SOURCE_THRESHOLD: usize = 256;

/// Compute one output slot per source node in parallel and flatten in source
/// order. `per_source` is a pure function of `source_index` plus the read-only
/// shared inputs it captures, so each slot is written by exactly one thread and
/// keyed on its source index — the flattened result is byte-for-byte identical
/// to a serial `for source_index in 0..n` regardless of thread scheduling.
fn graph_edges_by_source<F>(source_count: usize, per_source: F) -> Result<Vec<GraphEdge>>
where
    F: Fn(usize) -> Result<Vec<GraphEdge>> + Sync,
{
    let mut per_source_edges: Vec<Result<Vec<GraphEdge>>> =
        (0..source_count).map(|_| Ok(Vec::new())).collect();

    let thread_count = if source_count < GRAPH_PARALLEL_SOURCE_THRESHOLD {
        1
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(source_count)
            .max(1)
    };

    if thread_count == 1 {
        for (source_index, slot) in per_source_edges.iter_mut().enumerate() {
            *slot = per_source(source_index);
        }
    } else {
        // Each worker owns a disjoint, contiguous slice of the output indexed by
        // source position, so no synchronization is needed and ordering is fixed.
        let per_source_ref = &per_source;
        let chunk_len = source_count.div_ceil(thread_count);
        std::thread::scope(|scope| {
            let mut base = 0_usize;
            let mut slot_rest = per_source_edges.as_mut_slice();
            while !slot_rest.is_empty() {
                let take = chunk_len.min(slot_rest.len());
                let (chunk, next) = slot_rest.split_at_mut(take);
                slot_rest = next;
                let start = base;
                base += take;
                scope.spawn(move || {
                    for (offset, slot) in chunk.iter_mut().enumerate() {
                        *slot = per_source_ref(start + offset);
                    }
                });
            }
        });
    }

    let total = per_source_edges
        .iter()
        .flatten()
        .map(|edges| edges.len())
        .sum();
    let mut edges = Vec::with_capacity(total);
    for slot in per_source_edges {
        edges.extend(slot?);
    }
    Ok(edges)
}

fn exact_graph_edges(segment: &Segment, max_neighbors: usize) -> Result<Vec<GraphEdge>> {
    graph_edges_by_source(segment.records.len(), |source_index| {
        let source = &segment.records[source_index];
        let mut neighbors = segment
            .records
            .iter()
            .enumerate()
            .filter(|(candidate_index, _)| *candidate_index != source_index)
            .map(|(candidate_index, candidate)| {
                // Both operands are stored, already-validated segment vectors, so
                // the finite/dim scan that would otherwise run O(n^2) times here is
                // pure waste — score through the unchecked SIMD kernel.
                Ok(GraphEdge {
                    source_record_index: source_index,
                    neighbor_record_index: candidate_index,
                    distance: segment
                        .metric
                        .distance_unchecked(&source.vector, &candidate.vector)?,
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
        Ok(neighbors)
    })
}

fn bounded_graph_edges(segment: &Segment, max_neighbors: usize) -> Result<Vec<GraphEdge>> {
    let locality_order = graph_locality_order(segment);
    let locality_positions = graph_positions_by_record_index(&locality_order);
    let routing_order = graph_routing_order(segment);
    let routing_positions = graph_positions_by_record_index(&routing_order);

    graph_edges_by_source(segment.records.len(), |source_index| {
        let source = &segment.records[source_index];
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
                // Stored, already-validated segment vectors on both sides — skip
                // the per-pair finite/dim re-scan in this O(n·candidates) loop.
                Ok(GraphEdge {
                    source_record_index: source_index,
                    neighbor_record_index: candidate_index,
                    distance: segment.metric.distance_unchecked(
                        &source.vector,
                        &segment.records[candidate_index].vector,
                    )?,
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
        Ok(neighbors)
    })
}

fn graph_locality_order(segment: &Segment) -> Vec<usize> {
    // Precompute each record's locality key once (each is an O(dim * projections)
    // pass). The comparator recomputed both keys on every one of the O(n log n)
    // comparisons, which dominated graph setup on high-dimensional data; caching
    // is a pure hoist and preserves the exact ordering and tie-breaks.
    let keys = graph_locality_keys(segment);
    let mut order = (0..segment.records.len()).collect::<Vec<_>>();
    order.sort_by(|left, right| {
        keys[*left]
            .cmp(&keys[*right])
            .then_with(|| segment.records[*left].id.cmp(&segment.records[*right].id))
            .then_with(|| left.cmp(right))
    });
    order
}

/// One locality key per record, computed in parallel above a size threshold
/// (each key is an independent, index-keyed pure function of its vector, so the
/// result is identical to a serial map regardless of scheduling).
fn graph_locality_keys(segment: &Segment) -> Vec<[i32; VECTOR_LOCALITY_KEY_LEN]> {
    let records = &segment.records;
    let mut keys = vec![[0_i32; VECTOR_LOCALITY_KEY_LEN]; records.len()];

    let thread_count = if records.len() < GRAPH_PARALLEL_SOURCE_THRESHOLD {
        1
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(records.len())
            .max(1)
    };

    if thread_count == 1 {
        for (record, slot) in records.iter().zip(keys.iter_mut()) {
            *slot = vector_locality_key(&record.vector);
        }
        return keys;
    }

    let chunk_len = records.len().div_ceil(thread_count);
    std::thread::scope(|scope| {
        let mut record_rest = records.as_slice();
        let mut key_rest = keys.as_mut_slice();
        while !record_rest.is_empty() {
            let take = chunk_len.min(record_rest.len());
            let (record_chunk, record_next) = record_rest.split_at(take);
            let (key_chunk, key_next) = key_rest.split_at_mut(take);
            record_rest = record_next;
            key_rest = key_next;
            scope.spawn(move || {
                for (record, slot) in record_chunk.iter().zip(key_chunk.iter_mut()) {
                    *slot = vector_locality_key(&record.vector);
                }
            });
        }
    });
    keys
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
    Ok(encode_pq_codes(records, &mins, &maxes))
}

/// A segment's coarse code payload: per-dimension `(min, max)` bounds plus one
/// code (byte vector) per record. Shared by both quantizer build paths.
type CoarseCodes = (Vec<f32>, Vec<f32>, Vec<Vec<u8>>);

/// Build TurboQuant rotated bounds and codes, packed into the segment's
/// `pq_min`/`pq_max`/`pq_codes` slots. The bounds are fit on ALL records (fit and
/// assign in one pass), so no codebook sampling is needed for this cut.
fn turboquant_bounds_and_codes(
    records: &[VectorRecord],
    dimensions: usize,
    seed: u64,
    bits: u8,
) -> Result<CoarseCodes> {
    for record in records {
        if record.vector.len() != dimensions {
            return Err(BorsukError::DimensionMismatch {
                expected: dimensions,
                actual: record.vector.len(),
            });
        }
    }
    let fit_vectors: Vec<Vec<f32>> = records.iter().map(|r| r.vector.clone()).collect();
    let ((mins, maxes), codes) = crate::build_timing::timed(
        crate::build_timing::Phase::SegmentPqBounds,
        || -> Result<_> {
            let quantizer = TurboQuantizer::fit(seed, dimensions, bits, &fit_vectors);
            let codes: Vec<Vec<u8>> =
                crate::build_timing::timed(crate::build_timing::Phase::SegmentPqEncode, || {
                    fit_vectors.iter().map(|v| quantizer.encode(v)).collect()
                });
            Ok((quantizer_bounds(&quantizer), codes))
        },
    )?;
    Ok((mins, maxes, codes))
}

/// Extract the fitted per-coordinate bounds from a [`TurboQuantizer`] for
/// persistence in a segment's `pq_min`/`pq_max` slots. TurboQuant re-derives the
/// quantizer from these bounds + the persisted seed at query time.
fn quantizer_bounds(quantizer: &TurboQuantizer) -> (Vec<f32>, Vec<f32>) {
    quantizer.persisted_bounds()
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

/// Encode a PQ code per record, one entry per record in input order.
///
/// Each record's code is an independent, pure function of its vector and the
/// shared `mins`/`maxes` bounds, so the work is embarrassingly parallel. When
/// there are enough records to amortize thread-spawn cost, the index range is
/// split into contiguous chunks and each worker writes its own disjoint slice
/// of a pre-sized output `Vec`. Because every output position is written by
/// exactly one thread and is keyed on the record's index (never pushed), the
/// result is byte-for-byte identical to the serial path regardless of how the
/// OS schedules the threads.
fn encode_pq_codes(records: &[VectorRecord], mins: &[f32], maxes: &[f32]) -> Vec<Vec<u8>> {
    if records.len() < PQ_PARALLEL_RECORD_THRESHOLD {
        return records
            .iter()
            .map(|record| pq_code_for_vector(&record.vector, mins, maxes))
            .collect();
    }

    let thread_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .min(records.len())
        .max(1);
    if thread_count == 1 {
        return records
            .iter()
            .map(|record| pq_code_for_vector(&record.vector, mins, maxes))
            .collect();
    }

    // Pre-size the output so each worker writes into a disjoint slice indexed by
    // record position. No thread ever pushes, so ordering is deterministic.
    let mut codes: Vec<Vec<u8>> = vec![Vec::new(); records.len()];
    let chunk_len = records.len().div_ceil(thread_count);

    std::thread::scope(|scope| {
        let mut record_rest = records;
        let mut code_rest = codes.as_mut_slice();
        while !record_rest.is_empty() {
            let take = chunk_len.min(record_rest.len());
            let (record_chunk, next_records) = record_rest.split_at(take);
            let (code_chunk, next_codes) = code_rest.split_at_mut(take);
            record_rest = next_records;
            code_rest = next_codes;
            scope.spawn(move || {
                for (record, slot) in record_chunk.iter().zip(code_chunk.iter_mut()) {
                    *slot = pq_code_for_vector(&record.vector, mins, maxes);
                }
            });
        }
    });

    codes
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

    /// An independent serial reference for `bounded_graph_edges`, written as a
    /// plain in-order loop with no threading. The parallel path must match this
    /// byte-for-byte.
    fn bounded_graph_edges_serial_reference(
        segment: &Segment,
        max_neighbors: usize,
    ) -> Vec<GraphEdge> {
        let locality_order = graph_locality_order(segment);
        let locality_positions = graph_positions_by_record_index(&locality_order);
        let routing_order = graph_routing_order(segment);
        let routing_positions = graph_positions_by_record_index(&routing_order);
        let mut edges = Vec::new();
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
                .map(|candidate_index| GraphEdge {
                    source_record_index: source_index,
                    neighbor_record_index: candidate_index,
                    distance: segment
                        .metric
                        .distance(&source.vector, &segment.records[candidate_index].vector)
                        .unwrap(),
                })
                .collect::<Vec<_>>();
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
        edges
    }

    /// The parallel per-source graph builder must produce byte-identical edges to
    /// an independent serial reference for a segment above the parallelism
    /// threshold. Guards the determinism guarantee: each source's edges land in a
    /// slot keyed on the source index and are flattened in order, so thread
    /// scheduling can never reorder or corrupt the graph.
    #[test]
    fn parallel_bounded_graph_edges_match_serial_reference_above_threshold() {
        let dimensions = 32;
        // Above both the exact-graph limit (so the bounded path runs) and the
        // parallelism threshold (so the parallel driver actually spawns threads).
        let record_count = EXACT_GRAPH_RECORD_LIMIT
            .max(GRAPH_PARALLEL_SOURCE_THRESHOLD)
            .saturating_add(1)
            .max(4000);

        // Deterministic, varied vectors via a splitmix-style hash (no RNG state).
        let records = (0..record_count)
            .map(|idx| {
                let vector = (0..dimensions)
                    .map(|dim| {
                        let mut h = ((idx as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15))
                            ^ ((dim as u64 + 1).wrapping_mul(0xbf58_476d_1ce4_e5b9));
                        h ^= h >> 30;
                        h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
                        h ^= h >> 27;
                        ((h % 20_000) as f32 - 10_000.0) / 137.0
                    })
                    .collect::<Vec<f32>>();
                VectorRecord::new(format!("doc-{idx:05}"), vector)
            })
            .collect::<Vec<_>>();

        let segment = Segment::from_records(
            "seg".to_string(),
            0,
            VectorMetric::Euclidean,
            dimensions,
            records,
        )
        .unwrap();

        let serial = bounded_graph_edges_serial_reference(&segment, 16);
        let parallel = bounded_graph_edges(&segment, 16).unwrap();

        assert_eq!(
            parallel.len(),
            serial.len(),
            "parallel graph must have the same edge count as the serial reference"
        );
        for (left, right) in parallel.iter().zip(&serial) {
            assert_eq!(left.source_record_index, right.source_record_index);
            assert_eq!(left.neighbor_record_index, right.neighbor_record_index);
            assert_eq!(
                left.distance.to_bits(),
                right.distance.to_bits(),
                "parallel graph edges must be byte-identical to the serial reference"
            );
        }

        // The public entry point must agree too.
        let graph = SegmentGraph::from_segment(&segment, 16).unwrap();
        assert_eq!(graph.edges.len(), serial.len());
        for (left, right) in graph.edges.iter().zip(&serial) {
            assert_eq!(left.source_record_index, right.source_record_index);
            assert_eq!(left.neighbor_record_index, right.neighbor_record_index);
            assert_eq!(left.distance.to_bits(), right.distance.to_bits());
        }
    }

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

    /// The parallel PQ-encoding path must produce byte-identical codes to a
    /// serial reference for a segment above the parallelism threshold. This
    /// guards the determinism guarantee: results are keyed on record index and
    /// written into disjoint slices of a pre-sized `Vec`, so thread scheduling
    /// can never reorder or corrupt the output.
    #[test]
    fn parallel_pq_codes_match_serial_reference_above_threshold() {
        let dimensions = 128;
        let record_count = 3000;
        assert!(
            record_count >= PQ_PARALLEL_RECORD_THRESHOLD,
            "test must exercise the parallel path"
        );

        // Deterministic, varied vectors (a splitmix-style hash, no RNG state).
        let records = (0..record_count)
            .map(|idx| {
                let vector = (0..dimensions)
                    .map(|dim| {
                        let mut h = ((idx as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15))
                            ^ ((dim as u64 + 1).wrapping_mul(0xbf58_476d_1ce4_e5b9));
                        h ^= h >> 30;
                        h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
                        h ^= h >> 27;
                        ((h % 20_000) as f32 - 10_000.0) / 137.0
                    })
                    .collect::<Vec<f32>>();
                VectorRecord::new(format!("doc-{idx:05}"), vector)
            })
            .collect::<Vec<_>>();

        let (mins, maxes) = pq_bounds(&records, dimensions).unwrap();

        // Serial reference, independent of the thread-chunking helper.
        let serial_reference = records
            .iter()
            .map(|record| pq_code_for_vector(&record.vector, &mins, &maxes))
            .collect::<Vec<_>>();

        // Public path used by compaction / from_records.
        let parallel = pq_codes_for_records(&records, dimensions).unwrap();

        assert_eq!(parallel.len(), serial_reference.len());
        assert_eq!(
            parallel, serial_reference,
            "parallel PQ codes must be byte-identical to the serial reference"
        );

        // The direct helper must agree too, at and just above the threshold.
        for &count in &[
            PQ_PARALLEL_RECORD_THRESHOLD,
            PQ_PARALLEL_RECORD_THRESHOLD + 1,
        ] {
            let slice = &records[..count];
            let helper = encode_pq_codes(slice, &mins, &maxes);
            let serial = slice
                .iter()
                .map(|record| pq_code_for_vector(&record.vector, &mins, &maxes))
                .collect::<Vec<_>>();
            assert_eq!(helper, serial, "helper diverged for {count} records");
        }
    }
}
