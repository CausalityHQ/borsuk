use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
};

use half::f16;
use rayon::{ThreadPool, ThreadPoolBuilder, prelude::*};
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result, v23_incidence::V23FmaBackend};

pub(crate) const V23_INCIDENCE_RESERVOIR_ROWS: usize = 2_097_152;
pub(crate) const V23_INCIDENCE_TREE_DEPTH: usize = 16;
pub(crate) const V23_INCIDENCE_LEAVES: usize = 65_536;
pub(crate) const V23_INCIDENCE_PROGRESS_SOURCE_ROWS: u64 = 262_144;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V23IncidenceTrainingShape {
    pub(crate) dimensions: usize,
    pub(crate) reservoir_rows: usize,
    pub(crate) depth: usize,
    pub(crate) lloyd_iterations: usize,
}

impl V23IncidenceTrainingShape {
    pub(crate) const PRODUCTION: Self = Self {
        dimensions: 96,
        reservoir_rows: V23_INCIDENCE_RESERVOIR_ROWS,
        depth: V23_INCIDENCE_TREE_DEPTH,
        lloyd_iterations: 4,
    };
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23TrainingRow {
    pub(crate) source_ordinal: u64,
    pub(crate) vector: [f32; 96],
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23ReservoirRow {
    pub(crate) source_ordinal: u64,
    pub(crate) vector: [f16; 96],
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23IncidenceReservoir {
    pub(crate) rows: Vec<V23ReservoirRow>,
    pub(crate) source_rows: u64,
    pub(crate) peak_rows: usize,
}

struct ReservoirEntry {
    key: u64,
    ordinal: u64,
    row: V23ReservoirRow,
}

impl PartialEq for ReservoirEntry {
    fn eq(&self, other: &Self) -> bool {
        (self.key, self.ordinal) == (other.key, other.ordinal)
    }
}

impl Eq for ReservoirEntry {}

impl PartialOrd for ReservoirEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReservoirEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.key, self.ordinal).cmp(&(other.key, other.ordinal))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23TreeNode {
    pub(crate) child_zero: [f16; 96],
    pub(crate) child_one: [f16; 96],
    pub(crate) child_zero_inverse_norm: f32,
    pub(crate) child_one_inverse_norm: f32,
    pub(crate) boundary_score_bits: u32,
    pub(crate) boundary_source_ordinal: u64,
    pub(crate) child_zero_index: u32,
    pub(crate) child_one_index: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23TreeLeaf {
    pub(crate) centroid: [f16; 96],
    pub(crate) inverse_norm: f32,
    pub(crate) population: u32,
    pub(crate) mean_squared_residual: f32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23IncidenceTree {
    pub(crate) shape: V23IncidenceTrainingShape,
    pub(crate) reservoir_seed: u64,
    pub(crate) work: V23TrainingWork,
    pub(crate) nodes: Vec<V23TreeNode>,
    pub(crate) leaves: Vec<V23TreeLeaf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BeamSelectedLeaves(pub(crate) [u16; 2]);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V23TrainingWork {
    pub(crate) farthest_seed_dimensions: u64,
    pub(crate) lloyd_dimensions: u64,
    pub(crate) repartition_dimensions: u64,
    pub(crate) total_distance_dimensions: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct V23TrainingExecution {
    worker_threads: usize,
    resident_workers: usize,
    workers_used: u32,
    batch_rows: usize,
    parallel_batches: u64,
}

#[derive(Debug, Clone, PartialEq)]
struct V23IncidenceTrainingOutcome {
    tree: V23IncidenceTree,
    execution: V23TrainingExecution,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V23IncidenceTrainingMilestone {
    SourceRows {
        completed_rows: u64,
    },
    Reservoir {
        source_rows: u64,
        reservoir_rows: u64,
    },
    TreeLevel {
        level: u64,
        completed_nodes: u64,
    },
}

fn select_reservoir_with_progress(
    rows: impl IntoIterator<Item = Result<V23TrainingRow>>,
    shape: V23IncidenceTrainingShape,
    seed: u64,
    expected_source_rows: u64,
    progress_block_rows: u64,
    progress: &mut impl FnMut(V23IncidenceTrainingMilestone) -> Result<()>,
) -> Result<V23IncidenceReservoir> {
    if expected_source_rows == 0 || progress_block_rows == 0 {
        return Err(invalid("V23 incidence progress row schedule differs"));
    }
    let mut completed_rows = 0_u64;
    let mut reported_rows = 0_u64;
    let reservoir = {
        let tracked_rows = rows.into_iter().map(|result| {
            let row = result?;
            completed_rows = completed_rows
                .checked_add(1)
                .ok_or_else(|| invalid("V23 incidence progress row count overflows"))?;
            if completed_rows > expected_source_rows {
                return Err(invalid("V23 incidence progress source rows differ"));
            }
            if completed_rows.is_multiple_of(progress_block_rows) {
                progress(V23IncidenceTrainingMilestone::SourceRows { completed_rows })?;
                reported_rows = completed_rows;
            }
            Ok(row)
        });
        select_reservoir_streaming(tracked_rows, shape, seed)?
    };
    if completed_rows != expected_source_rows {
        return Err(invalid("V23 incidence progress source rows differ"));
    }
    if reported_rows != completed_rows {
        progress(V23IncidenceTrainingMilestone::SourceRows { completed_rows })?;
    }
    Ok(reservoir)
}

struct TrainingContext {
    pool: ThreadPool,
    worker_threads: usize,
    batch_rows: usize,
    parallel_batches: AtomicU64,
    workers_used: AtomicU64,
}

impl TrainingContext {
    fn new(worker_threads: usize, batch_rows: usize) -> Result<Self> {
        if !(1..=64).contains(&worker_threads)
            || batch_rows == 0
            || rayon::current_thread_index().is_some()
        {
            return Err(invalid("V23 incidence execution shape differs"));
        }
        let pool = ThreadPoolBuilder::new()
            .num_threads(worker_threads)
            .thread_name(|index| format!("borsuk-v23-train-{index}"))
            .build()
            .map_err(|_| invalid("V23 incidence training pool differs"))?;
        Ok(Self {
            pool,
            worker_threads,
            batch_rows,
            parallel_batches: AtomicU64::new(0),
            workers_used: AtomicU64::new(0),
        })
    }

    fn record_worker(&self) {
        if let Some(index) = rayon::current_thread_index() {
            self.workers_used
                .fetch_or(1_u64 << index, AtomicOrdering::Relaxed);
        }
    }

    fn record_row_batch(&self) {
        self.parallel_batches.fetch_add(1, AtomicOrdering::Relaxed);
        self.record_worker();
    }

    fn map_batches<T, U, F>(&self, values: &[T], map: F) -> Vec<U>
    where
        T: Sync,
        U: Send,
        F: Fn(&[T]) -> U + Send + Sync,
    {
        if values.len() <= self.batch_rows {
            return vec![map(values)];
        }
        self.pool.install(|| {
            values
                .par_chunks(self.batch_rows)
                .map(|batch| {
                    self.record_row_batch();
                    map(batch)
                })
                .collect()
        })
    }

    fn map_items<T, U, F>(&self, values: &[T], map: F) -> Vec<U>
    where
        T: Sync,
        U: Send,
        F: Fn(&T) -> U + Send + Sync,
    {
        if values.len() <= 1 {
            return values.iter().map(map).collect();
        }
        self.pool.install(|| {
            values
                .par_iter()
                .map(|value| {
                    self.record_worker();
                    map(value)
                })
                .collect::<Vec<_>>()
        })
    }

    fn try_fill_batches<T, U, F>(&self, input: &[T], output: &mut [U], map: F) -> Result<()>
    where
        T: Sync,
        U: Send,
        F: Fn(&T) -> Result<U> + Send + Sync,
    {
        if input.len() != output.len() {
            return Err(invalid("V23 incidence training batch length differs"));
        }
        if input.len() <= self.batch_rows {
            for (input, output) in input.iter().zip(output) {
                *output = map(input)?;
            }
            return Ok(());
        }
        self.pool.install(|| {
            output
                .par_chunks_mut(self.batch_rows)
                .zip(input.par_chunks(self.batch_rows))
                .try_for_each(|(output, input)| {
                    self.record_row_batch();
                    for (input, output) in input.iter().zip(output) {
                        *output = map(input)?;
                    }
                    Ok(())
                })
        })
    }

    fn execution(&self) -> V23TrainingExecution {
        V23TrainingExecution {
            worker_threads: self.worker_threads,
            resident_workers: self.pool.current_num_threads(),
            workers_used: self.workers_used.load(AtomicOrdering::Relaxed).count_ones(),
            batch_rows: self.batch_rows,
            parallel_batches: self.parallel_batches.load(AtomicOrdering::Relaxed),
        }
    }
}

fn training_work(shape: V23IncidenceTrainingShape) -> Result<V23TrainingWork> {
    let rows = u64::try_from(shape.reservoir_rows)
        .map_err(|_| invalid("V23 incidence training work overflows"))?;
    let depth =
        u64::try_from(shape.depth).map_err(|_| invalid("V23 incidence training work overflows"))?;
    let dimensions = u64::try_from(shape.dimensions)
        .map_err(|_| invalid("V23 incidence training work overflows"))?;
    let iterations = u64::try_from(shape.lloyd_iterations)
        .map_err(|_| invalid("V23 incidence training work overflows"))?;
    let farthest_seed_dimensions = rows
        .checked_mul(depth)
        .and_then(|value| value.checked_mul(dimensions))
        .ok_or_else(|| invalid("V23 incidence training work overflows"))?;
    let lloyd_dimensions = farthest_seed_dimensions
        .checked_mul(iterations)
        .and_then(|value| value.checked_mul(2))
        .ok_or_else(|| invalid("V23 incidence training work overflows"))?;
    let repartition_dimensions = farthest_seed_dimensions
        .checked_mul(2)
        .ok_or_else(|| invalid("V23 incidence training work overflows"))?;
    let total_distance_dimensions = farthest_seed_dimensions
        .checked_add(lloyd_dimensions)
        .and_then(|value| value.checked_add(repartition_dimensions))
        .ok_or_else(|| invalid("V23 incidence training work overflows"))?;
    Ok(V23TrainingWork {
        farthest_seed_dimensions,
        lloyd_dimensions,
        repartition_dimensions,
        total_distance_dimensions,
    })
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_string())
}

fn decode_lower_hex_32(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid("V23 incidence source archive digest differs"));
    }
    let mut bytes = [0_u8; 32];
    for (index, output) in bytes.iter_mut().enumerate() {
        *output = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| invalid("V23 incidence source archive digest differs"))?;
    }
    Ok(bytes)
}

pub(crate) fn reservoir_seed(source_archive_sha256: &str) -> Result<u64> {
    let archive = decode_lower_hex_32(source_archive_sha256)?;
    let mut digest = Sha256::new();
    digest.update(archive);
    digest.update(b"borsuk-v23-leaf-page-v1");
    let bytes: [u8; 32] = digest.finalize().into();
    Ok(u64::from_le_bytes(bytes[..8].try_into().unwrap()))
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

fn normalized(vector: &[f32; 96]) -> Result<[f32; 96]> {
    let mut squared_norm = 0.0_f64;
    for value in vector {
        if !value.is_finite() {
            return Err(invalid("V23 incidence training vector is non-finite"));
        }
        squared_norm += f64::from(*value) * f64::from(*value);
    }
    if !squared_norm.is_finite() || squared_norm == 0.0 {
        return Err(invalid("V23 incidence training vector has zero norm"));
    }
    let inverse = (squared_norm.sqrt().recip()) as f32;
    Ok(vector.map(|value| value * inverse))
}

pub(crate) fn normalize_v23_incidence_vector(vector: &[f32; 96]) -> Result<[f32; 96]> {
    normalized(vector)
}

pub(crate) struct V23NormalizedRow([f32; 96]);

pub(crate) fn normalize_incidence_row(vector: &[f32; 96]) -> Result<V23NormalizedRow> {
    normalized(vector).map(V23NormalizedRow)
}

fn exact_dot(left: &[f32; 96], right: &[f32; 96]) -> f32 {
    let mut lanes = [0.0_f32; 8];
    for (lane, accumulator) in lanes.iter_mut().enumerate() {
        for step in 0..12 {
            let dimension = lane * 12 + step;
            *accumulator = left[dimension].mul_add(right[dimension], *accumulator);
        }
    }
    lanes.into_iter().fold(0.0_f32, |sum, value| sum + value)
}

fn training_dot(use_fused: bool, left: &[f32; 96], right: &[f32; 96]) -> Result<f32> {
    if use_fused {
        return borsuk_fma::fused_dot_8x12(left, right)
            .map(|(score, _)| score)
            .map_err(|_| invalid("V23 incidence fused SIMD backend is unavailable"));
    }
    Ok(exact_dot(left, right))
}

pub(crate) fn split_score_scalar(node: &V23TreeNode, row: &[f32; 96]) -> f32 {
    let zero = node.child_zero.map(f16::to_f32);
    let one = node.child_one.map(f16::to_f32);
    exact_dot(row, &one) * node.child_one_inverse_norm
        - exact_dot(row, &zero) * node.child_zero_inverse_norm
}

pub(crate) fn split_score_simd(
    node: &V23TreeNode,
    row: &[f32; 96],
) -> Result<(f32, V23FmaBackend)> {
    let zero = node.child_zero.map(f16::to_f32);
    let one = node.child_one.map(f16::to_f32);
    let (zero_dot, zero_backend) = borsuk_fma::fused_dot_8x12(row, &zero)
        .map_err(|_| invalid("V23 incidence fused SIMD backend is unavailable"))?;
    let (one_dot, one_backend) = borsuk_fma::fused_dot_8x12(row, &one)
        .map_err(|_| invalid("V23 incidence fused SIMD backend is unavailable"))?;
    if zero_backend != one_backend {
        return Err(invalid("V23 incidence fused SIMD backend differs"));
    }
    let backend = match zero_backend {
        borsuk_fma::FmaBackend::Aarch64NeonFma => V23FmaBackend::Aarch64NeonFma,
        borsuk_fma::FmaBackend::X86AvxFma => V23FmaBackend::X86AvxFma,
    };
    Ok((
        one_dot * node.child_one_inverse_norm - zero_dot * node.child_zero_inverse_norm,
        backend,
    ))
}

pub(crate) fn select_reservoir(
    rows: &[V23TrainingRow],
    shape: V23IncidenceTrainingShape,
    seed: u64,
) -> Result<Vec<V23ReservoirRow>> {
    let mut ordered = rows.to_vec();
    ordered.sort_unstable_by_key(|row| row.source_ordinal);
    Ok(select_reservoir_streaming(ordered.into_iter().map(Ok), shape, seed)?.rows)
}

pub(crate) fn select_reservoir_streaming(
    rows: impl IntoIterator<Item = Result<V23TrainingRow>>,
    shape: V23IncidenceTrainingShape,
    seed: u64,
) -> Result<V23IncidenceReservoir> {
    if shape.dimensions != 96
        || shape.reservoir_rows == 0
        || shape.depth == 0
        || shape.depth > 16
        || shape.lloyd_iterations != 4
        || shape.reservoir_rows < (1_usize << shape.depth)
    {
        return Err(invalid("V23 incidence training shape differs"));
    }

    let mut selected = BinaryHeap::with_capacity(shape.reservoir_rows);
    let mut previous_ordinal = None;
    let mut source_rows = 0_u64;
    let mut peak_rows = 0_usize;
    for row in rows {
        let row = row?;
        source_rows = source_rows
            .checked_add(1)
            .ok_or_else(|| invalid("V23 incidence source row count overflows"))?;
        if previous_ordinal.is_some_and(|previous| row.source_ordinal <= previous) {
            return Err(invalid("V23 incidence source ordinals are not increasing"));
        }
        previous_ordinal = Some(row.source_ordinal);
        let row = V23ReservoirRow {
            source_ordinal: row.source_ordinal,
            vector: normalized(&row.vector)?.map(f16::from_f32),
        };
        let candidate = ReservoirEntry {
            key: splitmix64(row.source_ordinal ^ seed),
            ordinal: row.source_ordinal,
            row,
        };
        if selected.len() < shape.reservoir_rows {
            selected.push(candidate);
        } else if selected
            .peek()
            .is_some_and(|worst| candidate.cmp(worst).is_lt())
        {
            selected.pop();
            selected.push(candidate);
        }
        peak_rows = peak_rows.max(selected.len());
    }
    if source_rows
        < u64::try_from(shape.reservoir_rows)
            .map_err(|_| invalid("V23 incidence reservoir row count overflows"))?
    {
        return Err(invalid("V23 incidence training shape differs"));
    }
    let mut rows = selected
        .into_iter()
        .map(|entry| entry.row)
        .collect::<Vec<_>>();
    rows.sort_unstable_by_key(|row| row.source_ordinal);
    Ok(V23IncidenceReservoir {
        rows,
        source_rows,
        peak_rows,
    })
}

fn reservoir_vector(row: &V23ReservoirRow) -> [f32; 96] {
    row.vector.map(f16::to_f32)
}

fn centroid(
    rows: &[usize],
    reservoir: &[V23ReservoirRow],
    context: &TrainingContext,
) -> Result<[f32; 96]> {
    if rows.is_empty() {
        return Err(invalid("V23 incidence tree node is empty"));
    }
    let mut ordered = rows.to_vec();
    ordered.sort_unstable_by_key(|index| reservoir[*index].source_ordinal);
    let sum_partial = |batch: &[usize]| {
        let mut partial = [0.0_f64; 96];
        for index in batch {
            for (sum, value) in partial.iter_mut().zip(reservoir_vector(&reservoir[*index])) {
                *sum += f64::from(value);
            }
        }
        partial
    };
    let mut partials = if ordered.len() <= 4096 {
        vec![sum_partial(&ordered)]
    } else {
        context.pool.install(|| {
            ordered
                .par_chunks(4096)
                .map(|batch| {
                    context.record_worker();
                    sum_partial(batch)
                })
                .collect::<Vec<_>>()
        })
    };
    partials.resize(partials.len().next_power_of_two(), [0.0_f64; 96]);
    while partials.len() > 1 {
        let mut merged = Vec::with_capacity(partials.len() / 2);
        for pair in partials.as_chunks::<2>().0 {
            let mut sum = [0.0_f64; 96];
            for dimension in 0..96 {
                sum[dimension] = pair[0][dimension] + pair[1][dimension];
            }
            merged.push(sum);
        }
        partials = merged;
    }
    let sums = partials.pop().unwrap();
    let vector = sums.map(|sum| (sum / rows.len() as f64) as f32);
    normalized(&vector)
}

fn roundtrip_centroid(vector: &[f32; 96]) -> Result<([f16; 96], f32)> {
    let encoded = vector.map(f16::from_f32);
    let decoded = encoded.map(f16::to_f32);
    let norm = exact_dot(&decoded, &decoded);
    if !norm.is_finite() || norm <= 0.0 {
        return Err(invalid("V23 incidence centroid norm differs"));
    }
    Ok((encoded, norm.sqrt().recip()))
}

fn train_split(
    members: &[usize],
    reservoir: &[V23ReservoirRow],
    shape: V23IncidenceTrainingShape,
    use_fused: bool,
    context: &TrainingContext,
) -> Result<(V23TreeNode, Vec<usize>, Vec<usize>)> {
    if members.len() < 2 {
        return Err(invalid("V23 incidence tree split is empty"));
    }
    let first = *members
        .iter()
        .min_by_key(|index| reservoir[**index].source_ordinal)
        .unwrap();
    let first_vector = reservoir_vector(&reservoir[first]);
    let farthest = context.map_batches(members, |batch| {
        let mut farthest = None;
        for index in batch.iter().filter(|index| **index != first) {
            let vector = reservoir_vector(&reservoir[*index]);
            let candidate = (
                1.0 - training_dot(use_fused, &first_vector, &vector)?,
                reservoir[*index].source_ordinal,
                *index,
            );
            if farthest.as_ref().is_none_or(|current: &(f32, u64, usize)| {
                candidate
                    .0
                    .total_cmp(&current.0)
                    .then_with(|| current.1.cmp(&candidate.1))
                    .is_gt()
            }) {
                farthest = Some(candidate);
            }
        }
        Ok(farthest)
    });
    let second = farthest
        .into_iter()
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .max_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| right.1.cmp(&left.1))
        })
        .ok_or_else(|| invalid("V23 incidence tree split is empty"))?
        .2;
    let mut zero = first_vector;
    let mut one = reservoir_vector(&reservoir[second]);
    for _ in 0..shape.lloyd_iterations {
        let mut assignments = vec![0_u8; members.len()];
        context.try_fill_batches(members, &mut assignments, |index| {
            let vector = reservoir_vector(&reservoir[*index]);
            let zero_distance = 1.0 - training_dot(use_fused, &vector, &zero)?;
            let one_distance = 1.0 - training_dot(use_fused, &vector, &one)?;
            Ok(u8::from(zero_distance.total_cmp(&one_distance).is_gt()))
        })?;
        let one_count = assignments
            .iter()
            .map(|assignment| *assignment as usize)
            .sum();
        let mut zero_members = Vec::with_capacity(members.len() - one_count);
        let mut one_members = Vec::with_capacity(one_count);
        for (index, assignment) in members.iter().zip(assignments) {
            if assignment == 0 {
                zero_members.push(*index);
            } else {
                one_members.push(*index);
            }
        }
        zero = centroid(&zero_members, reservoir, context)?;
        one = centroid(&one_members, reservoir, context)?;
    }
    let (child_zero, child_zero_inverse_norm) = roundtrip_centroid(&zero)?;
    let (child_one, child_one_inverse_norm) = roundtrip_centroid(&one)?;
    let placeholder = V23TreeNode {
        child_zero,
        child_one,
        child_zero_inverse_norm,
        child_one_inverse_norm,
        boundary_score_bits: 0,
        boundary_source_ordinal: 0,
        child_zero_index: 0,
        child_one_index: 0,
    };
    let mut scored = vec![(0.0_f32, 0_u64, 0_usize); members.len()];
    context.try_fill_batches(members, &mut scored, |index| {
        Ok((
            if use_fused {
                split_score_simd(&placeholder, &reservoir_vector(&reservoir[*index]))?.0
            } else {
                split_score_scalar(&placeholder, &reservoir_vector(&reservoir[*index]))
            },
            reservoir[*index].source_ordinal,
            *index,
        ))
    })?;
    scored.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    let midpoint = scored.len() / 2;
    let boundary = scored[midpoint - 1];
    let zero_members = scored[..midpoint].iter().map(|entry| entry.2).collect();
    let one_members = scored[midpoint..].iter().map(|entry| entry.2).collect();
    Ok((
        V23TreeNode {
            child_zero,
            child_one,
            child_zero_inverse_norm,
            child_one_inverse_norm,
            boundary_score_bits: boundary.0.to_bits(),
            boundary_source_ordinal: boundary.1,
            child_zero_index: 0,
            child_one_index: 0,
        },
        zero_members,
        one_members,
    ))
}

fn leaf(
    members: &[usize],
    reservoir: &[V23ReservoirRow],
    use_fused: bool,
    context: &TrainingContext,
) -> Result<V23TreeLeaf> {
    let center = centroid(members, reservoir, context)?;
    let (centroid, inverse_norm) = roundtrip_centroid(&center)?;
    let decoded = centroid.map(f16::to_f32);
    let mut residual = 0.0_f64;
    for index in members {
        let vector = reservoir_vector(&reservoir[*index]);
        let distance = 1.0 - training_dot(use_fused, &vector, &decoded)? * inverse_norm;
        residual += f64::from(distance * distance);
    }
    Ok(V23TreeLeaf {
        centroid,
        inverse_norm,
        population: u32::try_from(members.len())
            .map_err(|_| invalid("V23 incidence leaf population overflows"))?,
        mean_squared_residual: (residual / members.len() as f64) as f32,
    })
}

fn train_incidence_tree_with_shape(
    rows: &[V23TrainingRow],
    shape: V23IncidenceTrainingShape,
    threads: usize,
    batch_rows: usize,
) -> Result<V23IncidenceTree> {
    train_incidence_tree_internal(rows, shape, threads, batch_rows, false)
}

#[cfg(test)]
fn train_incidence_tree_with_shape_fused(
    rows: &[V23TrainingRow],
    shape: V23IncidenceTrainingShape,
    threads: usize,
    batch_rows: usize,
) -> Result<V23IncidenceTree> {
    train_incidence_tree_internal(rows, shape, threads, batch_rows, true)
}

fn train_incidence_tree_internal(
    rows: &[V23TrainingRow],
    shape: V23IncidenceTrainingShape,
    threads: usize,
    batch_rows: usize,
    use_fused: bool,
) -> Result<V23IncidenceTree> {
    Ok(
        train_incidence_tree_internal_with_execution(rows, shape, threads, batch_rows, use_fused)?
            .tree,
    )
}

fn train_incidence_tree_internal_with_execution(
    rows: &[V23TrainingRow],
    shape: V23IncidenceTrainingShape,
    threads: usize,
    batch_rows: usize,
    use_fused: bool,
) -> Result<V23IncidenceTrainingOutcome> {
    let seed = reservoir_seed("77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d")?;
    let reservoir = select_reservoir(rows, shape, seed)?;
    train_incidence_tree_from_reservoir_with_execution(
        reservoir, shape, seed, threads, batch_rows, use_fused, None,
    )
}

pub(crate) fn train_incidence_tree_from_reservoir(
    reservoir: Vec<V23ReservoirRow>,
    shape: V23IncidenceTrainingShape,
    seed: u64,
    threads: usize,
    batch_rows: usize,
    use_fused: bool,
) -> Result<V23IncidenceTree> {
    Ok(train_incidence_tree_from_reservoir_with_execution(
        reservoir, shape, seed, threads, batch_rows, use_fused, None,
    )?
    .tree)
}

fn train_incidence_tree_from_reservoir_with_execution(
    reservoir: Vec<V23ReservoirRow>,
    shape: V23IncidenceTrainingShape,
    seed: u64,
    threads: usize,
    batch_rows: usize,
    use_fused: bool,
    mut progress: Option<&mut dyn FnMut(V23IncidenceTrainingMilestone) -> Result<()>>,
) -> Result<V23IncidenceTrainingOutcome> {
    let context = TrainingContext::new(threads, batch_rows)?;
    if use_fused {
        borsuk_fma::fused_dot_8x12(&[0.0; 96], &[0.0; 96])
            .map_err(|_| invalid("V23 incidence fused SIMD backend is unavailable"))?;
    }
    let mut groups = vec![(0..reservoir.len()).collect::<Vec<_>>()];
    let node_count = (1_usize << shape.depth) - 1;
    let mut nodes = Vec::with_capacity(node_count);
    for level in 0..shape.depth {
        let splits = context
            .map_items(&groups, |group| {
                train_split(group, &reservoir, shape, use_fused, &context)
            })
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let mut next = Vec::with_capacity(splits.len() * 2);
        for (group_index, (mut node, zero, one)) in splits.into_iter().enumerate() {
            let child_base = (1_usize << (level + 1)) - 1 + group_index * 2;
            if level + 1 == shape.depth {
                node.child_zero_index = u32::try_from(node_count + group_index * 2)
                    .map_err(|_| invalid("V23 incidence tree index overflows"))?;
                node.child_one_index = u32::try_from(node_count + group_index * 2 + 1)
                    .map_err(|_| invalid("V23 incidence tree index overflows"))?;
            } else {
                node.child_zero_index = u32::try_from(child_base)
                    .map_err(|_| invalid("V23 incidence tree index overflows"))?;
                node.child_one_index = u32::try_from(child_base + 1)
                    .map_err(|_| invalid("V23 incidence tree index overflows"))?;
            }
            nodes.push(node);
            next.push(zero);
            next.push(one);
        }
        groups = next;
        if let Some(progress) = progress.as_deref_mut() {
            progress(V23IncidenceTrainingMilestone::TreeLevel {
                level: u64::try_from(level + 1)
                    .map_err(|_| invalid("V23 incidence progress level overflows"))?,
                completed_nodes: u64::try_from(nodes.len())
                    .map_err(|_| invalid("V23 incidence progress node count overflows"))?,
            })?;
        }
    }
    let leaves = context
        .map_items(&groups, |group| {
            leaf(group, &reservoir, use_fused, &context)
        })
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    let tree = V23IncidenceTree {
        shape,
        reservoir_seed: seed,
        work: training_work(shape)?,
        nodes,
        leaves,
    };
    Ok(V23IncidenceTrainingOutcome {
        tree,
        execution: context.execution(),
    })
}

#[cfg(test)]
fn train_incidence_tree_streaming_with_shape(
    rows: impl IntoIterator<Item = Result<V23TrainingRow>>,
    shape: V23IncidenceTrainingShape,
    seed: u64,
    threads: usize,
    batch_rows: usize,
) -> Result<V23IncidenceTree> {
    let reservoir = select_reservoir_streaming(rows, shape, seed)?.rows;
    train_incidence_tree_from_reservoir(reservoir, shape, seed, threads, batch_rows, false)
}

#[cfg(test)]
fn train_incidence_tree_with_shape_and_execution(
    rows: &[V23TrainingRow],
    shape: V23IncidenceTrainingShape,
    threads: usize,
    batch_rows: usize,
) -> Result<V23IncidenceTrainingOutcome> {
    train_incidence_tree_internal_with_execution(rows, shape, threads, batch_rows, false)
}

pub(crate) fn train_incidence_tree(
    rows: impl IntoIterator<Item = Result<V23TrainingRow>>,
    expected_source_rows: u64,
    threads: usize,
    batch_rows: usize,
    mut progress: impl FnMut(V23IncidenceTrainingMilestone) -> Result<()>,
) -> Result<V23IncidenceTree> {
    let shape = V23IncidenceTrainingShape::PRODUCTION;
    let seed = reservoir_seed("77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d")?;
    let reservoir = select_reservoir_with_progress(
        rows,
        shape,
        seed,
        expected_source_rows,
        V23_INCIDENCE_PROGRESS_SOURCE_ROWS,
        &mut progress,
    )?;
    progress(V23IncidenceTrainingMilestone::Reservoir {
        source_rows: reservoir.source_rows,
        reservoir_rows: u64::try_from(reservoir.rows.len())
            .map_err(|_| invalid("V23 incidence progress reservoir count overflows"))?,
    })?;
    Ok(train_incidence_tree_from_reservoir_with_execution(
        reservoir.rows,
        shape,
        seed,
        threads,
        batch_rows,
        true,
        Some(&mut progress),
    )?
    .tree)
}

#[cfg(test)]
pub(crate) fn train_incidence_tree_test_shape(
    rows: &[V23TrainingRow],
    shape: V23IncidenceTrainingShape,
    threads: usize,
    batch_rows: usize,
) -> Result<V23IncidenceTree> {
    train_incidence_tree_with_shape(rows, shape, threads, batch_rows)
}

#[cfg(test)]
pub(crate) fn train_incidence_tree_test_shape_with_progress(
    rows: impl IntoIterator<Item = Result<V23TrainingRow>>,
    expected_source_rows: u64,
    progress_block_rows: u64,
    shape: V23IncidenceTrainingShape,
    threads: usize,
    batch_rows: usize,
    mut progress: impl FnMut(V23IncidenceTrainingMilestone) -> Result<()>,
) -> Result<V23IncidenceTree> {
    let seed = reservoir_seed("77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d")?;
    let reservoir = select_reservoir_with_progress(
        rows,
        shape,
        seed,
        expected_source_rows,
        progress_block_rows,
        &mut progress,
    )?;
    progress(V23IncidenceTrainingMilestone::Reservoir {
        source_rows: reservoir.source_rows,
        reservoir_rows: u64::try_from(reservoir.rows.len())
            .map_err(|_| invalid("V23 incidence progress reservoir count overflows"))?,
    })?;
    Ok(train_incidence_tree_from_reservoir_with_execution(
        reservoir.rows,
        shape,
        seed,
        threads,
        batch_rows,
        false,
        Some(&mut progress),
    )?
    .tree)
}

fn take_zero(node: &V23TreeNode, score: f32, ordinal: u64) -> bool {
    score
        .total_cmp(&f32::from_bits(node.boundary_score_bits))
        .then_with(|| ordinal.cmp(&node.boundary_source_ordinal))
        .is_le()
}

pub(crate) fn assign_one_leaf(
    tree: &V23IncidenceTree,
    vector: &[f32; 96],
    source_ordinal: u64,
) -> Result<u16> {
    let row = normalize_incidence_row(vector)?;
    assign_one_leaf_normalized(tree, &row, source_ordinal)
}

pub(crate) fn assign_one_leaf_normalized(
    tree: &V23IncidenceTree,
    row: &V23NormalizedRow,
    source_ordinal: u64,
) -> Result<u16> {
    let node_count = tree.nodes.len();
    let mut index = 0_usize;
    while index < node_count {
        let node = &tree.nodes[index];
        let score = split_score_simd(node, &row.0)?.0;
        index = if take_zero(node, score, source_ordinal) {
            node.child_zero_index as usize
        } else {
            node.child_one_index as usize
        };
    }
    let leaf = index
        .checked_sub(node_count)
        .filter(|leaf| *leaf < tree.leaves.len())
        .ok_or_else(|| invalid("V23 incidence leaf index differs"))?;
    u16::try_from(leaf).map_err(|_| invalid("V23 incidence leaf index overflows"))
}

pub(crate) fn assign_two_beam_leaves(
    tree: &V23IncidenceTree,
    vector: &[f32; 96],
    source_ordinal: u64,
) -> Result<BeamSelectedLeaves> {
    let row = normalize_incidence_row(vector)?;
    assign_two_beam_leaves_normalized(tree, &row, source_ordinal)
}

#[derive(Debug, Clone, Copy)]
struct V23TreeBeamCandidate {
    distance: f32,
    global_index: u32,
}

impl PartialEq for V23TreeBeamCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance.to_bits() == other.distance.to_bits()
            && self.global_index == other.global_index
    }
}

impl Eq for V23TreeBeamCandidate {}

impl PartialOrd for V23TreeBeamCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for V23TreeBeamCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.global_index.cmp(&other.global_index))
    }
}

pub(crate) fn v23_tree_beam_centroid_scores_for_depth(
    depth: usize,
    beam_width: usize,
) -> Result<u32> {
    let shift =
        u32::try_from(depth).map_err(|_| invalid("V23 incidence tree-beam work exceeds u32"))?;
    let leaf_count = 1_usize
        .checked_shl(shift)
        .ok_or_else(|| invalid("V23 incidence tree-beam work overflows"))?;
    if ![32, 64, 128].contains(&beam_width)
        || depth == 0
        || depth > V23_INCIDENCE_TREE_DEPTH
        || beam_width > leaf_count
    {
        return Err(invalid("V23 incidence tree-beam width differs"));
    }
    let mut scores = 0_u32;
    for level in 1..=depth {
        let level_candidates = 1_usize
            .checked_shl(u32::try_from(level).unwrap())
            .ok_or_else(|| invalid("V23 incidence tree-beam work overflows"))?;
        scores = scores
            .checked_add(
                u32::try_from(level_candidates.min(beam_width * 2))
                    .map_err(|_| invalid("V23 incidence tree-beam work exceeds u32"))?,
            )
            .ok_or_else(|| invalid("V23 incidence tree-beam work overflows"))?;
    }
    Ok(scores)
}

pub(crate) fn v23_tree_beam_centroid_scores(beam_width: usize) -> Result<u32> {
    v23_tree_beam_centroid_scores_for_depth(V23_INCIDENCE_TREE_DEPTH, beam_width)
}

fn v23_tree_beam_child_distance(
    query: &[f32; 96],
    centroid: &[f16; 96],
    inverse_norm: f32,
    use_fused: bool,
) -> Result<f32> {
    if !inverse_norm.is_finite() || inverse_norm <= 0.0 {
        return Err(invalid("V23 incidence tree-beam inverse norm differs"));
    }
    let centroid = centroid.map(f16::to_f32);
    if centroid.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V23 incidence tree-beam centroid is non-finite"));
    }
    let distance = 1.0 - training_dot(use_fused, query, &centroid)? * inverse_norm;
    if !distance.is_finite() {
        return Err(invalid("V23 incidence tree-beam distance is non-finite"));
    }
    Ok(distance)
}

fn rank_v23_incidence_tree_beam_impl(
    tree: &V23IncidenceTree,
    query: &[f32; 96],
    beam_width: usize,
    use_fused: bool,
) -> Result<Vec<u16>> {
    if ![32, 64, 128].contains(&beam_width)
        || tree.shape.dimensions != 96
        || tree.shape.depth == 0
        || tree.shape.depth > V23_INCIDENCE_TREE_DEPTH
    {
        return Err(invalid("V23 incidence tree-beam shape differs"));
    }
    let expected_leaves = 1_usize
        .checked_shl(u32::try_from(tree.shape.depth).unwrap())
        .ok_or_else(|| invalid("V23 incidence tree-beam shape overflows"))?;
    let expected_nodes = expected_leaves - 1;
    if tree.nodes.len() != expected_nodes
        || tree.leaves.len() != expected_leaves
        || beam_width > expected_leaves
    {
        return Err(invalid("V23 incidence tree-beam topology differs"));
    }
    let query = normalized(query)?;
    let mut current = Vec::with_capacity(beam_width);
    let mut next = Vec::with_capacity(beam_width * 2);
    current.push(0_u32);
    for level in 0..tree.shape.depth {
        next.clear();
        for index in current.drain(..) {
            let node = tree
                .nodes
                .get(usize::try_from(index).unwrap())
                .ok_or_else(|| invalid("V23 incidence tree-beam node differs"))?;
            for (global_index, centroid, inverse_norm) in [
                (
                    node.child_zero_index,
                    &node.child_zero,
                    node.child_zero_inverse_norm,
                ),
                (
                    node.child_one_index,
                    &node.child_one,
                    node.child_one_inverse_norm,
                ),
            ] {
                next.push(V23TreeBeamCandidate {
                    distance: v23_tree_beam_child_distance(
                        &query,
                        centroid,
                        inverse_norm,
                        use_fused,
                    )?,
                    global_index,
                });
            }
        }
        next.sort_unstable();
        next.truncate(beam_width);
        if level + 1 < tree.shape.depth
            && next
                .iter()
                .any(|candidate| candidate.global_index as usize >= expected_nodes)
        {
            return Err(invalid("V23 incidence tree-beam child differs"));
        }
        current.extend(next.iter().map(|candidate| candidate.global_index));
    }
    if current.len() != beam_width {
        return Err(invalid("V23 incidence tree-beam leaves differ"));
    }
    current
        .into_iter()
        .map(|global_index| {
            let leaf = usize::try_from(global_index)
                .unwrap()
                .checked_sub(expected_nodes)
                .filter(|leaf| *leaf < expected_leaves)
                .ok_or_else(|| invalid("V23 incidence tree-beam leaf differs"))?;
            u16::try_from(leaf).map_err(|_| invalid("V23 incidence tree-beam leaf exceeds u16"))
        })
        .collect()
}

pub(crate) fn rank_v23_incidence_tree_beam(
    tree: &V23IncidenceTree,
    query: &[f32; 96],
    beam_width: usize,
) -> Result<Vec<u16>> {
    rank_v23_incidence_tree_beam_impl(tree, query, beam_width, true)
}

pub(crate) fn rank_v23_incidence_tree_beam_scalar(
    tree: &V23IncidenceTree,
    query: &[f32; 96],
    beam_width: usize,
) -> Result<Vec<u16>> {
    rank_v23_incidence_tree_beam_impl(tree, query, beam_width, false)
}

pub(crate) fn assign_two_beam_leaves_normalized(
    tree: &V23IncidenceTree,
    row: &V23NormalizedRow,
    _source_ordinal: u64,
) -> Result<BeamSelectedLeaves> {
    let node_count = tree.nodes.len();
    let mut candidates = vec![0_usize];
    for _ in 0..tree.shape.depth {
        let mut next = Vec::with_capacity(candidates.len() * 2);
        for index in candidates {
            let node = tree
                .nodes
                .get(index)
                .ok_or_else(|| invalid("V23 incidence beam node differs"))?;
            for (child, centroid, inverse_norm) in [
                (
                    node.child_zero_index as usize,
                    node.child_zero.map(f16::to_f32),
                    node.child_zero_inverse_norm,
                ),
                (
                    node.child_one_index as usize,
                    node.child_one.map(f16::to_f32),
                    node.child_one_inverse_norm,
                ),
            ] {
                let dot = borsuk_fma::fused_dot_8x12(&row.0, &centroid)
                    .map_err(|_| invalid("V23 incidence fused SIMD backend is unavailable"))?
                    .0;
                next.push((1.0 - dot * inverse_norm, child));
            }
        }
        next.sort_unstable_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        next.truncate(2);
        candidates = next.into_iter().map(|entry| entry.1).collect();
    }
    if candidates.len() != 2 {
        return Err(invalid("V23 incidence beam leaves differ"));
    }
    let mut leaves = [0_u16; 2];
    for (output, index) in leaves.iter_mut().zip(candidates) {
        *output = u16::try_from(
            index
                .checked_sub(node_count)
                .filter(|leaf| *leaf < tree.leaves.len())
                .ok_or_else(|| invalid("V23 incidence beam leaf differs"))?,
        )
        .map_err(|_| invalid("V23 incidence beam leaf overflows"))?;
    }
    Ok(BeamSelectedLeaves(leaves))
}

pub(crate) fn encode_incidence_tree(tree: &V23IncidenceTree) -> Result<Vec<u8>> {
    if !codec_shape_is_allowed(tree.shape)
        || tree.nodes.len() != (1_usize << tree.shape.depth) - 1
        || tree.leaves.len() != 1_usize << tree.shape.depth
        || tree.shape.reservoir_rows > u32::MAX as usize
    {
        return Err(invalid("V23 incidence tree shape differs"));
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"BVIT\x01\0\0\0");
    for value in [
        tree.shape.dimensions as u32,
        tree.shape.reservoir_rows as u32,
        tree.shape.depth as u32,
        tree.shape.lloyd_iterations as u32,
        tree.nodes.len() as u32,
        tree.leaves.len() as u32,
    ] {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes.extend_from_slice(&tree.reservoir_seed.to_le_bytes());
    for value in [
        tree.work.farthest_seed_dimensions,
        tree.work.lloyd_dimensions,
        tree.work.repartition_dimensions,
        tree.work.total_distance_dimensions,
    ] {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    for node in &tree.nodes {
        for value in node.child_zero.iter().chain(&node.child_one) {
            bytes.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        for value in [
            node.child_zero_inverse_norm.to_bits(),
            node.child_one_inverse_norm.to_bits(),
            node.boundary_score_bits,
            node.child_zero_index,
            node.child_one_index,
        ] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes.extend_from_slice(&node.boundary_source_ordinal.to_le_bytes());
    }
    for leaf in &tree.leaves {
        for value in leaf.centroid {
            bytes.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        bytes.extend_from_slice(&leaf.inverse_norm.to_bits().to_le_bytes());
        bytes.extend_from_slice(&leaf.population.to_le_bytes());
        bytes.extend_from_slice(&leaf.mean_squared_residual.to_bits().to_le_bytes());
    }
    Ok(bytes)
}

fn codec_shape_is_allowed(shape: V23IncidenceTrainingShape) -> bool {
    if shape == V23IncidenceTrainingShape::PRODUCTION {
        return true;
    }
    #[cfg(test)]
    {
        shape.dimensions == 96
            && shape.reservoir_rows >= (1_usize << shape.depth)
            && shape.depth > 0
            && shape.depth <= 16
            && shape.lloyd_iterations == 4
    }
    #[cfg(not(test))]
    {
        false
    }
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let value = bytes
        .get(*offset..*offset + 4)
        .ok_or_else(|| invalid("V23 incidence tree is truncated"))?;
    *offset += 4;
    Ok(u32::from_le_bytes(value.try_into().unwrap()))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let value = bytes
        .get(*offset..*offset + 8)
        .ok_or_else(|| invalid("V23 incidence tree is truncated"))?;
    *offset += 8;
    Ok(u64::from_le_bytes(value.try_into().unwrap()))
}

fn read_f16_plane(bytes: &[u8], offset: &mut usize) -> Result<[f16; 96]> {
    let mut values = [f16::ZERO; 96];
    for output in &mut values {
        let value = bytes
            .get(*offset..*offset + 2)
            .ok_or_else(|| invalid("V23 incidence tree is truncated"))?;
        *offset += 2;
        *output = f16::from_bits(u16::from_le_bytes(value.try_into().unwrap()));
    }
    Ok(values)
}

pub(crate) fn decode_incidence_tree(bytes: &[u8]) -> Result<V23IncidenceTree> {
    if bytes.get(..8) != Some(b"BVIT\x01\0\0\0") {
        return Err(invalid("V23 incidence tree header differs"));
    }
    let mut offset = 8;
    let shape = V23IncidenceTrainingShape {
        dimensions: read_u32(bytes, &mut offset)? as usize,
        reservoir_rows: read_u32(bytes, &mut offset)? as usize,
        depth: read_u32(bytes, &mut offset)? as usize,
        lloyd_iterations: read_u32(bytes, &mut offset)? as usize,
    };
    let node_count = read_u32(bytes, &mut offset)? as usize;
    let leaf_count = read_u32(bytes, &mut offset)? as usize;
    let seed = read_u64(bytes, &mut offset)?;
    let work = V23TrainingWork {
        farthest_seed_dimensions: read_u64(bytes, &mut offset)?,
        lloyd_dimensions: read_u64(bytes, &mut offset)?,
        repartition_dimensions: read_u64(bytes, &mut offset)?,
        total_distance_dimensions: read_u64(bytes, &mut offset)?,
    };
    let registered_seed =
        reservoir_seed("77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d")?;
    if !codec_shape_is_allowed(shape)
        || node_count != (1_usize << shape.depth) - 1
        || leaf_count != 1_usize << shape.depth
        || seed != registered_seed
        || work != training_work(shape)?
    {
        return Err(invalid("V23 incidence tree header authority differs"));
    }
    let mut nodes = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        let child_zero = read_f16_plane(bytes, &mut offset)?;
        let child_one = read_f16_plane(bytes, &mut offset)?;
        nodes.push(V23TreeNode {
            child_zero,
            child_one,
            child_zero_inverse_norm: f32::from_bits(read_u32(bytes, &mut offset)?),
            child_one_inverse_norm: f32::from_bits(read_u32(bytes, &mut offset)?),
            boundary_score_bits: read_u32(bytes, &mut offset)?,
            child_zero_index: read_u32(bytes, &mut offset)?,
            child_one_index: read_u32(bytes, &mut offset)?,
            boundary_source_ordinal: read_u64(bytes, &mut offset)?,
        });
    }
    let mut leaves = Vec::with_capacity(leaf_count);
    for _ in 0..leaf_count {
        leaves.push(V23TreeLeaf {
            centroid: read_f16_plane(bytes, &mut offset)?,
            inverse_norm: f32::from_bits(read_u32(bytes, &mut offset)?),
            population: read_u32(bytes, &mut offset)?,
            mean_squared_residual: f32::from_bits(read_u32(bytes, &mut offset)?),
        });
    }
    let topology_differs = nodes.iter().enumerate().any(|(index, node)| {
        let level = usize::BITS as usize - (index + 1).leading_zeros() as usize - 1;
        let level_start = (1_usize << level) - 1;
        let group_index = index - level_start;
        let (expected_zero, expected_one) = if level + 1 == shape.depth {
            (
                node_count + group_index * 2,
                node_count + group_index * 2 + 1,
            )
        } else {
            let child_base = (1_usize << (level + 1)) - 1 + group_index * 2;
            (child_base, child_base + 1)
        };
        node.child_zero_index as usize != expected_zero
            || node.child_one_index as usize != expected_one
    });
    let centroid_authority_differs = nodes.iter().any(|node| {
        let zero = node.child_zero.map(f16::to_f32);
        let one = node.child_one.map(f16::to_f32);
        zero.iter().chain(&one).any(|value| !value.is_finite())
            || exact_dot(&zero, &zero).sqrt().recip().to_bits()
                != node.child_zero_inverse_norm.to_bits()
            || exact_dot(&one, &one).sqrt().recip().to_bits()
                != node.child_one_inverse_norm.to_bits()
    }) || leaves.iter().any(|leaf| {
        let centroid = leaf.centroid.map(f16::to_f32);
        centroid.iter().any(|value| !value.is_finite())
            || exact_dot(&centroid, &centroid).sqrt().recip().to_bits()
                != leaf.inverse_norm.to_bits()
    });
    if offset != bytes.len()
        || topology_differs
        || centroid_authority_differs
        || leaves
            .iter()
            .map(|leaf| u64::from(leaf.population))
            .sum::<u64>()
            != shape.reservoir_rows as u64
        || nodes.iter().any(|node| {
            !node.child_zero_inverse_norm.is_finite()
                || node.child_zero_inverse_norm <= 0.0
                || !node.child_one_inverse_norm.is_finite()
                || node.child_one_inverse_norm <= 0.0
                || !f32::from_bits(node.boundary_score_bits).is_finite()
        })
        || leaves.iter().any(|leaf| {
            leaf.population == 0
                || !leaf.inverse_norm.is_finite()
                || leaf.inverse_norm <= 0.0
                || !leaf.mean_squared_residual.is_finite()
                || leaf.mean_squared_residual < 0.0
        })
    {
        return Err(invalid("V23 incidence tree authority differs"));
    }
    Ok(V23IncidenceTree {
        shape,
        reservoir_seed: seed,
        work,
        nodes,
        leaves,
    })
}

#[cfg(test)]
mod tests {
    use half::f16;

    use super::{
        BeamSelectedLeaves, V23IncidenceTrainingShape, V23ReservoirRow, V23TrainingRow,
        V23TreeNode, assign_one_leaf, assign_one_leaf_normalized, assign_two_beam_leaves,
        assign_two_beam_leaves_normalized, decode_incidence_tree, encode_incidence_tree,
        normalize_incidence_row, rank_v23_incidence_tree_beam, rank_v23_incidence_tree_beam_scalar,
        reservoir_seed, select_reservoir_streaming, split_score_scalar, split_score_simd,
        train_incidence_tree_streaming_with_shape, train_incidence_tree_with_shape,
        train_incidence_tree_with_shape_and_execution, train_incidence_tree_with_shape_fused,
        training_work, v23_tree_beam_centroid_scores,
    };

    fn row(ordinal: u64) -> V23TrainingRow {
        let mut vector = [0.0_f32; 96];
        for (dimension, value) in vector.iter_mut().enumerate() {
            let signed = ((ordinal.wrapping_mul(131) + dimension as u64 * 17) % 257) as i32 - 128;
            *value = signed as f32 / 129.0;
        }
        V23TrainingRow {
            source_ordinal: ordinal,
            vector,
        }
    }

    fn shape() -> V23IncidenceTrainingShape {
        V23IncidenceTrainingShape {
            dimensions: 96,
            reservoir_rows: 32,
            depth: 3,
            lloyd_iterations: 4,
        }
    }

    fn assert_normalized_f16_row(row: &V23ReservoirRow) {
        assert_eq!(row.vector[0].to_bits(), f16::from_f32(0.6).to_bits());
        assert_eq!(row.vector[1].to_bits(), f16::from_f32(0.8).to_bits());
    }

    #[test]
    fn v23_incidence_tree_streaming_reservoir_is_bounded_and_exact() {
        let shape = V23IncidenceTrainingShape {
            dimensions: 96,
            reservoir_rows: 4,
            depth: 2,
            lloyd_iterations: 4,
        };
        let rows = (0..8).map(row).collect::<Vec<_>>();
        let selected = select_reservoir_streaming(rows.iter().cloned().map(Ok), shape, 0).unwrap();

        assert_eq!(selected.source_rows, 8);
        assert_eq!(selected.peak_rows, 4);
        assert_eq!(
            selected
                .rows
                .iter()
                .map(|selected| selected.source_ordinal)
                .collect::<Vec<_>>(),
            vec![3, 4, 5, 7]
        );
    }

    #[test]
    fn v23_incidence_tree_progress_reports_reservoir_and_fixed_node_milestones() {
        let shape = shape();
        let rows = (0..64).map(row).collect::<Vec<_>>();
        let mut observed = Vec::new();

        let tree = super::train_incidence_tree_test_shape_with_progress(
            rows.iter().cloned().map(Ok),
            64,
            16,
            shape,
            1,
            16,
            |milestone| {
                observed.push(milestone);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(tree.nodes.len(), 7);
        assert_eq!(tree.leaves.len(), 8);
        assert_eq!(
            observed,
            vec![
                super::V23IncidenceTrainingMilestone::SourceRows { completed_rows: 16 },
                super::V23IncidenceTrainingMilestone::SourceRows { completed_rows: 32 },
                super::V23IncidenceTrainingMilestone::SourceRows { completed_rows: 48 },
                super::V23IncidenceTrainingMilestone::SourceRows { completed_rows: 64 },
                super::V23IncidenceTrainingMilestone::Reservoir {
                    source_rows: 64,
                    reservoir_rows: 32,
                },
                super::V23IncidenceTrainingMilestone::TreeLevel {
                    level: 1,
                    completed_nodes: 1,
                },
                super::V23IncidenceTrainingMilestone::TreeLevel {
                    level: 2,
                    completed_nodes: 3,
                },
                super::V23IncidenceTrainingMilestone::TreeLevel {
                    level: 3,
                    completed_nodes: 7,
                },
            ]
        );
    }

    #[test]
    fn v23_incidence_tree_reservoir_stores_normalized_f16_rows() {
        assert_eq!(std::mem::size_of::<V23ReservoirRow>(), 200);
        let shape = V23IncidenceTrainingShape {
            dimensions: 96,
            reservoir_rows: 4,
            depth: 2,
            lloyd_iterations: 4,
        };
        let mut first = [0.0_f32; 96];
        first[0] = 3.0;
        first[1] = 4.0;
        let mut rows = vec![V23TrainingRow {
            source_ordinal: 0,
            vector: first,
        }];
        for ordinal in 1..4 {
            let mut vector = [0.0_f32; 96];
            vector[ordinal as usize] = 1.0;
            rows.push(V23TrainingRow {
                source_ordinal: ordinal,
                vector,
            });
        }

        let selected = select_reservoir_streaming(rows.into_iter().map(Ok), shape, 0).unwrap();
        assert_normalized_f16_row(&selected.rows[0]);
    }

    #[test]
    fn v23_incidence_tree_streaming_reservoir_validates_every_source_row() {
        let shape = V23IncidenceTrainingShape {
            dimensions: 96,
            reservoir_rows: 4,
            depth: 2,
            lloyd_iterations: 4,
        };
        let mut duplicate = (0..8).map(row).collect::<Vec<_>>();
        duplicate[7].source_ordinal = duplicate[0].source_ordinal;
        assert!(select_reservoir_streaming(duplicate.into_iter().map(Ok), shape, 0).is_err());

        let out_of_order = (0..8).rev().map(row).map(Ok);
        assert!(select_reservoir_streaming(out_of_order, shape, 0).is_err());

        let mut nonfinite = (0..8).map(row).collect::<Vec<_>>();
        nonfinite[7].vector[0] = f32::NAN;
        assert!(select_reservoir_streaming(nonfinite.into_iter().map(Ok), shape, 0).is_err());

        let interrupted = (0..8).map(|ordinal| {
            if ordinal == 7 {
                Err(super::invalid("fixture stream failed"))
            } else {
                Ok(row(ordinal))
            }
        });
        assert!(select_reservoir_streaming(interrupted, shape, 0).is_err());
    }

    #[test]
    fn v23_incidence_tree_streaming_training_matches_the_slice_control() {
        let rows = (0..64).map(row).collect::<Vec<_>>();
        let expected = train_incidence_tree_with_shape(&rows, shape(), 1, 16).unwrap();
        let actual = train_incidence_tree_streaming_with_shape(
            rows.into_iter().map(Ok),
            shape(),
            expected.reservoir_seed,
            1,
            16,
        )
        .unwrap();
        assert_eq!(
            encode_incidence_tree(&actual).unwrap(),
            encode_incidence_tree(&expected).unwrap()
        );
    }

    #[test]
    fn v23_incidence_tree_is_byte_identical_across_order_batches_and_threads() {
        let work = training_work(V23IncidenceTrainingShape::PRODUCTION).unwrap();
        assert_eq!(work.farthest_seed_dimensions, 3_221_225_472);
        assert_eq!(work.lloyd_dimensions, 25_769_803_776);
        assert_eq!(work.repartition_dimensions, 6_442_450_944);
        assert_eq!(work.total_distance_dimensions, 35_433_480_192);

        let rows = (0..64).map(row).collect::<Vec<_>>();
        let left = train_incidence_tree_with_shape(&rows, shape(), 1, 16).unwrap();

        let mut reversed = rows.clone();
        reversed.reverse();
        let right = train_incidence_tree_with_shape(&reversed, shape(), 8, 7).unwrap();
        let fused = train_incidence_tree_with_shape_fused(&reversed, shape(), 2, 9).unwrap();
        assert_eq!(
            encode_incidence_tree(&left).unwrap(),
            encode_incidence_tree(&right).unwrap()
        );
        assert_eq!(
            encode_incidence_tree(&left).unwrap(),
            encode_incidence_tree(&fused).unwrap()
        );
        assert_eq!(
            decode_incidence_tree(&encode_incidence_tree(&left).unwrap()).unwrap(),
            left
        );

        let encoded = encode_incidence_tree(&left).unwrap();
        let mut changed = encoded.clone();
        changed[32] ^= 1;
        assert!(decode_incidence_tree(&changed).is_err());

        let mut changed = encoded.clone();
        changed[72..74].copy_from_slice(&f16::NAN.to_bits().to_le_bytes());
        assert!(decode_incidence_tree(&changed).is_err());

        let mut changed = encoded.clone();
        changed[468..472].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_incidence_tree(&changed).is_err());

        let mut changed = encoded;
        changed.push(0);
        assert!(decode_incidence_tree(&changed).is_err());

        let duplicate = [rows[0].clone(), rows[0].clone()];
        assert!(train_incidence_tree_with_shape(&duplicate, shape(), 1, 1).is_err());
        let mut nonfinite = rows;
        nonfinite[3].vector[7] = f32::NAN;
        assert!(train_incidence_tree_with_shape(&nonfinite, shape(), 1, 8).is_err());
    }

    #[test]
    fn v23_incidence_tree_parallel_execution_uses_registered_workers_and_batches() {
        let rows = (0..64).map(row).collect::<Vec<_>>();
        let control = train_incidence_tree_with_shape(&rows, shape(), 1, 16).unwrap();
        let parallel = train_incidence_tree_with_shape_and_execution(&rows, shape(), 8, 7).unwrap();

        assert_eq!(parallel.execution.worker_threads, 8);
        assert_eq!(parallel.execution.resident_workers, 8);
        assert!(parallel.execution.workers_used > 1);
        assert_eq!(parallel.execution.batch_rows, 7);
        assert_eq!(parallel.execution.parallel_batches, 114);
        assert_eq!(
            encode_incidence_tree(&parallel.tree).unwrap(),
            encode_incidence_tree(&control).unwrap()
        );
        assert!(train_incidence_tree_with_shape_and_execution(&rows, shape(), 65, 7).is_err());
    }

    #[test]
    fn v23_incidence_assignment_shared_normalization_matches_public_controls() {
        let rows = (0..64).map(row).collect::<Vec<_>>();
        let tree = train_incidence_tree_with_shape(&rows, shape(), 2, 11).unwrap();
        let input = &rows[9];
        let normalized = normalize_incidence_row(&input.vector).unwrap();

        assert_eq!(
            assign_one_leaf_normalized(&tree, &normalized, input.source_ordinal).unwrap(),
            assign_one_leaf(&tree, &input.vector, input.source_ordinal).unwrap()
        );
        assert_eq!(
            assign_two_beam_leaves_normalized(&tree, &normalized, input.source_ordinal).unwrap(),
            assign_two_beam_leaves(&tree, &input.vector, input.source_ordinal).unwrap()
        );

        assert!(normalize_incidence_row(&[0.0; 96]).is_err());
    }

    fn reference_dot(left: &[f32; 96], right: &[f32; 96]) -> f32 {
        let mut lanes = [0.0_f32; 8];
        for (lane, accumulator) in lanes.iter_mut().enumerate() {
            for step in 0..12 {
                let dimension = lane * 12 + step;
                *accumulator = left[dimension].mul_add(right[dimension], *accumulator);
            }
        }
        lanes.into_iter().fold(0.0_f32, |sum, value| sum + value)
    }

    fn reference_beam(tree: &super::V23IncidenceTree, vector: &[f32; 96]) -> [u16; 2] {
        let row = super::normalized(vector).unwrap();
        let node_count = tree.nodes.len();
        let mut candidates = vec![0_usize];
        for _ in 0..tree.shape.depth {
            let mut next = Vec::with_capacity(candidates.len() * 2);
            for index in candidates {
                let node = &tree.nodes[index];
                for (child, centroid, inverse_norm) in [
                    (
                        node.child_zero_index as usize,
                        node.child_zero.map(f16::to_f32),
                        node.child_zero_inverse_norm,
                    ),
                    (
                        node.child_one_index as usize,
                        node.child_one.map(f16::to_f32),
                        node.child_one_inverse_norm,
                    ),
                ] {
                    let distance = 1.0 - reference_dot(&row, &centroid) * inverse_norm;
                    next.push((distance, child));
                }
            }
            next.sort_unstable_by(|left, right| {
                left.0
                    .total_cmp(&right.0)
                    .then_with(|| left.1.cmp(&right.1))
            });
            next.truncate(2);
            candidates = next.into_iter().map(|entry| entry.1).collect();
        }
        candidates
            .into_iter()
            .map(|index| u16::try_from(index - node_count).unwrap())
            .collect::<Vec<_>>()
            .try_into()
            .unwrap()
    }

    fn tree_beam_fixture() -> super::V23IncidenceTree {
        let depth = 8_usize;
        let node_count = (1_usize << depth) - 1;
        let leaf_count = 1_usize << depth;
        let centroid = {
            let mut value = [f16::ZERO; 96];
            value[0] = f16::ONE;
            value
        };
        let nodes = (0..node_count)
            .map(|index| V23TreeNode {
                child_zero: centroid,
                child_one: centroid,
                child_zero_inverse_norm: 1.0,
                child_one_inverse_norm: 1.0,
                boundary_score_bits: 0.0_f32.to_bits(),
                boundary_source_ordinal: index as u64,
                child_zero_index: u32::try_from(index * 2 + 1).unwrap(),
                child_one_index: u32::try_from(index * 2 + 2).unwrap(),
            })
            .collect();
        let leaves = (0..leaf_count)
            .map(|_| super::V23TreeLeaf {
                centroid,
                inverse_norm: 1.0,
                population: 1,
                mean_squared_residual: 0.0,
            })
            .collect();
        super::V23IncidenceTree {
            shape: V23IncidenceTrainingShape {
                dimensions: 96,
                reservoir_rows: leaf_count,
                depth,
                lloyd_iterations: 4,
            },
            reservoir_seed: 1,
            work: super::V23TrainingWork {
                farthest_seed_dimensions: 0,
                lloyd_dimensions: 0,
                repartition_dimensions: 0,
                total_distance_dimensions: 0,
            },
            nodes,
            leaves,
        }
    }

    #[test]
    fn v23_tree_beam_work_is_exact_and_bounded() {
        assert_eq!(v23_tree_beam_centroid_scores(32).unwrap(), 766);
        assert_eq!(v23_tree_beam_centroid_scores(64).unwrap(), 1_406);
        assert_eq!(v23_tree_beam_centroid_scores(128).unwrap(), 2_558);
        assert!(v23_tree_beam_centroid_scores(0).is_err());
        assert!(v23_tree_beam_centroid_scores(31).is_err());
        assert!(v23_tree_beam_centroid_scores(129).is_err());
    }

    #[test]
    fn v23_tree_beam_orders_ties_and_matches_scalar() {
        let tree = tree_beam_fixture();
        let mut query = [0.0_f32; 96];
        query[0] = 1.0;
        for width in [32, 64, 128] {
            let actual = rank_v23_incidence_tree_beam(&tree, &query, width).unwrap();
            assert_eq!(
                actual,
                (0..u16::try_from(width).unwrap()).collect::<Vec<_>>()
            );
            assert_eq!(
                actual,
                rank_v23_incidence_tree_beam_scalar(&tree, &query, width).unwrap()
            );
        }

        assert!(rank_v23_incidence_tree_beam(&tree, &[0.0; 96], 32).is_err());

        let mut malformed = tree.clone();
        malformed.nodes[0].child_zero_index = u32::MAX;
        assert!(rank_v23_incidence_tree_beam(&malformed, &query, 32).is_err());

        let mut nonfinite = tree;
        nonfinite.nodes[0].child_zero_inverse_norm = f32::NAN;
        assert!(rank_v23_incidence_tree_beam(&nonfinite, &query, 32).is_err());
    }

    #[test]
    fn v23_incidence_tree_split_uses_exact_fma_boundary_and_beam_semantics() {
        let mut query = [0.0_f32; 96];
        let mut child_zero = [f16::ZERO; 96];
        let mut child_one = [f16::ZERO; 96];
        for dimension in 0..96 {
            query[dimension] = (dimension as f32 - 47.0) / 53.0;
            child_zero[dimension] = f16::from_f32((dimension as f32 + 1.0) / 101.0);
            child_one[dimension] = f16::from_f32((97.0 - dimension as f32) / 103.0);
        }
        let node = V23TreeNode {
            child_zero,
            child_one,
            child_zero_inverse_norm: 0.75,
            child_one_inverse_norm: 1.25,
            boundary_score_bits: 0.0_f32.to_bits(),
            boundary_source_ordinal: 17,
            child_zero_index: 1,
            child_one_index: 2,
        };
        let zero = node.child_zero.map(f16::to_f32);
        let one = node.child_one.map(f16::to_f32);
        let expected = reference_dot(&query, &one) * node.child_one_inverse_norm
            - reference_dot(&query, &zero) * node.child_zero_inverse_norm;
        assert_eq!(
            split_score_scalar(&node, &query).to_bits(),
            expected.to_bits()
        );
        let (optimized, backend) = split_score_simd(&node, &query).unwrap();
        assert_eq!(optimized.to_bits(), expected.to_bits());
        assert_ne!(backend, crate::v23_incidence::V23FmaBackend::ScalarControl);

        let rows = (0..64).map(row).collect::<Vec<_>>();
        let tree = train_incidence_tree_with_shape(&rows, shape(), 2, 11).unwrap();
        let leaf = assign_one_leaf(&tree, &rows[9].vector, rows[9].source_ordinal).unwrap();
        assert!(leaf < 8);
        for row in &rows {
            let BeamSelectedLeaves(pair) =
                assign_two_beam_leaves(&tree, &row.vector, row.source_ordinal).unwrap();
            assert_eq!(pair, reference_beam(&tree, &row.vector));
            assert_ne!(pair[0], pair[1]);
        }

        let seed =
            reservoir_seed("77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d")
                .unwrap();
        assert_eq!(
            seed,
            reservoir_seed("77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d")
                .unwrap()
        );
    }
}
