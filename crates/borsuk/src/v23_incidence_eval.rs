use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    sync::Arc,
    time::Instant,
};

use arrow_array::{Array, FixedSizeListArray, Int32Array};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use half::f16;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};

use crate::{
    BorsukError, Result,
    v23_diagnostic::read_v23_query_vectors,
    v23_incidence::canonical_json_value,
    v23_incidence_postings::{PostingAssignmentArm, V23PostingPlane, validate_posting_prefix},
    v23_incidence_tree::{V23_INCIDENCE_LEAVES, V23IncidenceTree},
};

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_string())
}

fn normalized_query(query: &[f32; 96]) -> Result<[f32; 96]> {
    let mut squared_norm = 0.0_f64;
    for value in query {
        if !value.is_finite() {
            return Err(invalid("V23 incidence query is non-finite"));
        }
        squared_norm += f64::from(*value) * f64::from(*value);
    }
    if !squared_norm.is_finite() || squared_norm == 0.0 {
        return Err(invalid("V23 incidence query norm differs"));
    }
    let inverse = squared_norm.sqrt().recip() as f32;
    Ok(query.map(|value| value * inverse))
}

fn read_incidence_query_cohort(
    bytes: &[u8],
    query_ordinals: &[u64],
    first_ordinal: u64,
    count: usize,
) -> Result<Vec<[f32; 96]>> {
    if query_ordinals.len() != count
        || query_ordinals
            .iter()
            .copied()
            .ne(first_ordinal..first_ordinal + count as u64)
    {
        return Err(invalid("V23 incidence query cohort differs"));
    }
    read_v23_query_vectors(bytes, query_ordinals, count)?
        .into_iter()
        .map(|query| {
            query
                .try_into()
                .map_err(|_| invalid("V23 incidence query dimensions differ"))
        })
        .collect()
}

pub(crate) fn read_v23_incidence_development_queries(bytes: &[u8]) -> Result<Vec<[f32; 96]>> {
    read_incidence_query_cohort(bytes, &(0..32).collect::<Vec<_>>(), 0, 32)
}

pub(crate) fn read_v23_incidence_holdout_queries(
    bytes: &[u8],
    query_ordinals: &[u64],
) -> Result<Vec<[f32; 96]>> {
    read_incidence_query_cohort(bytes, query_ordinals, 32, 128)
}

pub(crate) fn read_v23_incidence_holdout_neighbors(bytes: &[u8]) -> Result<Vec<(u32, Vec<u64>)>> {
    const PHYSICAL_ROWS: i64 = 10_000;
    const TRAIN_ROWS: i32 = 9_990_000;
    let expected_schema = Schema::new(vec![Field::new(
        "neighbors_id",
        DataType::FixedSizeList(Arc::new(Field::new("element", DataType::Int32, false)), 100),
        false,
    )]);
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    if builder.schema().as_ref() != &expected_schema
        || builder.metadata().file_metadata().num_rows() != PHYSICAL_ROWS
    {
        return Err(invalid("V23 incidence neighbor Parquet schema differs"));
    }
    let mut selected = Vec::with_capacity(128);
    let mut physical_row = 0_u32;
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 1 || batch.column(0).null_count() != 0 {
            return Err(invalid("V23 incidence neighbor Parquet columns differ"));
        }
        let lists = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V23 incidence neighbor Parquet list differs"))?;
        for row in 0..batch.num_rows() {
            if lists.is_null(row) {
                return Err(invalid("V23 incidence neighbor row is null"));
            }
            let values = lists.value(row);
            let values = values
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| invalid("V23 incidence neighbor values differ"))?;
            if values.len() != 100
                || values.null_count() != 0
                || values
                    .values()
                    .iter()
                    .any(|value| *value < 0 || *value >= TRAIN_ROWS)
            {
                return Err(invalid("V23 incidence neighbor authority differs"));
            }
            let ids = values
                .values()
                .iter()
                .map(|value| *value as u64)
                .collect::<Vec<_>>();
            if ids.iter().copied().collect::<BTreeSet<_>>().len() != 100 {
                return Err(invalid("V23 incidence neighbor IDs are not unique"));
            }
            if (32..160).contains(&physical_row) {
                selected.push((physical_row, ids));
            }
            physical_row = physical_row
                .checked_add(1)
                .ok_or_else(|| invalid("V23 incidence neighbor row count overflows"))?;
        }
    }
    if physical_row != PHYSICAL_ROWS as u32 || selected.len() != 128 {
        return Err(invalid("V23 incidence neighbor row count differs"));
    }
    Ok(selected)
}

#[derive(Debug, Clone, Copy)]
struct RankedLeaf {
    distance: f32,
    leaf: u16,
}

impl PartialEq for RankedLeaf {
    fn eq(&self, other: &Self) -> bool {
        self.distance.to_bits() == other.distance.to_bits() && self.leaf == other.leaf
    }
}

impl Eq for RankedLeaf {}

impl PartialOrd for RankedLeaf {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedLeaf {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.leaf.cmp(&other.leaf))
    }
}

fn leaf_distance(query: &[f32; 96], centroid: &[f16; 96], inverse_norm: f32) -> Result<f32> {
    if !inverse_norm.is_finite() || inverse_norm <= 0.0 {
        return Err(invalid("V23 incidence leaf inverse norm differs"));
    }
    let centroid = centroid.map(f16::to_f32);
    if centroid.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V23 incidence leaf centroid is non-finite"));
    }
    let dot = borsuk_fma::fused_dot_8x12(query, &centroid)
        .map_err(|_| invalid("V23 incidence fused SIMD backend is unavailable"))?
        .0;
    let distance = 1.0 - dot * inverse_norm;
    if !distance.is_finite() {
        return Err(invalid("V23 incidence leaf distance is non-finite"));
    }
    Ok(distance)
}

pub(crate) fn rank_incidence_leaves(
    tree: &V23IncidenceTree,
    query: &[f32; 96],
    probes: usize,
) -> Result<Vec<u16>> {
    if tree.leaves.len() != V23_INCIDENCE_LEAVES || ![32, 64, 128].contains(&probes) {
        return Err(invalid("V23 incidence leaf ranking shape differs"));
    }
    let query = normalized_query(query)?;
    let mut best = BinaryHeap::with_capacity(128);
    for (block, leaves) in tree.leaves.chunks(256).enumerate() {
        for (within, leaf) in leaves.iter().enumerate() {
            let ordinal = block * 256 + within;
            let candidate = RankedLeaf {
                distance: leaf_distance(&query, &leaf.centroid, leaf.inverse_norm)?,
                leaf: u16::try_from(ordinal)
                    .map_err(|_| invalid("V23 incidence leaf ordinal exceeds u16"))?,
            };
            if best.len() < 128 {
                best.push(candidate);
            } else if candidate < *best.peek().unwrap() {
                best.pop();
                best.push(candidate);
            }
        }
    }
    let mut ranked = best.into_vec();
    ranked.sort_unstable();
    ranked.truncate(probes);
    Ok(ranked.into_iter().map(|entry| entry.leaf).collect())
}

fn reciprocal_q32(rank: usize) -> Result<u64> {
    let denominator = (rank + 1) as u128;
    let numerator = 1_u128 << 32;
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    let rounded = quotient
        + u128::from(
            remainder > denominator / 2
                || (denominator.is_multiple_of(2)
                    && remainder == denominator / 2
                    && !quotient.is_multiple_of(2)),
        );
    u64::try_from(rounded).map_err(|_| invalid("V23 incidence reciprocal exceeds u64"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23IncidenceQueryEvidence {
    pub(crate) ranked_leaf_ordinals: Vec<u16>,
    pub(crate) page_ordinals: Vec<u32>,
    pub(crate) posting_visits: u32,
    pub(crate) touched_pages: u32,
    pub(crate) scalar_pages_equal: bool,
}

pub(crate) struct V23IncidenceQueryWorkspace {
    scores: Vec<u64>,
    epochs: Vec<u32>,
    touched: Vec<u32>,
    epoch: u32,
}

impl V23IncidenceQueryWorkspace {
    pub(crate) fn new(page_count: usize) -> Result<Self> {
        if page_count < 8 || page_count > u32::MAX as usize {
            return Err(invalid("V23 incidence workspace page count differs"));
        }
        Ok(Self {
            scores: vec![0; page_count],
            epochs: vec![0; page_count],
            touched: Vec::with_capacity(262_144),
            epoch: 0,
        })
    }

    fn begin_query(&mut self) {
        self.touched.clear();
        if let Some(next) = self.epoch.checked_add(1) {
            self.epoch = next;
        } else {
            self.epochs.fill(0);
            self.epoch = 1;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23IncidenceQueryTruth {
    pub(crate) query_ordinal: u32,
    pub(crate) ground_truth_page_assignments: Vec<Vec<u32>>,
    pub(crate) oracle_pages: Vec<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidenceQuality {
    pub(crate) query_count: u32,
    pub(crate) total_hits: u32,
    pub(crate) minimum_hits: u32,
    pub(crate) oracle_hits: u32,
    pub(crate) aggregate_recall_ppm: u64,
    pub(crate) minimum_query_recall_ppm: u64,
    pub(crate) oracle_attainment_ppm: u64,
    pub(crate) passed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidenceLayoutQuality {
    pub(crate) query_count: u32,
    pub(crate) total_oracle_hits: u32,
    pub(crate) minimum_oracle_hits: u32,
    pub(crate) aggregate_recall_ppm: u64,
    pub(crate) minimum_query_recall_ppm: u64,
    pub(crate) passed: bool,
}

pub(crate) fn recompute_v23_incidence_layout_quality(
    truth: &[V23IncidenceQueryTruth],
) -> Result<V23IncidenceLayoutQuality> {
    if truth.is_empty() {
        return Err(invalid("V23 incidence layout truth is empty"));
    }
    let mut total = 0_u64;
    let mut minimum = 10_u64;
    for expected in truth {
        if expected.ground_truth_page_assignments.len() != 10 || expected.oracle_pages.len() != 8 {
            return Err(invalid("V23 incidence layout truth shape differs"));
        }
        let pages = expected
            .oracle_pages
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if pages.len() != 8 {
            return Err(invalid("V23 incidence layout oracle pages differ"));
        }
        let hits = expected
            .ground_truth_page_assignments
            .iter()
            .filter(|assignments| assignments.iter().any(|page| pages.contains(page)))
            .count() as u64;
        total += hits;
        minimum = minimum.min(hits);
    }
    let query_count = u32::try_from(truth.len())
        .map_err(|_| invalid("V23 incidence layout query count exceeds u32"))?;
    let aggregate_recall_ppm = total * 1_000_000 / (u64::from(query_count) * 10);
    let minimum_query_recall_ppm = minimum * 100_000;
    Ok(V23IncidenceLayoutQuality {
        query_count,
        total_oracle_hits: u32::try_from(total)
            .map_err(|_| invalid("V23 incidence layout hits exceed u32"))?,
        minimum_oracle_hits: minimum as u32,
        aggregate_recall_ppm,
        minimum_query_recall_ppm,
        passed: aggregate_recall_ppm >= 985_000 && minimum_query_recall_ppm >= 900_000,
    })
}

pub(crate) fn recompute_v23_incidence_quality(
    selections: &[(u32, Vec<u32>)],
    truth: &[V23IncidenceQueryTruth],
    page_count: usize,
) -> Result<V23IncidenceQuality> {
    if selections.is_empty() || selections.len() != truth.len() || page_count < 8 {
        return Err(invalid("V23 incidence quality shape differs"));
    }
    let mut total_hits = 0_u64;
    let mut total_oracle_hits = 0_u64;
    let mut minimum_hits = 10_u64;
    for ((query_ordinal, selected), expected) in selections.iter().zip(truth) {
        let unique_selected = selected
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let unique_oracle = expected
            .oracle_pages
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        if *query_ordinal != expected.query_ordinal
            || selected.len() != 8
            || unique_selected.len() != 8
            || expected.oracle_pages.len() != 8
            || unique_oracle.len() != 8
            || selected
                .iter()
                .chain(&expected.oracle_pages)
                .any(|page| *page as usize >= page_count)
            || expected.ground_truth_page_assignments.len() != 10
        {
            return Err(invalid("V23 incidence query truth authority differs"));
        }
        let mut hits = 0_u64;
        let mut oracle_hits = 0_u64;
        for assignments in &expected.ground_truth_page_assignments {
            let unique = assignments
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>();
            if assignments.is_empty()
                || assignments.len() > 2
                || unique.len() != assignments.len()
                || assignments.iter().any(|page| *page as usize >= page_count)
            {
                return Err(invalid("V23 incidence neighbor page authority differs"));
            }
            hits += u64::from(
                assignments
                    .iter()
                    .any(|page| unique_selected.contains(page)),
            );
            oracle_hits += u64::from(assignments.iter().any(|page| unique_oracle.contains(page)));
        }
        if oracle_hits == 0 {
            return Err(invalid("V23 incidence query oracle has zero hits"));
        }
        total_hits += hits;
        total_oracle_hits += oracle_hits;
        minimum_hits = minimum_hits.min(hits);
    }
    let denominator = selections.len() as u64 * 10;
    let aggregate_recall_ppm = total_hits * 1_000_000 / denominator;
    let minimum_query_recall_ppm = minimum_hits * 100_000;
    let oracle_attainment_ppm = total_hits * 1_000_000 / total_oracle_hits;
    Ok(V23IncidenceQuality {
        query_count: u32::try_from(selections.len())
            .map_err(|_| invalid("V23 incidence query count exceeds u32"))?,
        total_hits: u32::try_from(total_hits)
            .map_err(|_| invalid("V23 incidence total hits exceed u32"))?,
        minimum_hits: u32::try_from(minimum_hits)
            .map_err(|_| invalid("V23 incidence minimum hits exceed u32"))?,
        oracle_hits: u32::try_from(total_oracle_hits)
            .map_err(|_| invalid("V23 incidence oracle hits exceed u32"))?,
        aggregate_recall_ppm,
        minimum_query_recall_ppm,
        oracle_attainment_ppm,
        passed: aggregate_recall_ppm >= 975_000
            && minimum_query_recall_ppm >= 800_000
            && oracle_attainment_ppm >= 995_000,
    })
}

fn exact_coverage_oracle(assignments: &[Vec<u32>]) -> Result<Vec<u32>> {
    let candidates = assignments
        .iter()
        .flatten()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if candidates.len() < 8 || candidates.len() > 20 {
        return Err(invalid("V23 incidence oracle candidate count differs"));
    }
    fn visit(
        candidates: &[u32],
        assignments: &[Vec<u32>],
        start: usize,
        selected: &mut Vec<u32>,
        best: &mut Option<(usize, Vec<u32>)>,
    ) {
        if selected.len() == 8 {
            let hits = assignments
                .iter()
                .filter(|pages| {
                    pages
                        .iter()
                        .any(|page| selected.binary_search(page).is_ok())
                })
                .count();
            if best.as_ref().is_none_or(|current| hits > current.0) {
                *best = Some((hits, selected.clone()));
            }
            return;
        }
        let needed = 8 - selected.len();
        for index in start..=candidates.len() - needed {
            selected.push(candidates[index]);
            visit(candidates, assignments, index + 1, selected, best);
            selected.pop();
        }
    }
    let mut best = None;
    visit(
        &candidates,
        assignments,
        0,
        &mut Vec::with_capacity(8),
        &mut best,
    );
    best.map(|entry| entry.1)
        .ok_or_else(|| invalid("V23 incidence oracle is absent"))
}

pub(crate) fn bind_v23_incidence_holdout_truth(
    neighbors: &[(u32, Vec<u64>)],
    page_assignments: &BTreeMap<u64, Vec<u32>>,
    page_count: usize,
) -> Result<Vec<V23IncidenceQueryTruth>> {
    if neighbors.len() != 128
        || neighbors.iter().map(|entry| u64::from(entry.0)).ne(32..160)
        || page_count < 8
    {
        return Err(invalid("V23 incidence holdout neighbor cohort differs"));
    }
    neighbors
        .iter()
        .map(|(query_ordinal, ids)| {
            if ids.len() != 100 || ids.iter().copied().collect::<BTreeSet<_>>().len() != 100 {
                return Err(invalid("V23 incidence holdout neighbor IDs differ"));
            }
            let all_assignments = ids
                .iter()
                .map(|id| {
                    let pages = page_assignments
                        .get(id)
                        .ok_or_else(|| invalid("V23 incidence neighbor page is unbound"))?;
                    if pages.is_empty()
                        || pages.len() > 2
                        || pages.iter().copied().collect::<BTreeSet<_>>().len() != pages.len()
                        || pages.iter().any(|page| *page as usize >= page_count)
                    {
                        return Err(invalid("V23 incidence neighbor page assignment differs"));
                    }
                    Ok(pages.clone())
                })
                .collect::<Result<Vec<_>>>()?;
            let ground_truth_page_assignments = all_assignments[..10].to_vec();
            let oracle_pages = exact_coverage_oracle(&ground_truth_page_assignments)?;
            Ok(V23IncidenceQueryTruth {
                query_ordinal: *query_ordinal,
                ground_truth_page_assignments,
                oracle_pages,
            })
        })
        .collect()
}

fn selected_pages_q32(
    plane: &V23PostingPlane,
    ranked: &[u16],
    cap: usize,
    workspace: &mut V23IncidenceQueryWorkspace,
) -> Result<(Vec<u32>, u32, u32)> {
    workspace.begin_query();
    let mut visits = 0_u32;
    for (rank, leaf) in ranked.iter().copied().enumerate() {
        let reciprocal = reciprocal_q32(rank)?;
        let postings = &plane.leaves[usize::from(leaf)];
        for (&page, &mass) in postings.pages.iter().zip(&postings.masses).take(cap) {
            let page_index = usize::try_from(page)
                .ok()
                .filter(|page| *page < workspace.scores.len())
                .ok_or_else(|| invalid("V23 incidence posting page is out of range"))?;
            visits = visits
                .checked_add(1)
                .ok_or_else(|| invalid("V23 incidence posting visits overflow"))?;
            if workspace.epochs[page_index] != workspace.epoch {
                workspace.epochs[page_index] = workspace.epoch;
                workspace.scores[page_index] = 0;
                workspace.touched.push(page);
                if workspace.touched.len() > 262_144 {
                    return Err(invalid("V23 incidence touched workspace exceeded"));
                }
            }
            let contribution = u64::from(mass)
                .checked_mul(reciprocal)
                .ok_or_else(|| invalid("V23 incidence page contribution overflows"))?;
            workspace.scores[page_index] =
                workspace.scores[page_index]
                    .checked_add(contribution)
                    .ok_or_else(|| invalid("V23 incidence page score overflows"))?;
        }
    }
    workspace.touched.sort_unstable_by(|left, right| {
        workspace.scores[*right as usize]
            .cmp(&workspace.scores[*left as usize])
            .then_with(|| left.cmp(right))
    });
    if workspace.touched.len() < 8 {
        return Err(invalid("V23 incidence cannot select eight pages"));
    }
    let touched_pages = workspace.touched.len() as u32;
    Ok((workspace.touched[..8].to_vec(), visits, touched_pages))
}

fn selected_pages_scalar(
    plane: &V23PostingPlane,
    ranked: &[u16],
    cap: usize,
    page_count: usize,
) -> Result<Vec<u32>> {
    let mut scores = vec![0.0_f64; page_count];
    let mut touched = vec![false; page_count];
    for (rank, leaf) in ranked.iter().copied().enumerate() {
        let postings = &plane.leaves[usize::from(leaf)];
        for (&page, &mass) in postings.pages.iter().zip(&postings.masses).take(cap) {
            let page_index = usize::try_from(page)
                .ok()
                .filter(|page| *page < page_count)
                .ok_or_else(|| invalid("V23 incidence scalar page is out of range"))?;
            touched[page_index] = true;
            scores[page_index] += f64::from(mass) / 65_535.0 / (rank + 1) as f64;
        }
    }
    let mut pages = touched
        .into_iter()
        .enumerate()
        .filter_map(|(page, touched)| touched.then_some(page as u32))
        .collect::<Vec<_>>();
    pages.sort_unstable_by(|left, right| {
        scores[*right as usize]
            .total_cmp(&scores[*left as usize])
            .then_with(|| left.cmp(right))
    });
    if pages.len() < 8 {
        return Err(invalid("V23 incidence scalar cannot select eight pages"));
    }
    pages.truncate(8);
    Ok(pages)
}

pub(crate) fn score_incidence_query(
    tree: &V23IncidenceTree,
    plane: &V23PostingPlane,
    cell: V23IncidenceCell,
    query: &[f32; 96],
    page_count: usize,
) -> Result<V23IncidenceQueryEvidence> {
    if cell.arm != plane.arm
        || ![512, 1024, 2048].contains(&cell.cap)
        || ![32, 64, 128].contains(&cell.probes)
        || page_count < 8
    {
        return Err(invalid("V23 incidence query cell differs"));
    }
    validate_posting_prefix(plane, usize::from(cell.cap))?;
    let ranked_leaf_ordinals = rank_incidence_leaves(tree, query, usize::from(cell.probes))?;
    let mut workspace = V23IncidenceQueryWorkspace::new(page_count)?;
    let (page_ordinals, posting_visits, touched_pages) = selected_pages_q32(
        plane,
        &ranked_leaf_ordinals,
        usize::from(cell.cap),
        &mut workspace,
    )?;
    let scalar = selected_pages_scalar(
        plane,
        &ranked_leaf_ordinals,
        usize::from(cell.cap),
        page_count,
    )?;
    if scalar != page_ordinals {
        return Err(invalid("V23 incidence scalar and optimized pages differ"));
    }
    Ok(V23IncidenceQueryEvidence {
        ranked_leaf_ordinals,
        scalar_pages_equal: true,
        page_ordinals,
        posting_visits,
        touched_pages,
    })
}

pub(crate) fn score_incidence_query_native(
    tree: &V23IncidenceTree,
    plane: &V23PostingPlane,
    cell: V23IncidenceCell,
    query: &[f32; 96],
    workspace: &mut V23IncidenceQueryWorkspace,
) -> Result<V23IncidenceQueryEvidence> {
    if cell.arm != plane.arm
        || ![512, 1024, 2048].contains(&cell.cap)
        || ![32, 64, 128].contains(&cell.probes)
    {
        return Err(invalid("V23 incidence native query cell differs"));
    }
    validate_posting_prefix(plane, usize::from(cell.cap))?;
    let ranked_leaf_ordinals = rank_incidence_leaves(tree, query, usize::from(cell.probes))?;
    let (page_ordinals, posting_visits, touched_pages) = selected_pages_q32(
        plane,
        &ranked_leaf_ordinals,
        usize::from(cell.cap),
        workspace,
    )?;
    Ok(V23IncidenceQueryEvidence {
        ranked_leaf_ordinals,
        page_ordinals,
        posting_visits,
        touched_pages,
        scalar_pages_equal: false,
    })
}

const LATENCY_MAGIC: &[u8; 8] = b"BVIL\x01\0\0\0";

pub(crate) fn v23_incidence_latency_p99_ns(samples: &[u64]) -> Result<u64> {
    if samples.len() < 10_000 || samples.contains(&0) {
        return Err(invalid("V23 incidence latency samples differ"));
    }
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    let rank = 99_usize
        .checked_mul(ordered.len())
        .and_then(|value| value.checked_add(99))
        .map(|value| value / 100)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| invalid("V23 incidence latency rank overflows"))?;
    Ok(ordered[rank])
}

pub(crate) fn encode_v23_incidence_latency_samples(samples: &[u64]) -> Result<Vec<u8>> {
    v23_incidence_latency_p99_ns(samples)?;
    let mut bytes = Vec::with_capacity(16 + samples.len() * 8 + 32);
    bytes.extend_from_slice(LATENCY_MAGIC);
    bytes.extend_from_slice(
        &u64::try_from(samples.len())
            .map_err(|_| invalid("V23 incidence latency count exceeds u64"))?
            .to_le_bytes(),
    );
    for sample in samples {
        bytes.extend_from_slice(&sample.to_le_bytes());
    }
    let digest = blake3::hash(&bytes);
    bytes.extend_from_slice(digest.as_bytes());
    Ok(bytes)
}

pub(crate) fn decode_v23_incidence_latency_samples(bytes: &[u8]) -> Result<Vec<u64>> {
    if bytes.len() < 48 || bytes.get(..8) != Some(LATENCY_MAGIC) {
        return Err(invalid("V23 incidence latency header differs"));
    }
    let (body, claimed_digest) = bytes.split_at(bytes.len() - 32);
    if blake3::hash(body).as_bytes() != claimed_digest {
        return Err(invalid("V23 incidence latency checksum differs"));
    }
    let count = u64::from_le_bytes(
        body.get(8..16)
            .ok_or_else(|| invalid("V23 incidence latency count is absent"))?
            .try_into()
            .unwrap(),
    );
    let count =
        usize::try_from(count).map_err(|_| invalid("V23 incidence latency count exceeds usize"))?;
    if body.len() != 16_usize.saturating_add(count.saturating_mul(8)) {
        return Err(invalid("V23 incidence latency length differs"));
    }
    let samples = body[16..]
        .chunks_exact(8)
        .map(|sample| u64::from_le_bytes(sample.try_into().unwrap()))
        .collect::<Vec<_>>();
    v23_incidence_latency_p99_ns(&samples)?;
    Ok(samples)
}

pub(crate) fn measure_v23_incidence_latency(
    mut invocation: impl FnMut() -> Result<()>,
) -> Result<Vec<u8>> {
    for _ in 0..1_024 {
        invocation()?;
    }
    let mut samples = Vec::with_capacity(10_000);
    for _ in 0..10_000 {
        let started = Instant::now();
        invocation()?;
        let elapsed = u64::try_from(started.elapsed().as_nanos())
            .map_err(|_| invalid("V23 incidence latency sample exceeds u64"))?
            .max(1);
        samples.push(elapsed);
    }
    encode_v23_incidence_latency_samples(&samples)
}

#[cfg(test)]
fn rank_incidence_leaves_scalar(
    tree: &V23IncidenceTree,
    query: &[f32; 96],
    probes: usize,
) -> Result<Vec<u16>> {
    if tree.leaves.len() != V23_INCIDENCE_LEAVES || ![32, 64, 128].contains(&probes) {
        return Err(invalid("V23 incidence scalar leaf ranking shape differs"));
    }
    let query = normalized_query(query)?;
    let mut ranked = tree
        .leaves
        .iter()
        .enumerate()
        .map(|(leaf, value)| {
            Ok(RankedLeaf {
                distance: leaf_distance(&query, &value.centroid, value.inverse_norm)?,
                leaf: u16::try_from(leaf)
                    .map_err(|_| invalid("V23 incidence leaf ordinal exceeds u16"))?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    ranked.sort_unstable();
    ranked.truncate(probes);
    Ok(ranked.into_iter().map(|entry| entry.leaf).collect())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidenceCell {
    pub(crate) cap: u16,
    pub(crate) arm: PostingAssignmentArm,
    pub(crate) probes: u16,
}

impl V23IncidenceCell {
    pub(crate) fn registered_ladder() -> Vec<Self> {
        let mut cells = Vec::with_capacity(18);
        for cap in [512, 1024, 2048] {
            for arm in [
                PostingAssignmentArm::OneLeaf,
                PostingAssignmentArm::TwoBeamLeaves,
            ] {
                for probes in [32, 64, 128] {
                    cells.push(Self { cap, arm, probes });
                }
            }
        }
        cells
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidenceSelection {
    pub(crate) query_ordinal: u32,
    pub(crate) page_ordinals: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidenceCellResult {
    pub(crate) cell: V23IncidenceCell,
    pub(crate) retention_passed: bool,
    pub(crate) quality: V23IncidenceQuality,
    pub(crate) projected_serving_bytes: u64,
    pub(crate) maximum_posting_visits: u32,
    pub(crate) maximum_touched_pages: u32,
    pub(crate) p99_ns: u64,
    pub(crate) determinism_passed: bool,
    pub(crate) latency_blake3: String,
    pub(crate) latency_bytes: u64,
    pub(crate) selections: Vec<V23IncidenceSelection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidenceHoldoutResult {
    pub(crate) cell: V23IncidenceCell,
    pub(crate) quality: V23IncidenceQuality,
    pub(crate) projected_serving_bytes: u64,
    pub(crate) maximum_posting_visits: u32,
    pub(crate) maximum_touched_pages: u32,
    pub(crate) p99_ns: u64,
    pub(crate) determinism_passed: bool,
    pub(crate) latency_blake3: String,
    pub(crate) latency_bytes: u64,
    pub(crate) selections: Vec<V23IncidenceSelection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidenceCampaignInput {
    pub(crate) authority_passed: bool,
    pub(crate) resource_passed: bool,
    pub(crate) determinism_passed: bool,
    pub(crate) development: Vec<V23IncidenceCellResult>,
    pub(crate) holdout_layout: V23IncidenceLayoutQuality,
    pub(crate) holdout: Option<V23IncidenceHoldoutResult>,
}

impl V23IncidenceCampaignInput {
    #[cfg(test)]
    fn passing_fixture() -> Self {
        let latency = encode_v23_incidence_latency_samples(&vec![15_000_000; 10_000]).unwrap();
        Self::passing_fixture_for_latency(&latency)
    }

    #[cfg(test)]
    fn passing_fixture_for_latency(latency: &[u8]) -> Self {
        let latency_blake3 = blake3::hash(latency).to_hex().to_string();
        let latency_bytes = latency.len() as u64;
        let quality = V23IncidenceQuality {
            query_count: 32,
            total_hits: 320,
            minimum_hits: 10,
            oracle_hits: 320,
            aggregate_recall_ppm: 1_000_000,
            minimum_query_recall_ppm: 1_000_000,
            oracle_attainment_ppm: 1_000_000,
            passed: true,
        };
        let development_selections = (0..32)
            .map(|query_ordinal| V23IncidenceSelection {
                query_ordinal,
                page_ordinals: (0..8).collect(),
            })
            .collect();
        let holdout_selections = (32..160)
            .map(|query_ordinal| V23IncidenceSelection {
                query_ordinal,
                page_ordinals: (0..8).collect(),
            })
            .collect();
        Self {
            authority_passed: true,
            resource_passed: true,
            determinism_passed: true,
            development: vec![V23IncidenceCellResult {
                cell: V23IncidenceCell::registered_ladder()[0],
                retention_passed: true,
                quality,
                projected_serving_bytes: 1_119_235_716,
                maximum_posting_visits: 16_384,
                maximum_touched_pages: 8_192,
                p99_ns: 15_000_000,
                determinism_passed: true,
                latency_blake3: latency_blake3.clone(),
                latency_bytes,
                selections: development_selections,
            }],
            holdout_layout: V23IncidenceLayoutQuality {
                query_count: 128,
                total_oracle_hits: 1_280,
                minimum_oracle_hits: 10,
                aggregate_recall_ppm: 1_000_000,
                minimum_query_recall_ppm: 1_000_000,
                passed: true,
            },
            holdout: Some(V23IncidenceHoldoutResult {
                cell: V23IncidenceCell::registered_ladder()[0],
                quality: V23IncidenceQuality {
                    query_count: 128,
                    total_hits: 1_280,
                    oracle_hits: 1_280,
                    ..quality
                },
                projected_serving_bytes: 1_119_235_716,
                maximum_posting_visits: 16_384,
                maximum_touched_pages: 8_192,
                p99_ns: 15_000_000,
                determinism_passed: true,
                latency_blake3,
                latency_bytes,
                selections: holdout_selections,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum V23IncidenceCampaignClass {
    #[serde(rename = "authority-stop")]
    AuthorityStop,
    #[serde(rename = "resource-stop")]
    ResourceStop,
    #[serde(rename = "determinism-stop")]
    DeterminismStop,
    #[serde(rename = "incidence-retention-rejected")]
    RetentionRejected,
    #[serde(rename = "incidence-quality-rejected")]
    QualityRejected,
    #[serde(rename = "incidence-budget-rejected")]
    BudgetRejected,
    #[serde(rename = "incidence-kernel-rejected")]
    KernelRejected,
    #[serde(rename = "holdout-layout-rejected")]
    HoldoutLayoutRejected,
    #[serde(rename = "incidence-generalization-rejected")]
    GeneralizationRejected,
    #[serde(rename = "incidence-holdout-budget-rejected")]
    HoldoutBudgetRejected,
    #[serde(rename = "incidence-holdout-kernel-rejected")]
    HoldoutKernelRejected,
    #[serde(rename = "incidence-falsifier-passed")]
    FalsifierPassed,
}

pub(crate) fn classify_v23_incidence_campaign(
    input: &V23IncidenceCampaignInput,
) -> V23IncidenceCampaignClass {
    if !input.authority_passed {
        return V23IncidenceCampaignClass::AuthorityStop;
    }
    if !input.resource_passed {
        return V23IncidenceCampaignClass::ResourceStop;
    }
    if !input.determinism_passed {
        return V23IncidenceCampaignClass::DeterminismStop;
    }
    if input
        .development
        .iter()
        .any(|result| !result.determinism_passed)
    {
        return V23IncidenceCampaignClass::DeterminismStop;
    }
    let eligible = input
        .development
        .iter()
        .filter(|result| result.retention_passed)
        .collect::<Vec<_>>();
    if eligible.is_empty() {
        return V23IncidenceCampaignClass::RetentionRejected;
    }
    let quality_passed = |result: &&V23IncidenceCellResult| result.quality.passed;
    let structural_passed = |result: &&V23IncidenceCellResult| {
        result.projected_serving_bytes <= 3 * 1024 * 1024 * 1024
            && result.maximum_posting_visits <= 262_144
            && result.maximum_touched_pages <= 8_192
    };
    let kernel_passed = |result: &&V23IncidenceCellResult| result.p99_ns <= 15_000_000;
    let sealed = eligible.iter().copied().find(|result| {
        quality_passed(result) && structural_passed(result) && kernel_passed(result)
    });
    if sealed.is_none() {
        if !eligible.iter().any(quality_passed) {
            return V23IncidenceCampaignClass::QualityRejected;
        }
        if !eligible
            .iter()
            .any(|result| quality_passed(result) && structural_passed(result))
        {
            return V23IncidenceCampaignClass::BudgetRejected;
        }
        return V23IncidenceCampaignClass::KernelRejected;
    }
    if !input.holdout_layout.passed {
        return V23IncidenceCampaignClass::HoldoutLayoutRejected;
    }
    let Some(holdout) = &input.holdout else {
        return V23IncidenceCampaignClass::AuthorityStop;
    };
    if holdout.cell != sealed.unwrap().cell {
        return V23IncidenceCampaignClass::AuthorityStop;
    }
    if !holdout.determinism_passed {
        return V23IncidenceCampaignClass::DeterminismStop;
    }
    if !holdout.quality.passed {
        return V23IncidenceCampaignClass::GeneralizationRejected;
    }
    if holdout.projected_serving_bytes > 3 * 1024 * 1024 * 1024
        || holdout.maximum_posting_visits > 262_144
        || holdout.maximum_touched_pages > 8_192
    {
        return V23IncidenceCampaignClass::HoldoutBudgetRejected;
    }
    if holdout.p99_ns > 15_000_000 {
        return V23IncidenceCampaignClass::HoldoutKernelRejected;
    }
    V23IncidenceCampaignClass::FalsifierPassed
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidenceCampaignResult {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) source_commit: String,
    pub(crate) source_archive_sha256: String,
    pub(crate) index_id: String,
    pub(crate) dataset_id: String,
    pub(crate) query_cohort_sha256: String,
    pub(crate) tree_blake3: String,
    pub(crate) posting_blake3: String,
    pub(crate) executable_sha256: String,
    pub(crate) campaign: V23IncidenceCampaignInput,
    pub(crate) sealed_cell: Option<V23IncidenceCell>,
    pub(crate) classification: V23IncidenceCampaignClass,
    pub(crate) page_body_reads: u64,
}

impl V23IncidenceCampaignResult {
    #[cfg(test)]
    fn passing_fixture(latency: &[u8]) -> Self {
        Self {
            schema: "borsuk-v23-incidence-result-v1".to_string(),
            claim_eligible: false,
            source_commit: "1".repeat(40),
            source_archive_sha256: "2".repeat(64),
            index_id: "index-fixture".to_string(),
            dataset_id: "deep-image-96".to_string(),
            query_cohort_sha256: "3".repeat(64),
            tree_blake3: "4".repeat(64),
            posting_blake3: "5".repeat(64),
            executable_sha256: "6".repeat(64),
            campaign: V23IncidenceCampaignInput::passing_fixture_for_latency(latency),
            sealed_cell: Some(V23IncidenceCell::registered_ladder()[0]),
            classification: V23IncidenceCampaignClass::FalsifierPassed,
            page_body_reads: 0,
        }
    }
}

fn exact_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_quality(quality: V23IncidenceQuality, expected_queries: u32) -> Result<()> {
    let denominator = u64::from(quality.query_count)
        .checked_mul(10)
        .ok_or_else(|| invalid("V23 incidence quality denominator overflows"))?;
    if quality.query_count != expected_queries
        || denominator == 0
        || quality.minimum_hits > 10
        || u64::from(quality.total_hits) > denominator
        || quality.oracle_hits == 0
        || quality.total_hits > quality.oracle_hits
    {
        return Err(invalid("V23 incidence quality counts differ"));
    }
    let aggregate = u64::from(quality.total_hits) * 1_000_000 / denominator;
    let minimum = u64::from(quality.minimum_hits) * 100_000;
    let attainment = u64::from(quality.total_hits) * 1_000_000 / u64::from(quality.oracle_hits);
    let passed = aggregate >= 975_000 && minimum >= 800_000 && attainment >= 995_000;
    if quality.aggregate_recall_ppm != aggregate
        || quality.minimum_query_recall_ppm != minimum
        || quality.oracle_attainment_ppm != attainment
        || quality.passed != passed
    {
        return Err(invalid("V23 incidence quality evidence differs"));
    }
    Ok(())
}

fn validate_layout_quality(layout: V23IncidenceLayoutQuality) -> Result<()> {
    if layout.query_count != 128
        || layout.minimum_oracle_hits > 10
        || layout.total_oracle_hits > 1_280
    {
        return Err(invalid("V23 incidence layout quality counts differ"));
    }
    let aggregate = u64::from(layout.total_oracle_hits) * 1_000_000 / 1_280;
    let minimum = u64::from(layout.minimum_oracle_hits) * 100_000;
    let passed = aggregate >= 985_000 && minimum >= 900_000;
    if layout.aggregate_recall_ppm != aggregate
        || layout.minimum_query_recall_ppm != minimum
        || layout.passed != passed
    {
        return Err(invalid("V23 incidence layout quality evidence differs"));
    }
    Ok(())
}

fn validate_latency_binding(
    p99_ns: u64,
    digest: &str,
    claimed_bytes: u64,
    artifact: &[u8],
) -> Result<()> {
    let samples = decode_v23_incidence_latency_samples(artifact)?;
    if p99_ns != v23_incidence_latency_p99_ns(&samples)?
        || claimed_bytes != artifact.len() as u64
        || digest != blake3::hash(artifact).to_hex().as_str()
    {
        return Err(invalid("V23 incidence latency evidence differs"));
    }
    Ok(())
}

pub(crate) fn canonical_v23_incidence_result_bytes(
    result: &V23IncidenceCampaignResult,
    latency_artifacts: &[&[u8]],
    development_truth: &[V23IncidenceQueryTruth],
    holdout_truth: &[V23IncidenceQueryTruth],
) -> Result<Vec<u8>> {
    if result.schema != "borsuk-v23-incidence-result-v1"
        || result.claim_eligible
        || result.page_body_reads != 0
        || !exact_lower_hex(&result.source_commit, 40)
        || !exact_lower_hex(&result.source_archive_sha256, 64)
        || !exact_lower_hex(&result.query_cohort_sha256, 64)
        || !exact_lower_hex(&result.tree_blake3, 64)
        || !exact_lower_hex(&result.posting_blake3, 64)
        || !exact_lower_hex(&result.executable_sha256, 64)
        || result.index_id.is_empty()
        || result.dataset_id.is_empty()
    {
        return Err(invalid("V23 incidence result authority differs"));
    }
    let expected_artifacts =
        result.campaign.development.len() + usize::from(result.campaign.holdout.is_some());
    if latency_artifacts.len() != expected_artifacts
        || result.campaign.development.is_empty()
        || result.campaign.development.len() > 18
    {
        return Err(invalid("V23 incidence result artifact count differs"));
    }
    let layout = recompute_v23_incidence_layout_quality(holdout_truth)?;
    if layout != result.campaign.holdout_layout {
        return Err(invalid("V23 incidence holdout layout evidence differs"));
    }
    validate_layout_quality(result.campaign.holdout_layout)?;
    let ladder = V23IncidenceCell::registered_ladder();
    let mut prior_position = None;
    for (index, cell) in result.campaign.development.iter().enumerate() {
        let position = ladder
            .iter()
            .position(|registered| *registered == cell.cell)
            .ok_or_else(|| invalid("V23 incidence development cell is unregistered"))?;
        if prior_position.is_some_and(|prior| position <= prior) {
            return Err(invalid("V23 incidence development cell order differs"));
        }
        prior_position = Some(position);
        let selections = cell
            .selections
            .iter()
            .map(|selection| (selection.query_ordinal, selection.page_ordinals.clone()))
            .collect::<Vec<_>>();
        let quality = recompute_v23_incidence_quality(&selections, development_truth, 28_282)?;
        if quality != cell.quality {
            return Err(invalid(
                "V23 incidence development quality evidence differs",
            ));
        }
        validate_quality(cell.quality, 32)?;
        let projection = project_v23_incidence_serving_bytes(100_000_000, cell.cell.cap as usize)?;
        if cell.projected_serving_bytes != projection.total_bytes
            || cell.maximum_posting_visits > u32::from(cell.cell.cap) * u32::from(cell.cell.probes)
            || cell.maximum_touched_pages > cell.maximum_posting_visits
        {
            return Err(invalid("V23 incidence development budget evidence differs"));
        }
        validate_latency_binding(
            cell.p99_ns,
            &cell.latency_blake3,
            cell.latency_bytes,
            latency_artifacts[index],
        )?;
    }
    if let Some(holdout) = &result.campaign.holdout {
        let selections = holdout
            .selections
            .iter()
            .map(|selection| (selection.query_ordinal, selection.page_ordinals.clone()))
            .collect::<Vec<_>>();
        let quality = recompute_v23_incidence_quality(&selections, holdout_truth, 28_282)?;
        if quality != holdout.quality {
            return Err(invalid("V23 incidence holdout quality evidence differs"));
        }
        validate_quality(holdout.quality, 128)?;
        let projection =
            project_v23_incidence_serving_bytes(100_000_000, holdout.cell.cap as usize)?;
        if holdout.projected_serving_bytes != projection.total_bytes
            || holdout.maximum_posting_visits
                > u32::from(holdout.cell.cap) * u32::from(holdout.cell.probes)
            || holdout.maximum_touched_pages > holdout.maximum_posting_visits
        {
            return Err(invalid("V23 incidence holdout budget evidence differs"));
        }
        let index = result.campaign.development.len();
        validate_latency_binding(
            holdout.p99_ns,
            &holdout.latency_blake3,
            holdout.latency_bytes,
            latency_artifacts[index],
        )?;
    }
    let structural = |cell: &V23IncidenceCellResult| {
        cell.projected_serving_bytes <= 3 * 1024 * 1024 * 1024
            && cell.maximum_posting_visits <= 262_144
            && cell.maximum_touched_pages <= 8_192
    };
    let expected_sealed = result
        .campaign
        .development
        .iter()
        .find(|cell| {
            cell.retention_passed
                && cell.quality.passed
                && structural(cell)
                && cell.p99_ns <= 15_000_000
        })
        .map(|cell| cell.cell);
    let class = classify_v23_incidence_campaign(&result.campaign);
    if result.sealed_cell != expected_sealed || result.classification != class {
        return Err(invalid("V23 incidence result classification differs"));
    }
    let value = serde_json::to_value(result)
        .map_err(|_| invalid("V23 incidence result serialization failed"))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|_| invalid("V23 incidence result serialization failed"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn evaluate_v23_incidence_cell(
    tree: &V23IncidenceTree,
    plane: &V23PostingPlane,
    cell: V23IncidenceCell,
    queries: &[[f32; 96]],
    truth: &[V23IncidenceQueryTruth],
    page_count: usize,
    latency_artifact: &[u8],
) -> Result<V23IncidenceCellResult> {
    if queries.is_empty() || queries.len() != truth.len() {
        return Err(invalid("V23 incidence evaluation cohort differs"));
    }
    validate_posting_prefix(plane, usize::from(cell.cap))?;
    let latency_samples = decode_v23_incidence_latency_samples(latency_artifact)?;
    let p99_ns = v23_incidence_latency_p99_ns(&latency_samples)?;
    let mut selections = Vec::with_capacity(queries.len());
    let mut maximum_posting_visits = 0_u32;
    let mut maximum_touched_pages = 0_u32;
    let mut determinism_passed = true;
    for (query, expected) in queries.iter().zip(truth) {
        let evidence = score_incidence_query(tree, plane, cell, query, page_count)?;
        maximum_posting_visits = maximum_posting_visits.max(evidence.posting_visits);
        maximum_touched_pages = maximum_touched_pages.max(evidence.touched_pages);
        determinism_passed &= evidence.scalar_pages_equal;
        selections.push((expected.query_ordinal, evidence.page_ordinals));
    }
    let quality = recompute_v23_incidence_quality(&selections, truth, page_count)?;
    let projection = project_v23_incidence_serving_bytes(100_000_000, usize::from(cell.cap))?;
    Ok(V23IncidenceCellResult {
        cell,
        retention_passed: true,
        quality,
        projected_serving_bytes: projection.total_bytes,
        maximum_posting_visits,
        maximum_touched_pages,
        p99_ns,
        determinism_passed,
        latency_blake3: blake3::hash(latency_artifact).to_hex().to_string(),
        latency_bytes: latency_artifact.len() as u64,
        selections: selections
            .into_iter()
            .map(|(query_ordinal, page_ordinals)| V23IncidenceSelection {
                query_ordinal,
                page_ordinals,
            })
            .collect(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V23IncidenceServingProjection {
    pub(crate) projected_pages: u64,
    pub(crate) posting_bytes: u64,
    pub(crate) touched_workspace_bytes: u64,
    pub(crate) total_bytes: u64,
}

pub(crate) fn project_v23_incidence_serving_bytes(
    rows: u64,
    cap: usize,
) -> Result<V23IncidenceServingProjection> {
    if rows == 0 || ![512, 1024, 2048].contains(&cap) {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence serving projection shape differs".to_string(),
        ));
    }
    let projected_pages = 28_282_u64
        .checked_mul(rows)
        .and_then(|value| value.checked_add(9_990_000 - 1))
        .map(|value| value / 9_990_000)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence serving projection overflows".to_string())
        })?;
    let posting_bytes = 65_536_u64 * cap as u64 * 6;
    let touched_workspace_bytes = 262_144_u64 * 4;
    let total_bytes = 12_582_912_u64
        + 262_148
        + 786_432
        + posting_bytes
        + projected_pages * 12
        + touched_workspace_bytes
        + projected_pages * 320
        + 3_932_160
        + 536_870_912
        + 268_435_456;
    Ok(V23IncidenceServingProjection {
        projected_pages,
        posting_bytes,
        touched_workspace_bytes,
        total_bytes,
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use half::f16;
    use parquet::arrow::ArrowWriter;

    use super::{
        V23IncidenceCampaignClass, V23IncidenceCampaignInput, V23IncidenceCampaignResult,
        V23IncidenceCell, V23IncidenceQueryTruth, V23IncidenceQueryWorkspace,
        bind_v23_incidence_holdout_truth, canonical_v23_incidence_result_bytes,
        classify_v23_incidence_campaign, decode_v23_incidence_latency_samples,
        encode_v23_incidence_latency_samples, evaluate_v23_incidence_cell,
        measure_v23_incidence_latency, project_v23_incidence_serving_bytes, rank_incidence_leaves,
        rank_incidence_leaves_scalar, read_v23_incidence_development_queries,
        read_v23_incidence_holdout_neighbors, read_v23_incidence_holdout_queries,
        recompute_v23_incidence_layout_quality, recompute_v23_incidence_quality,
        score_incidence_query, score_incidence_query_native, v23_incidence_latency_p99_ns,
    };
    use crate::{
        v23_incidence_postings::{
            PostingAssignmentArm, V23PostingLeaf, V23PostingPlane, V23PostingPrefixEvidence,
        },
        v23_incidence_tree::{
            V23IncidenceTrainingShape, V23IncidenceTree, V23TrainingWork, V23TreeLeaf,
        },
    };

    fn ranking_tree() -> V23IncidenceTree {
        let leaves = (0..65_536_u32)
            .map(|ordinal| {
                let mut centroid = [f16::ZERO; 96];
                centroid[ordinal as usize % 96] = if ordinal % 17 == 0 {
                    f16::from_bits(1)
                } else {
                    f16::from_f32((ordinal % 31 + 1) as f32 / 31.0)
                };
                V23TreeLeaf {
                    centroid,
                    inverse_norm: 1.0 / centroid[ordinal as usize % 96].to_f32(),
                    population: 1,
                    mean_squared_residual: 0.0,
                }
            })
            .collect();
        V23IncidenceTree {
            shape: V23IncidenceTrainingShape::PRODUCTION,
            reservoir_seed: 1,
            work: V23TrainingWork {
                farthest_seed_dimensions: 0,
                lloyd_dimensions: 0,
                repartition_dimensions: 0,
                total_distance_dimensions: 0,
            },
            nodes: Vec::new(),
            leaves,
        }
    }

    fn posting_plane() -> V23PostingPlane {
        let prefixes = [512, 1024, 2048].map(|_| V23PostingPrefixEvidence {
            retained_assignments: 65_535,
            retained_mass_ppm: 1_000_000,
            quantization_error_numerator: 0,
            quantization_tv_ppm: 0,
        });
        V23PostingPlane {
            arm: PostingAssignmentArm::OneLeaf,
            max_pages_per_leaf: 2048,
            partition_count: 256,
            source_records: 65_536 * 65_535,
            maximum_resident_records: 1,
            maximum_merge_entries: 2,
            scratch_bytes_peak: 65_536 * 65_535 * 8,
            leaves: (0..65_536_u32)
                .map(|leaf| V23PostingLeaf {
                    pages: vec![(leaf / 96) % 16, ((leaf / 96) + 7) % 16],
                    masses: vec![40_000, 25_535],
                    total_mass: 65_535,
                    prefixes,
                })
                .collect(),
        }
    }

    fn neighbor_parquet(child_name: &str, width: i32) -> Vec<u8> {
        let values = (0..10_000_i32)
            .flat_map(|row| (0..width).map(move |neighbor| row * 100 + neighbor))
            .collect::<Vec<_>>();
        let child = Arc::new(Field::new(child_name, DataType::Int32, false));
        let neighbors = FixedSizeListArray::try_new(
            Arc::clone(&child),
            width,
            Arc::new(Int32Array::from(values)),
            None,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "neighbors_id",
            DataType::FixedSizeList(child, width),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(neighbors)]).unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        bytes
    }

    fn canonical_truth(first: u32, count: u32) -> Vec<V23IncidenceQueryTruth> {
        (first..first + count)
            .map(|query_ordinal| V23IncidenceQueryTruth {
                query_ordinal,
                ground_truth_page_assignments: (0..10)
                    .map(|neighbor| vec![(neighbor % 8) as u32])
                    .collect(),
                oracle_pages: (0..8).collect(),
            })
            .collect()
    }

    fn canonical_fixture(
        result: &V23IncidenceCampaignResult,
        latency: &[u8],
    ) -> crate::Result<Vec<u8>> {
        canonical_v23_incidence_result_bytes(
            result,
            &[latency, latency],
            &canonical_truth(0, 32),
            &canonical_truth(32, 128),
        )
    }

    #[test]
    fn v23_incidence_eval_bounded_leaf_ranking_matches_scalar_full_sort() {
        let tree = ranking_tree();
        let mut query = [0.0_f32; 96];
        for (dimension, value) in query.iter_mut().enumerate() {
            *value = (dimension as f32 - 47.5) / 49.0;
        }
        for probes in [32, 64, 128] {
            assert_eq!(
                rank_incidence_leaves(&tree, &query, probes).unwrap(),
                rank_incidence_leaves_scalar(&tree, &query, probes).unwrap()
            );
        }
        query[7] = f32::NAN;
        assert!(rank_incidence_leaves(&tree, &query, 128).is_err());
    }

    #[test]
    fn v23_incidence_eval_posting_kernel_matches_scalar_and_selects_eight_pages() {
        let tree = ranking_tree();
        let plane = posting_plane();
        let mut query = [0.0_f32; 96];
        for (dimension, value) in query.iter_mut().enumerate() {
            *value = (dimension as f32 + 1.0) / 97.0;
        }
        for probes in [32, 64, 128] {
            let evidence = score_incidence_query(
                &tree,
                &plane,
                V23IncidenceCell {
                    cap: 2048,
                    arm: PostingAssignmentArm::OneLeaf,
                    probes,
                },
                &query,
                16,
            )
            .unwrap();
            assert_eq!(evidence.page_ordinals.len(), 8);
            assert!(evidence.scalar_pages_equal);
            assert!(evidence.posting_visits <= 262_144);
            assert!(evidence.touched_pages <= 16);
        }
    }

    #[test]
    fn v23_incidence_eval_native_kernel_reuses_resident_workspace_without_scalar_oracle() {
        let tree = ranking_tree();
        let plane = posting_plane();
        let cell = V23IncidenceCell {
            cap: 2048,
            arm: PostingAssignmentArm::OneLeaf,
            probes: 128,
        };
        let mut query = [0.0_f32; 96];
        for (dimension, value) in query.iter_mut().enumerate() {
            *value = (dimension as f32 + 1.0) / 97.0;
        }
        let expected = score_incidence_query(&tree, &plane, cell, &query, 16).unwrap();
        let mut workspace = V23IncidenceQueryWorkspace::new(16).unwrap();
        let score_allocation = workspace.scores.as_ptr();
        let epoch_allocation = workspace.epochs.as_ptr();
        let touched_allocation = workspace.touched.as_ptr();
        for _ in 0..3 {
            let actual =
                score_incidence_query_native(&tree, &plane, cell, &query, &mut workspace).unwrap();
            assert_eq!(actual.ranked_leaf_ordinals, expected.ranked_leaf_ordinals);
            assert_eq!(actual.page_ordinals, expected.page_ordinals);
            assert!(!actual.scalar_pages_equal);
            assert_eq!(workspace.scores.as_ptr(), score_allocation);
            assert_eq!(workspace.epochs.as_ptr(), epoch_allocation);
            assert_eq!(workspace.touched.as_ptr(), touched_allocation);
        }
    }

    #[test]
    fn v23_incidence_eval_latency_artifact_binds_all_raw_samples_and_p99() {
        let samples = (0..10_000_u64)
            .map(|index| 1_000_000 + index * 1_000)
            .collect::<Vec<_>>();
        let bytes = encode_v23_incidence_latency_samples(&samples).unwrap();
        assert_eq!(
            decode_v23_incidence_latency_samples(&bytes).unwrap(),
            samples
        );
        assert_eq!(v23_incidence_latency_p99_ns(&samples).unwrap(), 10_899_000);

        let mut changed = bytes.clone();
        changed[20] ^= 1;
        assert!(decode_v23_incidence_latency_samples(&changed).is_err());
        assert!(encode_v23_incidence_latency_samples(&samples[..9_999]).is_err());
        let mut too_slow = samples;
        too_slow[9_899..].fill(15_000_001);
        assert!(v23_incidence_latency_p99_ns(&too_slow).unwrap() > 15_000_000);
    }

    #[test]
    fn v23_incidence_eval_quality_recomputes_page_containment_and_oracle_attainment() {
        let truth = vec![V23IncidenceQueryTruth {
            query_ordinal: 32,
            ground_truth_page_assignments: (0..10_u32).map(|page| vec![page]).collect(),
            oracle_pages: (0..8).collect(),
        }];
        let selections = vec![(32, (0..8).collect())];
        let quality = recompute_v23_incidence_quality(&selections, &truth, 16).unwrap();
        assert_eq!(quality.aggregate_recall_ppm, 800_000);
        assert_eq!(quality.minimum_query_recall_ppm, 800_000);
        assert_eq!(quality.oracle_attainment_ppm, 1_000_000);
        assert!(!quality.passed);

        let mut changed = selections;
        changed[0].1[7] = 0;
        assert!(recompute_v23_incidence_quality(&changed, &truth, 16).is_err());
        let mut changed = truth;
        changed[0].ground_truth_page_assignments[0].clear();
        assert!(recompute_v23_incidence_quality(&[(32, (0..8).collect())], &changed, 16).is_err());
        assert!(read_v23_incidence_development_queries(b"not parquet").is_err());
        assert!(read_v23_incidence_holdout_queries(b"", &(31..159).collect::<Vec<_>>()).is_err());
    }

    #[test]
    fn v23_incidence_eval_holdout_binds_all_neighbors_before_first_ten_truth() {
        let neighbors = (32..160_u32)
            .map(|query| {
                (
                    query,
                    (0..100_u64)
                        .map(|neighbor| u64::from(query) * 100 + neighbor)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let mut pages = BTreeMap::new();
        for (_, ids) in &neighbors {
            for (neighbor, id) in ids.iter().enumerate() {
                pages.insert(*id, vec![(neighbor % 10) as u32]);
            }
        }
        let truth = bind_v23_incidence_holdout_truth(&neighbors, &pages, 16).unwrap();
        assert_eq!(truth.len(), 128);
        assert_eq!(truth[0].query_ordinal, 32);
        assert_eq!(truth[0].ground_truth_page_assignments.len(), 10);
        assert_eq!(truth[0].oracle_pages, (0..8).collect::<Vec<_>>());
        let layout = recompute_v23_incidence_layout_quality(&truth).unwrap();
        assert_eq!(layout.query_count, 128);
        assert_eq!(layout.total_oracle_hits, 1_024);
        assert_eq!(layout.minimum_oracle_hits, 8);
        assert!(!layout.passed);

        pages.remove(&neighbors[127].1[99]);
        assert!(bind_v23_incidence_holdout_truth(&neighbors, &pages, 16).is_err());
    }

    #[test]
    fn v23_incidence_eval_reads_exact_holdout_neighbor_rows_and_physical_schema() {
        let bytes = neighbor_parquet("element", 100);
        let neighbors = read_v23_incidence_holdout_neighbors(&bytes).unwrap();
        assert_eq!(neighbors.len(), 128);
        assert_eq!(neighbors[0].0, 32);
        assert_eq!(neighbors[0].1, (3_200..3_300).collect::<Vec<_>>());
        assert_eq!(neighbors[127].0, 159);
        assert_eq!(neighbors[127].1, (15_900..16_000).collect::<Vec<_>>());
        assert!(read_v23_incidence_holdout_neighbors(&neighbor_parquet("item", 100)).is_err());
        assert!(read_v23_incidence_holdout_neighbors(&neighbor_parquet("element", 99)).is_err());
    }

    #[test]
    fn v23_incidence_eval_native_timer_records_exact_warmup_and_sample_count() {
        let mut calls = 0_usize;
        let artifact = measure_v23_incidence_latency(|| {
            calls += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(calls, 1_024 + 10_000);
        assert_eq!(
            decode_v23_incidence_latency_samples(&artifact)
                .unwrap()
                .len(),
            10_000
        );
    }

    #[test]
    fn v23_incidence_eval_cell_recomputes_quality_budgets_and_latency_binding() {
        let tree = ranking_tree();
        let plane = posting_plane();
        let cell = V23IncidenceCell {
            cap: 512,
            arm: PostingAssignmentArm::OneLeaf,
            probes: 32,
        };
        let mut query = [0.0_f32; 96];
        for (dimension, value) in query.iter_mut().enumerate() {
            *value = (dimension as f32 + 1.0) / 97.0;
        }
        let selected = score_incidence_query(&tree, &plane, cell, &query, 16)
            .unwrap()
            .page_ordinals;
        let truth = vec![V23IncidenceQueryTruth {
            query_ordinal: 32,
            ground_truth_page_assignments: (0..10)
                .map(|index| vec![selected[index % selected.len()]])
                .collect(),
            oracle_pages: selected.clone(),
        }];
        let latency = encode_v23_incidence_latency_samples(&vec![1_000_000; 10_000]).unwrap();
        let result =
            evaluate_v23_incidence_cell(&tree, &plane, cell, &[query], &truth, 16, &latency)
                .unwrap();
        assert_eq!(result.cell, cell);
        assert!(result.quality.passed);
        assert_eq!(result.p99_ns, 1_000_000);
        assert_eq!(
            result.latency_blake3,
            blake3::hash(&latency).to_hex().as_str()
        );
        assert_eq!(result.latency_bytes, latency.len() as u64);
        assert!(result.maximum_posting_visits <= 16_384);
        assert!(result.maximum_touched_pages <= 16);

        let mut changed = latency;
        changed[20] ^= 1;
        assert!(
            evaluate_v23_incidence_cell(&tree, &plane, cell, &[query], &truth, 16, &changed)
                .is_err()
        );
    }

    #[test]
    fn v23_incidence_eval_campaign_uses_frozen_cells_and_exhaustive_precedence() {
        let ladder = V23IncidenceCell::registered_ladder();
        assert_eq!(ladder.len(), 18);
        assert_eq!(
            ladder[0],
            V23IncidenceCell {
                cap: 512,
                arm: PostingAssignmentArm::OneLeaf,
                probes: 32,
            }
        );
        assert_eq!(
            ladder[17],
            V23IncidenceCell {
                cap: 2048,
                arm: PostingAssignmentArm::TwoBeamLeaves,
                probes: 128,
            }
        );

        let mut input = V23IncidenceCampaignInput::passing_fixture();
        input.authority_passed = false;
        assert_eq!(
            classify_v23_incidence_campaign(&input),
            V23IncidenceCampaignClass::AuthorityStop
        );
        let mut input = V23IncidenceCampaignInput::passing_fixture();
        input.resource_passed = false;
        assert_eq!(
            classify_v23_incidence_campaign(&input),
            V23IncidenceCampaignClass::ResourceStop
        );
        let mut input = V23IncidenceCampaignInput::passing_fixture();
        input.determinism_passed = false;
        assert_eq!(
            classify_v23_incidence_campaign(&input),
            V23IncidenceCampaignClass::DeterminismStop
        );
        let mut input = V23IncidenceCampaignInput::passing_fixture();
        input.development[0].retention_passed = false;
        assert_eq!(
            classify_v23_incidence_campaign(&input),
            V23IncidenceCampaignClass::RetentionRejected
        );
        let mut input = V23IncidenceCampaignInput::passing_fixture();
        input.development[0].quality.passed = false;
        assert_eq!(
            classify_v23_incidence_campaign(&input),
            V23IncidenceCampaignClass::QualityRejected
        );
        let mut input = V23IncidenceCampaignInput::passing_fixture();
        input.development[0].maximum_touched_pages = 8_193;
        assert_eq!(
            classify_v23_incidence_campaign(&input),
            V23IncidenceCampaignClass::BudgetRejected
        );
        let mut input = V23IncidenceCampaignInput::passing_fixture();
        input.development[0].p99_ns = 15_000_001;
        assert_eq!(
            classify_v23_incidence_campaign(&input),
            V23IncidenceCampaignClass::KernelRejected
        );
        let mut input = V23IncidenceCampaignInput::passing_fixture();
        input.holdout_layout.passed = false;
        assert_eq!(
            classify_v23_incidence_campaign(&input),
            V23IncidenceCampaignClass::HoldoutLayoutRejected
        );
        let mut input = V23IncidenceCampaignInput::passing_fixture();
        input.holdout.as_mut().unwrap().quality.passed = false;
        assert_eq!(
            classify_v23_incidence_campaign(&input),
            V23IncidenceCampaignClass::GeneralizationRejected
        );
        let mut input = V23IncidenceCampaignInput::passing_fixture();
        input.holdout.as_mut().unwrap().maximum_touched_pages = 8_193;
        assert_eq!(
            classify_v23_incidence_campaign(&input),
            V23IncidenceCampaignClass::HoldoutBudgetRejected
        );
        let mut input = V23IncidenceCampaignInput::passing_fixture();
        input.holdout.as_mut().unwrap().p99_ns = 15_000_001;
        assert_eq!(
            classify_v23_incidence_campaign(&input),
            V23IncidenceCampaignClass::HoldoutKernelRejected
        );
        assert_eq!(
            classify_v23_incidence_campaign(&V23IncidenceCampaignInput::passing_fixture()),
            V23IncidenceCampaignClass::FalsifierPassed
        );
    }

    #[test]
    fn v23_incidence_eval_serving_projection_is_exact_at_maximum_cell() {
        let projection = project_v23_incidence_serving_bytes(100_000_000, 2048).unwrap();
        assert_eq!(projection.projected_pages, 283_104);
        assert_eq!(projection.posting_bytes, 805_306_368);
        assert_eq!(projection.touched_workspace_bytes, 1_048_576);
        assert_eq!(projection.total_bytes, 1_723_215_492);
    }

    #[test]
    fn v23_incidence_eval_canonical_result_recomputes_every_gate_and_latency_artifact() {
        let samples = vec![15_000_000_u64; 10_000];
        let latency = encode_v23_incidence_latency_samples(&samples).unwrap();
        let mut result = V23IncidenceCampaignResult::passing_fixture(&latency);
        let bytes = canonical_fixture(&result, &latency).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["schema"], "borsuk-v23-incidence-result-v1");
        assert_eq!(value["claim_eligible"], false);
        assert_eq!(value["page_body_reads"], 0);
        assert_eq!(value["classification"], "incidence-falsifier-passed");

        let mut later_eligible = V23IncidenceCampaignResult::passing_fixture(&latency);
        later_eligible.campaign.development[0].cell = V23IncidenceCell::registered_ladder()[3];
        later_eligible.campaign.holdout.as_mut().unwrap().cell =
            V23IncidenceCell::registered_ladder()[3];
        later_eligible.sealed_cell = Some(V23IncidenceCell::registered_ladder()[3]);
        canonical_fixture(&later_eligible, &latency).unwrap();

        result.classification = V23IncidenceCampaignClass::KernelRejected;
        assert!(canonical_fixture(&result, &latency).is_err());
        let mut result = V23IncidenceCampaignResult::passing_fixture(&latency);
        result.campaign.development[0].quality.query_count = 2;
        result.campaign.development[0].quality.total_hits = 10;
        result.campaign.development[0].quality.oracle_hits = 10;
        assert!(canonical_fixture(&result, &latency).is_err());
        let mut result = V23IncidenceCampaignResult::passing_fixture(&latency);
        result.campaign.holdout_layout.total_oracle_hits -= 1;
        assert!(canonical_fixture(&result, &latency).is_err());
        let mut result = V23IncidenceCampaignResult::passing_fixture(&latency);
        result.campaign.development[0].selections[0].page_ordinals[0] = 9;
        assert!(canonical_fixture(&result, &latency).is_err());
        let mut result = V23IncidenceCampaignResult::passing_fixture(&latency);
        result.campaign.development[0].p99_ns ^= 1;
        assert!(canonical_fixture(&result, &latency).is_err());
        let mut result = V23IncidenceCampaignResult::passing_fixture(&latency);
        result.sealed_cell = Some(V23IncidenceCell::registered_ladder()[1]);
        assert!(canonical_fixture(&result, &latency).is_err());
        let mut result = V23IncidenceCampaignResult::passing_fixture(&latency);
        result
            .campaign
            .holdout
            .as_mut()
            .unwrap()
            .maximum_touched_pages = 8_193;
        assert!(canonical_fixture(&result, &latency).is_err());
        let mut changed = latency;
        changed[20] ^= 1;
        let result = V23IncidenceCampaignResult::passing_fixture(&changed);
        assert!(canonical_fixture(&result, &changed).is_err());
    }
}
