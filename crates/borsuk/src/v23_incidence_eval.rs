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
    v23_diagnostic::{
        V23D1ArmKey, V23D2Report, V23QuantizerFamily, read_v23_query_vectors, validate_d2_report,
    },
    v23_incidence::canonical_json_value,
    v23_incidence_postings::{
        PostingAssignmentArm, V23PostingPlane, posting_prefix_eligibility, validate_posting_prefix,
    },
    v23_incidence_tree::{
        V23_INCIDENCE_LEAVES, V23IncidenceTree, rank_v23_incidence_tree_beam,
        rank_v23_incidence_tree_beam_scalar, v23_tree_beam_centroid_scores,
        v23_tree_beam_centroid_scores_for_depth,
    },
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V23IncidenceD2ReportEnvelope {
    claim_eligible: bool,
    d1_report_sha256: String,
    dataset_id: String,
    document_kind: String,
    index_id: String,
    page_uri: String,
    report: V23D2Report,
    schema: String,
    source_archive_sha256: String,
    stage: String,
}

pub(crate) fn read_v23_incidence_development_truth(
    bytes: &[u8],
) -> Result<Vec<V23IncidenceQueryTruth>> {
    let envelope: V23IncidenceD2ReportEnvelope =
        serde_json::from_slice(bytes).map_err(|error| {
            BorsukError::InvalidStorage(format!("V23 incidence D2 report JSON differs: {error}"))
        })?;
    if envelope.claim_eligible
        || envelope.schema != "borsuk-v23-d2-artifact-v1"
        || envelope.document_kind != "publication-v3-v23-d2-report"
        || envelope.stage != "d2"
        || envelope.index_id != "index-bcda7bb66812e162d45077e6"
        || envelope.dataset_id != "deep-image-96"
        || envelope.source_archive_sha256
            != "77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d"
        || !exact_lower_hex(&envelope.d1_report_sha256, 64)
        || envelope.page_uri.is_empty()
    {
        return Err(invalid("V23 incidence D2 report outer authority differs"));
    }
    validate_d2_report(&envelope.report)?;
    if envelope.report.query_ordinals != (0..32).collect::<Vec<_>>() {
        return Err(invalid("V23 incidence development query ordinals differ"));
    }
    let selector_key = V23D1ArmKey {
        family: V23QuantizerFamily::SrhtPq,
        code_width_bytes: 12,
    };
    let arm = envelope
        .report
        .arms
        .iter()
        .find(|arm| arm.selector_key == selector_key)
        .ok_or_else(|| invalid("V23 incidence development width-12 arm is absent"))?;
    if arm.pages.len() != 28_282 || arm.query_samples.len() != 32 {
        return Err(invalid("V23 incidence development truth shape differs"));
    }
    arm.query_samples
        .iter()
        .zip(&envelope.report.query_ordinals)
        .map(|(sample, query_ordinal)| {
            let query_ordinal = u32::try_from(*query_ordinal)
                .map_err(|_| invalid("V23 incidence development query ordinal overflows"))?;
            let truth = V23IncidenceQueryTruth {
                query_ordinal,
                ground_truth_page_assignments: sample.ground_truth_page_assignments.clone(),
                oracle_pages: sample.oracle_page_ordinals.clone(),
            };
            if sample.query_index != query_ordinal
                || truth.oracle_pages
                    != exact_coverage_oracle(&truth.ground_truth_page_assignments, 28_282)?
                || truth
                    .ground_truth_page_assignments
                    .iter()
                    .flatten()
                    .chain(&truth.oracle_pages)
                    .any(|page| *page >= 28_282)
            {
                return Err(invalid("V23 incidence development truth differs"));
            }
            Ok(truth)
        })
        .collect()
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

fn rank_incidence_leaves_with_shape(
    tree: &V23IncidenceTree,
    query: &[f32; 96],
    probes: usize,
    expected_leaves: usize,
) -> Result<Vec<u16>> {
    if tree.leaves.len() != expected_leaves || probes == 0 || probes > 128 {
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

pub(crate) fn rank_incidence_leaves(
    tree: &V23IncidenceTree,
    query: &[f32; 96],
    probes: usize,
) -> Result<Vec<u16>> {
    if ![32, 64, 128].contains(&probes) {
        return Err(invalid("V23 incidence leaf ranking shape differs"));
    }
    rank_incidence_leaves_with_shape(tree, query, probes, V23_INCIDENCE_LEAVES)
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23IncidenceQueryTruth {
    pub(crate) query_ordinal: u32,
    pub(crate) ground_truth_page_assignments: Vec<Vec<u32>>,
    pub(crate) oracle_pages: Vec<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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

fn exact_coverage_candidates(assignments: &[Vec<u32>], page_count: usize) -> Result<Vec<u32>> {
    let mut candidates = assignments
        .iter()
        .flatten()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if page_count < 8
        || candidates.len() > 20
        || candidates
            .iter()
            .any(|page| usize::try_from(*page).map_or(true, |page| page >= page_count))
    {
        return Err(invalid("V23 incidence oracle candidate count differs"));
    }
    for page in
        0..u32::try_from(page_count).map_err(|_| invalid("V23 incidence page count exceeds u32"))?
    {
        if candidates.len() >= 8 {
            break;
        }
        if candidates.binary_search(&page).is_err() {
            candidates.push(page);
            candidates.sort_unstable();
        }
    }
    Ok(candidates)
}

fn exact_coverage_oracle(assignments: &[Vec<u32>], page_count: usize) -> Result<Vec<u32>> {
    let candidates = exact_coverage_candidates(assignments, page_count)?;
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
            let oracle_pages = exact_coverage_oracle(&ground_truth_page_assignments, page_count)?;
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

fn score_incidence_query_with_shape(
    tree: &V23IncidenceTree,
    plane: &V23PostingPlane,
    cell: V23IncidenceCell,
    query: &[f32; 96],
    page_count: usize,
    expected_leaves: usize,
) -> Result<V23IncidenceQueryEvidence> {
    if cell.arm != plane.arm
        || ![512, 1024, 2048].contains(&cell.cap)
        || ![32, 64, 128].contains(&cell.beam_width)
        || page_count < 8
        || tree.leaves.len() != expected_leaves
    {
        return Err(invalid("V23 incidence query cell differs"));
    }
    posting_prefix_eligibility(plane, usize::from(cell.cap))?;
    let ranked_leaf_ordinals =
        rank_v23_incidence_tree_beam(tree, query, usize::from(cell.beam_width))?;
    #[cfg(test)]
    {
        if ranked_leaf_ordinals
            != rank_v23_incidence_tree_beam_scalar(tree, query, usize::from(cell.beam_width))?
        {
            return Err(invalid(
                "V23 incidence scalar and optimized tree-beam leaves differ",
            ));
        }
    }
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

pub(crate) fn score_incidence_query(
    tree: &V23IncidenceTree,
    plane: &V23PostingPlane,
    cell: V23IncidenceCell,
    query: &[f32; 96],
    page_count: usize,
) -> Result<V23IncidenceQueryEvidence> {
    score_incidence_query_with_shape(tree, plane, cell, query, page_count, V23_INCIDENCE_LEAVES)
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
        || ![32, 64, 128].contains(&cell.beam_width)
    {
        return Err(invalid("V23 incidence native query cell differs"));
    }
    posting_prefix_eligibility(plane, usize::from(cell.cap))?;
    let ranked_leaf_ordinals =
        rank_v23_incidence_tree_beam(tree, query, usize::from(cell.beam_width))?;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V23IncidenceEvaluationPreflightMeasurement {
    pub(crate) beam_width: u16,
    pub(crate) scored_centroids_per_query: u32,
    pub(crate) distance_dimensions: u64,
    pub(crate) distance_elapsed_ns: u64,
    pub(crate) posting_visits: u64,
    pub(crate) posting_elapsed_ns: u64,
}

fn v23_incidence_synthetic_preflight_query(ordinal: u64) -> [f32; 96] {
    std::array::from_fn(|dimension| {
        let value = ordinal
            .wrapping_mul(131)
            .wrapping_add((dimension as u64).wrapping_mul(17))
            .wrapping_add(1)
            % 251;
        (value as i32 - 125) as f32
    })
}

pub(crate) fn measure_v23_incidence_evaluation_preflight(
    tree: &V23IncidenceTree,
    plane: &V23PostingPlane,
    query_count: usize,
    page_count: usize,
) -> Result<V23IncidenceEvaluationPreflightMeasurement> {
    if query_count == 0 {
        return Err(invalid(
            "V23 incidence evaluation preflight query count differs",
        ));
    }
    validate_posting_prefix(plane, 2_048)?;
    let mut workspace = V23IncidenceQueryWorkspace::new(page_count)?;
    let mut distance_elapsed_ns = 0_u64;
    let mut posting_elapsed_ns = 0_u64;
    let mut posting_visits = 0_u64;
    let beam_width = 128_usize;
    let scored_centroids_per_query = v23_tree_beam_centroid_scores(beam_width)?;
    for ordinal in 0..query_count {
        let query = v23_incidence_synthetic_preflight_query(ordinal as u64);
        let started = Instant::now();
        let ranked = rank_v23_incidence_tree_beam(tree, &query, beam_width)?;
        distance_elapsed_ns = distance_elapsed_ns
            .checked_add(
                u64::try_from(started.elapsed().as_nanos())
                    .unwrap_or(u64::MAX)
                    .max(1),
            )
            .ok_or_else(|| invalid("V23 incidence evaluation preflight time overflows"))?;
        let started = Instant::now();
        let (pages, visits, _) = selected_pages_q32(plane, &ranked, 2_048, &mut workspace)?;
        std::hint::black_box(pages);
        posting_elapsed_ns = posting_elapsed_ns
            .checked_add(
                u64::try_from(started.elapsed().as_nanos())
                    .unwrap_or(u64::MAX)
                    .max(1),
            )
            .ok_or_else(|| invalid("V23 incidence evaluation preflight time overflows"))?;
        posting_visits = posting_visits
            .checked_add(u64::from(visits))
            .ok_or_else(|| invalid("V23 incidence evaluation preflight visits overflow"))?;
    }
    let distance_dimensions = u64::try_from(query_count)
        .ok()
        .and_then(|count| count.checked_mul(u64::from(scored_centroids_per_query)))
        .and_then(|count| count.checked_mul(96))
        .ok_or_else(|| invalid("V23 incidence evaluation preflight work overflows"))?;
    Ok(V23IncidenceEvaluationPreflightMeasurement {
        beam_width: u16::try_from(beam_width).unwrap(),
        scored_centroids_per_query,
        distance_dimensions,
        distance_elapsed_ns,
        posting_visits,
        posting_elapsed_ns,
    })
}

const LATENCY_MAGIC: &[u8; 8] = b"BVIL\x01\0\0\0";
const DEVELOPMENT_LATENCY_BUNDLE_MAGIC: &[u8; 8] = b"BVIB\x01\0\0\0";

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
        .as_chunks::<8>()
        .0
        .iter()
        .map(|sample| u64::from_le_bytes(*sample))
        .collect::<Vec<_>>();
    v23_incidence_latency_p99_ns(&samples)?;
    Ok(samples)
}

pub(crate) fn encode_v23_incidence_development_latency_bundle(
    artifacts: &[Vec<u8>],
) -> Result<Vec<u8>> {
    if artifacts.len() != 18 {
        return Err(invalid(
            "V23 incidence development latency artifact count differs",
        ));
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(DEVELOPMENT_LATENCY_BUNDLE_MAGIC);
    bytes.extend_from_slice(&(artifacts.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    for artifact in artifacts {
        decode_v23_incidence_latency_samples(artifact)?;
        bytes.extend_from_slice(
            &u64::try_from(artifact.len())
                .map_err(|_| invalid("V23 incidence latency artifact length exceeds u64"))?
                .to_le_bytes(),
        );
        bytes.extend_from_slice(artifact);
    }
    let digest = blake3::hash(&bytes);
    bytes.extend_from_slice(digest.as_bytes());
    Ok(bytes)
}

pub(crate) fn decode_v23_incidence_development_latency_bundle(
    bytes: &[u8],
) -> Result<Vec<Vec<u8>>> {
    if bytes.len() < 48 || bytes.get(..8) != Some(DEVELOPMENT_LATENCY_BUNDLE_MAGIC) {
        return Err(invalid(
            "V23 incidence development latency bundle header differs",
        ));
    }
    let (body, claimed_digest) = bytes.split_at(bytes.len() - 32);
    if blake3::hash(body).as_bytes() != claimed_digest
        || body.get(8..12) != Some(&18_u32.to_le_bytes())
        || body.get(12..16) != Some(&0_u32.to_le_bytes())
    {
        return Err(invalid(
            "V23 incidence development latency bundle authority differs",
        ));
    }
    let mut cursor = 16_usize;
    let mut artifacts = Vec::with_capacity(18);
    for _ in 0..18 {
        let length = body
            .get(cursor..cursor + 8)
            .map(|value| u64::from_le_bytes(value.try_into().unwrap()))
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| invalid("V23 incidence latency bundle length differs"))?;
        cursor = cursor
            .checked_add(8)
            .ok_or_else(|| invalid("V23 incidence latency bundle offset overflows"))?;
        let end = cursor
            .checked_add(length)
            .ok_or_else(|| invalid("V23 incidence latency bundle offset overflows"))?;
        let artifact = body
            .get(cursor..end)
            .ok_or_else(|| invalid("V23 incidence latency bundle is truncated"))?
            .to_vec();
        decode_v23_incidence_latency_samples(&artifact)?;
        artifacts.push(artifact);
        cursor = end;
    }
    if cursor != body.len() {
        return Err(invalid(
            "V23 incidence development latency bundle length differs",
        ));
    }
    Ok(artifacts)
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

fn rank_incidence_leaves_scalar_with_shape(
    tree: &V23IncidenceTree,
    query: &[f32; 96],
    probes: usize,
    expected_leaves: usize,
) -> Result<Vec<u16>> {
    if tree.leaves.len() != expected_leaves || ![32, 64, 128].contains(&probes) {
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

#[cfg(test)]
fn rank_incidence_leaves_scalar(
    tree: &V23IncidenceTree,
    query: &[f32; 96],
    probes: usize,
) -> Result<Vec<u16>> {
    rank_incidence_leaves_scalar_with_shape(tree, query, probes, V23_INCIDENCE_LEAVES)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23IncidenceCell {
    pub(crate) cap: u16,
    pub(crate) arm: PostingAssignmentArm,
    pub(crate) beam_width: u16,
}

impl V23IncidenceCell {
    pub(crate) fn registered_ladder() -> Vec<Self> {
        let mut cells = Vec::with_capacity(18);
        for cap in [512, 1024, 2048] {
            for arm in [
                PostingAssignmentArm::OneLeaf,
                PostingAssignmentArm::TwoBeamLeaves,
            ] {
                for beam_width in [32, 64, 128] {
                    cells.push(Self {
                        cap,
                        arm,
                        beam_width,
                    });
                }
            }
        }
        cells
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23IncidenceSelection {
    pub(crate) query_ordinal: u32,
    pub(crate) page_ordinals: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23IncidenceCellResult {
    pub(crate) cell: V23IncidenceCell,
    pub(crate) scored_centroids_per_query: u32,
    pub(crate) distance_dimensions_per_query: u32,
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
#[serde(deny_unknown_fields)]
pub(crate) struct V23IncidenceDevelopmentAuthority {
    pub(crate) query_router: String,
    pub(crate) source_commit: String,
    pub(crate) source_archive_sha256: String,
    pub(crate) index_id: String,
    pub(crate) dataset_id: String,
    pub(crate) query_cohort_sha256: String,
    pub(crate) tree_blake3: String,
    pub(crate) posting_one_blake3: String,
    pub(crate) posting_two_blake3: String,
    pub(crate) executable_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23IncidenceDevelopmentArtifact {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) authority: V23IncidenceDevelopmentAuthority,
    pub(crate) development: Vec<V23IncidenceCellResult>,
    pub(crate) development_truth: Vec<V23IncidenceQueryTruth>,
    pub(crate) sealed_cell: Option<V23IncidenceCell>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23IncidenceHoldoutTruthAuthority {
    pub(crate) development_result_sha256: String,
    pub(crate) neighbors_sha256: String,
    pub(crate) page_roster_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23IncidenceHoldoutTruthArtifact {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) authority: V23IncidenceHoldoutTruthAuthority,
    pub(crate) sealed_cell: V23IncidenceCell,
    pub(crate) truth: Vec<V23IncidenceQueryTruth>,
    pub(crate) layout: V23IncidenceLayoutQuality,
}

pub(crate) fn canonical_v23_incidence_holdout_truth_bytes(
    artifact: &V23IncidenceHoldoutTruthArtifact,
    expected_authority: &V23IncidenceHoldoutTruthAuthority,
    expected_cell: V23IncidenceCell,
) -> Result<Vec<u8>> {
    if artifact.schema != "borsuk-v23-incidence-holdout-truth-v2"
        || artifact.claim_eligible
        || artifact.authority != *expected_authority
        || artifact.sealed_cell != expected_cell
        || !exact_lower_hex(&artifact.authority.development_result_sha256, 64)
        || !exact_lower_hex(&artifact.authority.neighbors_sha256, 64)
        || !exact_lower_hex(&artifact.authority.page_roster_sha256, 64)
        || artifact.truth.len() != 128
        || artifact
            .truth
            .iter()
            .map(|truth| truth.query_ordinal)
            .ne(32..160)
        || recompute_v23_incidence_layout_quality(&artifact.truth)? != artifact.layout
    {
        return Err(invalid("V23 incidence holdout truth authority differs"));
    }
    validate_layout_quality(artifact.layout)?;
    let value = serde_json::to_value(artifact)
        .map_err(|_| invalid("V23 incidence holdout truth serialization failed"))?;
    let mut bytes = serde_json::to_vec(&crate::v23_incidence::canonical_json_value(value))
        .map_err(|_| invalid("V23 incidence holdout truth serialization failed"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn canonical_v23_incidence_development_artifact_bytes(
    artifact: &V23IncidenceDevelopmentArtifact,
    expected_authority: &V23IncidenceDevelopmentAuthority,
    latency_artifacts: &[Vec<u8>],
    development_truth: &[V23IncidenceQueryTruth],
) -> Result<Vec<u8>> {
    let authority = &artifact.authority;
    if artifact.schema != "borsuk-v23-incidence-development-v2"
        || artifact.claim_eligible
        || authority != expected_authority
        || authority.query_router != "centroid-tree-beam-v1"
        || !exact_lower_hex(&authority.source_commit, 40)
        || !exact_lower_hex(&authority.source_archive_sha256, 64)
        || !exact_lower_hex(&authority.query_cohort_sha256, 64)
        || !exact_lower_hex(&authority.tree_blake3, 64)
        || !exact_lower_hex(&authority.posting_one_blake3, 64)
        || !exact_lower_hex(&authority.posting_two_blake3, 64)
        || !exact_lower_hex(&authority.executable_sha256, 64)
        || authority.index_id.is_empty()
        || authority.dataset_id.is_empty()
        || artifact.development_truth != development_truth
        || artifact.development.len() != 18
        || latency_artifacts.len() != 18
        || artifact
            .development
            .iter()
            .map(|cell| cell.cell)
            .ne(V23IncidenceCell::registered_ladder())
    {
        return Err(invalid("V23 incidence development authority differs"));
    }
    for (cell, latency) in artifact.development.iter().zip(latency_artifacts) {
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
        let projection =
            project_v23_incidence_serving_bytes(100_000_000, usize::from(cell.cell.cap))?;
        let scored_centroids = v23_tree_beam_centroid_scores(usize::from(cell.cell.beam_width))?;
        if cell.scored_centroids_per_query != scored_centroids
            || cell.distance_dimensions_per_query != scored_centroids * 96
            || cell.projected_serving_bytes != projection.total_bytes
            || cell.maximum_posting_visits
                > u32::from(cell.cell.cap) * u32::from(cell.cell.beam_width)
            || cell.maximum_touched_pages > cell.maximum_posting_visits
        {
            return Err(invalid("V23 incidence development budget evidence differs"));
        }
        validate_latency_binding(
            cell.p99_ns,
            &cell.latency_blake3,
            cell.latency_bytes,
            latency,
        )?;
    }
    let expected_sealed = artifact
        .development
        .iter()
        .find(|cell| {
            cell.retention_passed
                && cell.quality.passed
                && cell.determinism_passed
                && cell.projected_serving_bytes <= 3 * 1024 * 1024 * 1024
                && cell.maximum_posting_visits <= 262_144
                && cell.maximum_touched_pages <= 8_192
                && cell.p99_ns <= 15_000_000
        })
        .map(|cell| cell.cell);
    if artifact.sealed_cell != expected_sealed {
        return Err(invalid("V23 incidence development seal differs"));
    }
    let value = serde_json::to_value(artifact)
        .map_err(|_| invalid("V23 incidence development serialization failed"))?;
    let mut bytes = serde_json::to_vec(&crate::v23_incidence::canonical_json_value(value))
        .map_err(|_| invalid("V23 incidence development serialization failed"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
                scored_centroids_per_query: 766,
                distance_dimensions_per_query: 73_536,
                retention_passed: true,
                quality,
                projected_serving_bytes: 1_172_979_332,
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
                projected_serving_bytes: 1_172_979_332,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum V23IncidenceScreenClass {
    #[serde(rename = "leaf-incidence-quality-rejected")]
    LeafIncidenceQualityRejected,
    #[serde(rename = "tree-beam-selector-rejected")]
    TreeBeamSelectorRejected,
    #[serde(rename = "tree-beam-screen-passed")]
    TreeBeamScreenPassed,
}

pub(crate) const fn classify_v23_incidence_screen(
    exhaustive_control_passed: bool,
    tree_beam_passed: bool,
) -> V23IncidenceScreenClass {
    if tree_beam_passed {
        V23IncidenceScreenClass::TreeBeamScreenPassed
    } else if exhaustive_control_passed {
        V23IncidenceScreenClass::TreeBeamSelectorRejected
    } else {
        V23IncidenceScreenClass::LeafIncidenceQualityRejected
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23IncidenceScreenObjectIdentity {
    pub(crate) role: String,
    pub(crate) uri: String,
    pub(crate) digest_algorithm: String,
    pub(crate) digest: String,
    pub(crate) encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23IncidenceScreenAuthority {
    pub(crate) source_commit: String,
    pub(crate) source_archive_sha256: String,
    pub(crate) index_id: String,
    pub(crate) objects: Vec<V23IncidenceScreenObjectIdentity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum V23IncidenceScreenSelector {
    #[serde(rename = "centroid-tree-beam-v1")]
    TreeBeam,
    #[serde(rename = "exhaustive-leaf-control-v1")]
    ExhaustiveControl,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23IncidenceScreenCellResult {
    pub(crate) selector: V23IncidenceScreenSelector,
    pub(crate) cell: V23IncidenceCell,
    pub(crate) scored_centroids_per_query: u32,
    pub(crate) distance_dimensions_per_query: u32,
    pub(crate) quality: V23IncidenceQuality,
    pub(crate) projected_serving_bytes: u64,
    pub(crate) maximum_posting_visits: u32,
    pub(crate) maximum_touched_pages: u32,
    pub(crate) determinism_passed: bool,
    pub(crate) selections: Vec<V23IncidenceSelection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23IncidenceScreenResult {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) authority: V23IncidenceScreenAuthority,
    pub(crate) development_truth: Vec<V23IncidenceQueryTruth>,
    pub(crate) tree_beam: Vec<V23IncidenceScreenCellResult>,
    pub(crate) exhaustive_control: Vec<V23IncidenceScreenCellResult>,
    pub(crate) exhaustive_control_passed: bool,
    pub(crate) tree_beam_passed: bool,
    pub(crate) selected_cell: Option<V23IncidenceCell>,
    pub(crate) classification: V23IncidenceScreenClass,
    pub(crate) page_body_reads: u64,
    pub(crate) holdout_rows_read: u64,
}

impl V23IncidenceScreenResult {
    #[cfg(test)]
    fn passing_fixture() -> Self {
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
        let development_truth = (0..32)
            .map(|query_ordinal| V23IncidenceQueryTruth {
                query_ordinal,
                ground_truth_page_assignments: (0..10).map(|page| vec![page % 8]).collect(),
                oracle_pages: (0..8).collect(),
            })
            .collect::<Vec<_>>();
        let selections = (0..32)
            .map(|query_ordinal| V23IncidenceSelection {
                query_ordinal,
                page_ordinals: (0..8).collect(),
            })
            .collect::<Vec<_>>();
        let cells = V23IncidenceCell::registered_ladder();
        let tree_beam = cells
            .iter()
            .map(|cell| {
                let scored_centroids_per_query =
                    v23_tree_beam_centroid_scores(usize::from(cell.beam_width)).unwrap();
                V23IncidenceScreenCellResult {
                    selector: V23IncidenceScreenSelector::TreeBeam,
                    cell: *cell,
                    scored_centroids_per_query,
                    distance_dimensions_per_query: scored_centroids_per_query * 96,
                    quality,
                    projected_serving_bytes: project_v23_incidence_serving_bytes(
                        100_000_000,
                        usize::from(cell.cap),
                    )
                    .unwrap()
                    .total_bytes,
                    maximum_posting_visits: 8,
                    maximum_touched_pages: 8,
                    determinism_passed: true,
                    selections: selections.clone(),
                }
            })
            .collect();
        let exhaustive_control = cells
            .iter()
            .map(|cell| V23IncidenceScreenCellResult {
                selector: V23IncidenceScreenSelector::ExhaustiveControl,
                cell: *cell,
                scored_centroids_per_query: 65_536,
                distance_dimensions_per_query: 65_536 * 96,
                quality,
                projected_serving_bytes: project_v23_incidence_serving_bytes(
                    100_000_000,
                    usize::from(cell.cap),
                )
                .unwrap()
                .total_bytes,
                maximum_posting_visits: 8,
                maximum_touched_pages: 8,
                determinism_passed: true,
                selections: selections.clone(),
            })
            .collect();
        let roles = [
            ("tree-receipt", "sha256"),
            ("incidence-tree", "blake3"),
            ("posting-receipt", "sha256"),
            ("incidence-postings-one", "blake3"),
            ("incidence-postings-two", "blake3"),
            ("d2-report", "sha256"),
            ("query-parquet", "sha256"),
        ];
        Self {
            schema: "borsuk-v23-incidence-development-screen-v1".to_string(),
            claim_eligible: false,
            authority: V23IncidenceScreenAuthority {
                source_commit: "1".repeat(40),
                source_archive_sha256: "2".repeat(64),
                index_id: "index-fixture".to_string(),
                objects: roles
                    .into_iter()
                    .enumerate()
                    .map(
                        |(index, (role, digest_algorithm))| V23IncidenceScreenObjectIdentity {
                            role: role.to_string(),
                            uri: format!("s3://fixture/{role}"),
                            digest_algorithm: digest_algorithm.to_string(),
                            digest: char::from(b'3' + u8::try_from(index).unwrap())
                                .to_string()
                                .repeat(64),
                            encoded_bytes: u64::try_from(index + 1).unwrap(),
                        },
                    )
                    .collect(),
            },
            development_truth,
            tree_beam,
            exhaustive_control,
            exhaustive_control_passed: true,
            tree_beam_passed: true,
            selected_cell: Some(cells[0]),
            classification: V23IncidenceScreenClass::TreeBeamScreenPassed,
            page_body_reads: 0,
            holdout_rows_read: 0,
        }
    }
}

pub(crate) fn canonical_v23_incidence_screen_result_bytes(
    result: &V23IncidenceScreenResult,
    expected_authority: &V23IncidenceScreenAuthority,
) -> Result<Vec<u8>> {
    let roles = [
        ("tree-receipt", "sha256"),
        ("incidence-tree", "blake3"),
        ("posting-receipt", "sha256"),
        ("incidence-postings-one", "blake3"),
        ("incidence-postings-two", "blake3"),
        ("d2-report", "sha256"),
        ("query-parquet", "sha256"),
    ];
    if result.authority != *expected_authority
        || !exact_lower_hex(&result.authority.source_commit, 40)
        || !exact_lower_hex(&result.authority.source_archive_sha256, 64)
        || result.authority.index_id.is_empty()
        || result.authority.objects.len() != roles.len()
        || result
            .authority
            .objects
            .iter()
            .zip(roles)
            .any(|(object, (role, algorithm))| {
                object.role != role
                    || object.digest_algorithm != algorithm
                    || !object.uri.starts_with("s3://")
                    || !exact_lower_hex(&object.digest, 64)
                    || object.encoded_bytes == 0
            })
    {
        return Err(invalid("V23 incidence screen authority differs"));
    }
    let ladder = V23IncidenceCell::registered_ladder();
    let validate_cells = |cells: &[V23IncidenceScreenCellResult],
                          selector: V23IncidenceScreenSelector|
     -> Result<bool> {
        if cells.len() != ladder.len() {
            return Err(invalid("V23 incidence screen cell count differs"));
        }
        let mut any_passed = false;
        for (cell, expected_cell) in cells.iter().zip(&ladder) {
            let expected_scores = match selector {
                V23IncidenceScreenSelector::TreeBeam => {
                    v23_tree_beam_centroid_scores(usize::from(expected_cell.beam_width))?
                }
                V23IncidenceScreenSelector::ExhaustiveControl => 65_536,
            };
            let quality = recompute_v23_incidence_quality(
                &cell
                    .selections
                    .iter()
                    .map(|selection| (selection.query_ordinal, selection.page_ordinals.clone()))
                    .collect::<Vec<_>>(),
                &result.development_truth,
                28_282,
            )?;
            let projection =
                project_v23_incidence_serving_bytes(100_000_000, usize::from(cell.cell.cap))?;
            if cell.selector != selector
                || cell.cell != *expected_cell
                || cell.scored_centroids_per_query != expected_scores
                || cell.distance_dimensions_per_query != expected_scores * 96
                || cell.quality != quality
                || cell.projected_serving_bytes != projection.total_bytes
                || cell.maximum_posting_visits > 262_144
                || cell.maximum_touched_pages > 8_192
                || cell.maximum_touched_pages > cell.maximum_posting_visits
                || !cell.determinism_passed
            {
                return Err(invalid("V23 incidence screen cell evidence differs"));
            }
            any_passed |= quality.passed;
        }
        Ok(any_passed)
    };
    let tree_beam_passed = validate_cells(&result.tree_beam, V23IncidenceScreenSelector::TreeBeam)?;
    let exhaustive_control_passed = validate_cells(
        &result.exhaustive_control,
        V23IncidenceScreenSelector::ExhaustiveControl,
    )?;
    let expected_selected = result
        .tree_beam
        .iter()
        .find(|cell| cell.quality.passed)
        .map(|cell| cell.cell);
    if result.schema != "borsuk-v23-incidence-development-screen-v1"
        || result.claim_eligible
        || result.tree_beam_passed != tree_beam_passed
        || result.exhaustive_control_passed != exhaustive_control_passed
        || result.selected_cell != expected_selected
        || result.page_body_reads != 0
        || result.holdout_rows_read != 0
        || result.classification
            != classify_v23_incidence_screen(
                result.exhaustive_control_passed,
                result.tree_beam_passed,
            )
    {
        return Err(invalid("V23 incidence screen result differs"));
    }
    let value = serde_json::to_value(result)
        .map_err(|_| invalid("V23 incidence screen result serialization failed"))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|_| invalid("V23 incidence screen result serialization failed"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn evaluate_v23_incidence_screen_cell(
    tree: &V23IncidenceTree,
    plane: &V23PostingPlane,
    cell: V23IncidenceCell,
    selector: V23IncidenceScreenSelector,
    queries: &[[f32; 96]],
    truth: &[V23IncidenceQueryTruth],
    page_count: usize,
    expected_leaves: usize,
) -> Result<V23IncidenceScreenCellResult> {
    if queries.len() != 32 || truth.len() != 32 || cell.arm != plane.arm {
        return Err(invalid("V23 incidence screen cohort differs"));
    }
    posting_prefix_eligibility(plane, usize::from(cell.cap))?;
    let mut selections = Vec::with_capacity(32);
    let mut maximum_posting_visits = 0_u32;
    let mut maximum_touched_pages = 0_u32;
    for (query, expected) in queries.iter().zip(truth) {
        let ranked = match selector {
            V23IncidenceScreenSelector::TreeBeam => {
                let ranked =
                    rank_v23_incidence_tree_beam(tree, query, usize::from(cell.beam_width))?;
                if ranked
                    != rank_v23_incidence_tree_beam_scalar(
                        tree,
                        query,
                        usize::from(cell.beam_width),
                    )?
                {
                    return Err(invalid(
                        "V23 incidence screen tree-beam determinism differs",
                    ));
                }
                ranked
            }
            V23IncidenceScreenSelector::ExhaustiveControl => {
                let ranked = rank_incidence_leaves_with_shape(
                    tree,
                    query,
                    usize::from(cell.beam_width),
                    expected_leaves,
                )?;
                if ranked
                    != rank_incidence_leaves_scalar_with_shape(
                        tree,
                        query,
                        usize::from(cell.beam_width),
                        expected_leaves,
                    )?
                {
                    return Err(invalid(
                        "V23 incidence screen exhaustive-control determinism differs",
                    ));
                }
                ranked
            }
        };
        let mut workspace = V23IncidenceQueryWorkspace::new(page_count)?;
        let (page_ordinals, posting_visits, touched_pages) =
            selected_pages_q32(plane, &ranked, usize::from(cell.cap), &mut workspace)?;
        if page_ordinals
            != selected_pages_scalar(plane, &ranked, usize::from(cell.cap), page_count)?
        {
            return Err(invalid("V23 incidence screen page reducer differs"));
        }
        maximum_posting_visits = maximum_posting_visits.max(posting_visits);
        maximum_touched_pages = maximum_touched_pages.max(touched_pages);
        selections.push(V23IncidenceSelection {
            query_ordinal: expected.query_ordinal,
            page_ordinals,
        });
    }
    let quality = recompute_v23_incidence_quality(
        &selections
            .iter()
            .map(|selection| (selection.query_ordinal, selection.page_ordinals.clone()))
            .collect::<Vec<_>>(),
        truth,
        page_count,
    )?;
    let scored_centroids_per_query = match selector {
        V23IncidenceScreenSelector::TreeBeam => {
            v23_tree_beam_centroid_scores_for_depth(tree.shape.depth, usize::from(cell.beam_width))?
        }
        V23IncidenceScreenSelector::ExhaustiveControl => u32::try_from(expected_leaves)
            .map_err(|_| invalid("V23 incidence screen leaf count exceeds u32"))?,
    };
    Ok(V23IncidenceScreenCellResult {
        selector,
        cell,
        scored_centroids_per_query,
        distance_dimensions_per_query: scored_centroids_per_query
            .checked_mul(96)
            .ok_or_else(|| invalid("V23 incidence screen dimensions overflow"))?,
        quality,
        projected_serving_bytes: project_v23_incidence_serving_bytes(
            100_000_000,
            usize::from(cell.cap),
        )?
        .total_bytes,
        maximum_posting_visits,
        maximum_touched_pages,
        determinism_passed: true,
        selections,
    })
}

fn evaluate_v23_incidence_development_screen_with_shape(
    tree: &V23IncidenceTree,
    one: &V23PostingPlane,
    two: &V23PostingPlane,
    queries: &[[f32; 96]],
    truth: &[V23IncidenceQueryTruth],
    page_count: usize,
    expected_leaves: usize,
    authority: V23IncidenceScreenAuthority,
) -> Result<V23IncidenceScreenResult> {
    if one.arm != PostingAssignmentArm::OneLeaf
        || two.arm != PostingAssignmentArm::TwoBeamLeaves
        || tree.leaves.len() != expected_leaves
    {
        return Err(invalid("V23 incidence screen artifact shape differs"));
    }
    let mut tree_beam = Vec::with_capacity(18);
    let mut exhaustive_control = Vec::with_capacity(18);
    for cell in V23IncidenceCell::registered_ladder() {
        let plane = match cell.arm {
            PostingAssignmentArm::OneLeaf => one,
            PostingAssignmentArm::TwoBeamLeaves => two,
        };
        tree_beam.push(evaluate_v23_incidence_screen_cell(
            tree,
            plane,
            cell,
            V23IncidenceScreenSelector::TreeBeam,
            queries,
            truth,
            page_count,
            expected_leaves,
        )?);
        exhaustive_control.push(evaluate_v23_incidence_screen_cell(
            tree,
            plane,
            cell,
            V23IncidenceScreenSelector::ExhaustiveControl,
            queries,
            truth,
            page_count,
            expected_leaves,
        )?);
    }
    let cell_passed = |cell: &V23IncidenceScreenCellResult| {
        cell.quality.passed
            && cell.projected_serving_bytes <= 3 * 1024 * 1024 * 1024
            && cell.maximum_posting_visits <= 262_144
            && cell.maximum_touched_pages <= 8_192
            && cell.determinism_passed
    };
    let tree_beam_passed = tree_beam.iter().any(cell_passed);
    let exhaustive_control_passed = exhaustive_control.iter().any(cell_passed);
    let selected_cell = tree_beam
        .iter()
        .find(|cell| cell_passed(cell))
        .map(|cell| cell.cell);
    Ok(V23IncidenceScreenResult {
        schema: "borsuk-v23-incidence-development-screen-v1".to_string(),
        claim_eligible: false,
        authority,
        development_truth: truth.to_vec(),
        tree_beam,
        exhaustive_control,
        exhaustive_control_passed,
        tree_beam_passed,
        selected_cell,
        classification: classify_v23_incidence_screen(exhaustive_control_passed, tree_beam_passed),
        page_body_reads: 0,
        holdout_rows_read: 0,
    })
}

pub(crate) fn evaluate_v23_incidence_development_screen(
    tree: &V23IncidenceTree,
    one: &V23PostingPlane,
    two: &V23PostingPlane,
    queries: &[[f32; 96]],
    truth: &[V23IncidenceQueryTruth],
    authority: V23IncidenceScreenAuthority,
) -> Result<V23IncidenceScreenResult> {
    evaluate_v23_incidence_development_screen_with_shape(
        tree,
        one,
        two,
        queries,
        truth,
        28_282,
        V23_INCIDENCE_LEAVES,
        authority,
    )
}

#[cfg(test)]
pub(crate) fn evaluate_v23_incidence_development_screen_test_shape(
    tree: &V23IncidenceTree,
    one: &V23PostingPlane,
    two: &V23PostingPlane,
    queries: &[[f32; 96]],
    truth: &[V23IncidenceQueryTruth],
    page_count: usize,
    authority: V23IncidenceScreenAuthority,
) -> Result<V23IncidenceScreenResult> {
    evaluate_v23_incidence_development_screen_with_shape(
        tree,
        one,
        two,
        queries,
        truth,
        page_count,
        tree.leaves.len(),
        authority,
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23IncidenceCampaignResult {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) query_router: String,
    pub(crate) source_commit: String,
    pub(crate) source_archive_sha256: String,
    pub(crate) index_id: String,
    pub(crate) dataset_id: String,
    pub(crate) query_cohort_sha256: String,
    pub(crate) tree_blake3: String,
    pub(crate) posting_one_blake3: String,
    pub(crate) posting_two_blake3: String,
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
            schema: "borsuk-v23-incidence-result-v2".to_string(),
            claim_eligible: false,
            query_router: "centroid-tree-beam-v1".to_string(),
            source_commit: "1".repeat(40),
            source_archive_sha256: "2".repeat(64),
            index_id: "index-fixture".to_string(),
            dataset_id: "deep-image-96".to_string(),
            query_cohort_sha256: "3".repeat(64),
            tree_blake3: "4".repeat(64),
            posting_one_blake3: "5".repeat(64),
            posting_two_blake3: "6".repeat(64),
            executable_sha256: "7".repeat(64),
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
    if result.schema != "borsuk-v23-incidence-result-v2"
        || result.claim_eligible
        || result.query_router != "centroid-tree-beam-v1"
        || result.page_body_reads != 0
        || !exact_lower_hex(&result.source_commit, 40)
        || !exact_lower_hex(&result.source_archive_sha256, 64)
        || !exact_lower_hex(&result.query_cohort_sha256, 64)
        || !exact_lower_hex(&result.tree_blake3, 64)
        || !exact_lower_hex(&result.posting_one_blake3, 64)
        || !exact_lower_hex(&result.posting_two_blake3, 64)
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
        let scored_centroids = v23_tree_beam_centroid_scores(usize::from(cell.cell.beam_width))?;
        if cell.scored_centroids_per_query != scored_centroids
            || cell.distance_dimensions_per_query != scored_centroids * 96
            || cell.projected_serving_bytes != projection.total_bytes
            || cell.maximum_posting_visits
                > u32::from(cell.cell.cap) * u32::from(cell.cell.beam_width)
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
                > u32::from(holdout.cell.cap) * u32::from(holdout.cell.beam_width)
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

#[derive(Debug, Clone, Copy)]
struct V23IncidenceEvaluationShape {
    page_count: usize,
    expected_leaves: usize,
}

fn evaluate_v23_incidence_cell_with_shape(
    tree: &V23IncidenceTree,
    plane: &V23PostingPlane,
    cell: V23IncidenceCell,
    queries: &[[f32; 96]],
    truth: &[V23IncidenceQueryTruth],
    latency_artifact: &[u8],
    shape: V23IncidenceEvaluationShape,
) -> Result<V23IncidenceCellResult> {
    if queries.is_empty() || queries.len() != truth.len() {
        return Err(invalid("V23 incidence evaluation cohort differs"));
    }
    let retention_passed = posting_prefix_eligibility(plane, usize::from(cell.cap))?;
    let latency_samples = decode_v23_incidence_latency_samples(latency_artifact)?;
    let p99_ns = v23_incidence_latency_p99_ns(&latency_samples)?;
    let mut selections = Vec::with_capacity(queries.len());
    let mut maximum_posting_visits = 0_u32;
    let mut maximum_touched_pages = 0_u32;
    let mut determinism_passed = true;
    for (query, expected) in queries.iter().zip(truth) {
        let evidence = score_incidence_query_with_shape(
            tree,
            plane,
            cell,
            query,
            shape.page_count,
            shape.expected_leaves,
        )?;
        maximum_posting_visits = maximum_posting_visits.max(evidence.posting_visits);
        maximum_touched_pages = maximum_touched_pages.max(evidence.touched_pages);
        determinism_passed &= evidence.scalar_pages_equal;
        selections.push((expected.query_ordinal, evidence.page_ordinals));
    }
    let quality = recompute_v23_incidence_quality(&selections, truth, shape.page_count)?;
    let projection = project_v23_incidence_serving_bytes(100_000_000, usize::from(cell.cap))?;
    Ok(V23IncidenceCellResult {
        cell,
        scored_centroids_per_query: v23_tree_beam_centroid_scores(usize::from(cell.beam_width))?,
        distance_dimensions_per_query: v23_tree_beam_centroid_scores(usize::from(cell.beam_width))?
            .checked_mul(96)
            .ok_or_else(|| invalid("V23 incidence tree-beam dimensions overflow"))?,
        retention_passed,
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

pub(crate) fn evaluate_v23_incidence_cell(
    tree: &V23IncidenceTree,
    plane: &V23PostingPlane,
    cell: V23IncidenceCell,
    queries: &[[f32; 96]],
    truth: &[V23IncidenceQueryTruth],
    page_count: usize,
    latency_artifact: &[u8],
) -> Result<V23IncidenceCellResult> {
    evaluate_v23_incidence_cell_with_shape(
        tree,
        plane,
        cell,
        queries,
        truth,
        latency_artifact,
        V23IncidenceEvaluationShape {
            page_count,
            expected_leaves: V23_INCIDENCE_LEAVES,
        },
    )
}

#[cfg(test)]
pub(crate) fn evaluate_v23_incidence_cell_test_shape(
    tree: &V23IncidenceTree,
    plane: &V23PostingPlane,
    cell: V23IncidenceCell,
    queries: &[[f32; 96]],
    truth: &[V23IncidenceQueryTruth],
    page_count: usize,
    latency_artifact: &[u8],
) -> Result<V23IncidenceCellResult> {
    evaluate_v23_incidence_cell_with_shape(
        tree,
        plane,
        cell,
        queries,
        truth,
        latency_artifact,
        V23IncidenceEvaluationShape {
            page_count,
            expected_leaves: tree.leaves.len(),
        },
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V23IncidenceServingProjection {
    pub(crate) projected_pages: u64,
    pub(crate) decoded_tree_bytes: u64,
    pub(crate) beam_workspace_bytes: u64,
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
    let decoded_tree_bytes = 64_u64 * 1024 * 1024;
    let beam_workspace_bytes = 4_096_u64;
    let posting_bytes = 65_536_u64
        .checked_mul(cap as u64)
        .and_then(|value| value.checked_mul(6))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 incidence serving projection overflows".to_string())
        })?;
    let touched_workspace_bytes = 262_144_u64 * 4;
    let page_reference_bytes = projected_pages.checked_mul(12).ok_or_else(|| {
        BorsukError::InvalidStorage("V23 incidence serving projection overflows".to_string())
    })?;
    let page_workspace_bytes = projected_pages.checked_mul(320).ok_or_else(|| {
        BorsukError::InvalidStorage("V23 incidence serving projection overflows".to_string())
    })?;
    let total_bytes = [
        decoded_tree_bytes,
        beam_workspace_bytes,
        262_148,
        posting_bytes,
        page_reference_bytes,
        touched_workspace_bytes,
        page_workspace_bytes,
        3_932_160,
        536_870_912,
        268_435_456,
    ]
    .into_iter()
    .try_fold(0_u64, |sum, value| sum.checked_add(value))
    .ok_or_else(|| {
        BorsukError::InvalidStorage("V23 incidence serving projection overflows".to_string())
    })?;
    Ok(V23IncidenceServingProjection {
        projected_pages,
        decoded_tree_bytes,
        beam_workspace_bytes,
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
        V23IncidenceCell, V23IncidenceDevelopmentArtifact, V23IncidenceDevelopmentAuthority,
        V23IncidenceHoldoutTruthArtifact, V23IncidenceHoldoutTruthAuthority,
        V23IncidenceQueryTruth, V23IncidenceQueryWorkspace, V23IncidenceScreenClass,
        V23IncidenceScreenResult, bind_v23_incidence_holdout_truth,
        canonical_v23_incidence_development_artifact_bytes,
        canonical_v23_incidence_holdout_truth_bytes, canonical_v23_incidence_result_bytes,
        canonical_v23_incidence_screen_result_bytes, classify_v23_incidence_campaign,
        classify_v23_incidence_screen, decode_v23_incidence_development_latency_bundle,
        decode_v23_incidence_latency_samples, encode_v23_incidence_development_latency_bundle,
        encode_v23_incidence_latency_samples, evaluate_v23_incidence_cell,
        evaluate_v23_incidence_development_screen_test_shape,
        measure_v23_incidence_evaluation_preflight, measure_v23_incidence_latency,
        project_v23_incidence_serving_bytes, rank_incidence_leaves, rank_incidence_leaves_scalar,
        read_v23_incidence_development_queries, read_v23_incidence_development_truth,
        read_v23_incidence_holdout_neighbors, read_v23_incidence_holdout_queries,
        recompute_v23_incidence_layout_quality, recompute_v23_incidence_quality,
        score_incidence_query, score_incidence_query_native, v23_incidence_latency_p99_ns,
    };
    use crate::{
        v23_incidence_postings::{
            PostingAssignmentArm, V23PostingLeaf, V23PostingPlane, V23PostingPrefixEvidence,
        },
        v23_incidence_tree::{
            V23IncidenceTrainingShape, V23IncidenceTree, V23TrainingWork, V23TreeLeaf, V23TreeNode,
            v23_tree_beam_centroid_scores,
        },
    };

    fn ranking_tree() -> V23IncidenceTree {
        let node_count = 65_535_usize;
        let mut child = [f16::ZERO; 96];
        child[0] = f16::from_f32(1.0);
        let nodes = (0..node_count)
            .map(|ordinal| V23TreeNode {
                child_zero: child,
                child_one: child,
                child_zero_inverse_norm: 1.0,
                child_one_inverse_norm: 1.0,
                boundary_score_bits: 0,
                boundary_source_ordinal: 0,
                child_zero_index: u32::try_from(ordinal * 2 + 1).unwrap(),
                child_one_index: u32::try_from(ordinal * 2 + 2).unwrap(),
            })
            .collect();
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
            nodes,
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
                    pages: vec![leaf % 16, (leaf / 96) % 16],
                    masses: vec![40_000, 25_535],
                    total_mass: 65_535,
                    prefixes,
                })
                .collect(),
        }
    }

    fn screen_tree() -> V23IncidenceTree {
        let mut tree = ranking_tree();
        tree.shape.depth = 7;
        tree.shape.reservoir_rows = 128;
        tree.nodes.truncate(127);
        tree.leaves.truncate(128);
        tree
    }

    fn screen_posting_plane(arm: PostingAssignmentArm) -> V23PostingPlane {
        let mut plane = posting_plane();
        plane.arm = arm;
        plane
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
        for beam_width in [32, 64, 128] {
            let evidence = score_incidence_query(
                &tree,
                &plane,
                V23IncidenceCell {
                    cap: 2048,
                    arm: PostingAssignmentArm::OneLeaf,
                    beam_width,
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
            beam_width: 128,
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
    fn v23_tree_beam_preflight_measures_width_128_and_posting_visits_separately() {
        let measured =
            measure_v23_incidence_evaluation_preflight(&ranking_tree(), &posting_plane(), 4, 16)
                .unwrap();
        assert_eq!(measured.beam_width, 128);
        assert_eq!(measured.scored_centroids_per_query, 2_558);
        assert_eq!(measured.distance_dimensions, 4 * 2_558 * 96);
        assert!(measured.distance_elapsed_ns > 0);
        assert!((1..=4 * 128 * 2048).contains(&measured.posting_visits));
        assert!(measured.posting_elapsed_ns > 0);
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
    fn v23_incidence_eval_development_latency_bundle_binds_all_eighteen_cells() {
        let artifacts = (0..18)
            .map(|ordinal| {
                encode_v23_incidence_latency_samples(&vec![10_000 + ordinal; 10_000]).unwrap()
            })
            .collect::<Vec<_>>();
        let bytes = encode_v23_incidence_development_latency_bundle(&artifacts).unwrap();
        assert_eq!(
            decode_v23_incidence_development_latency_bundle(&bytes).unwrap(),
            artifacts
        );
        for changed in [
            bytes[..bytes.len() - 1].to_vec(),
            {
                let mut changed = bytes.clone();
                changed[8] ^= 1;
                changed
            },
            {
                let mut changed = bytes.clone();
                changed[24] ^= 1;
                changed
            },
        ] {
            assert!(decode_v23_incidence_development_latency_bundle(&changed).is_err());
        }
        assert!(encode_v23_incidence_development_latency_bundle(&artifacts[..17]).is_err());
    }

    #[test]
    fn v23_incidence_eval_development_artifact_recomputes_all_cells_and_seal() {
        let latency = encode_v23_incidence_latency_samples(&vec![1_000_000; 10_000]).unwrap();
        let latencies = vec![latency; 18];
        let mut base = V23IncidenceCampaignInput::passing_fixture()
            .development
            .remove(0);
        base.p99_ns = 1_000_000;
        base.latency_blake3 = blake3::hash(&latencies[0]).to_hex().to_string();
        base.latency_bytes = latencies[0].len() as u64;
        let development = V23IncidenceCell::registered_ladder()
            .into_iter()
            .map(|cell| {
                base.cell = cell;
                base.scored_centroids_per_query =
                    v23_tree_beam_centroid_scores(usize::from(cell.beam_width)).unwrap();
                base.distance_dimensions_per_query =
                    base.scored_centroids_per_query.checked_mul(96).unwrap();
                base.projected_serving_bytes =
                    project_v23_incidence_serving_bytes(100_000_000, cell.cap as usize)
                        .unwrap()
                        .total_bytes;
                base.clone()
            })
            .collect::<Vec<_>>();
        let authority = V23IncidenceDevelopmentAuthority {
            query_router: "centroid-tree-beam-v1".to_string(),
            source_commit: "1".repeat(40),
            source_archive_sha256: "2".repeat(64),
            index_id: "index-fixture".to_string(),
            dataset_id: "deep-image-96".to_string(),
            query_cohort_sha256: "3".repeat(64),
            tree_blake3: "4".repeat(64),
            posting_one_blake3: "5".repeat(64),
            posting_two_blake3: "6".repeat(64),
            executable_sha256: "7".repeat(64),
        };
        let artifact = V23IncidenceDevelopmentArtifact {
            schema: "borsuk-v23-incidence-development-v2".to_string(),
            claim_eligible: false,
            authority: authority.clone(),
            development,
            development_truth: canonical_truth(0, 32),
            sealed_cell: Some(V23IncidenceCell::registered_ladder()[0]),
        };
        let bytes = canonical_v23_incidence_development_artifact_bytes(
            &artifact,
            &authority,
            &latencies,
            &canonical_truth(0, 32),
        )
        .unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));

        let mut changed = artifact.clone();
        changed.authority.tree_blake3 = "8".repeat(64);
        assert!(
            canonical_v23_incidence_development_artifact_bytes(
                &changed,
                &authority,
                &latencies,
                &canonical_truth(0, 32),
            )
            .is_err()
        );
        let mut changed = artifact.clone();
        changed.development_truth[0].ground_truth_page_assignments[0][0] = 9;
        assert!(
            canonical_v23_incidence_development_artifact_bytes(
                &changed,
                &authority,
                &latencies,
                &canonical_truth(0, 32),
            )
            .is_err()
        );
        let mut changed = artifact;
        changed.development[3].selections[0].page_ordinals[1] =
            changed.development[3].selections[0].page_ordinals[0];
        assert!(
            canonical_v23_incidence_development_artifact_bytes(
                &changed,
                &authority,
                &latencies,
                &canonical_truth(0, 32),
            )
            .is_err()
        );
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
    fn v23_incidence_eval_development_truth_rejects_nonartifact_outer_schema() {
        assert!(read_v23_incidence_development_truth(b"{}\n").is_err());
        assert!(
            read_v23_incidence_development_truth(
                br#"{"claim_eligible":false,"report":null,"schema":"borsuk-v23-d2-artifact-v1"}\n"#,
            )
            .is_err()
        );
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
    fn v23_incidence_eval_exact_eight_pads_low_cardinality_candidate_set() {
        let neighbors = (32..160_u32)
            .map(|query| (query, (0..100_u64).collect::<Vec<_>>()))
            .collect::<Vec<_>>();
        let pages = (0..100_u64)
            .map(|id| (id, vec![(id % 4) as u32]))
            .collect::<BTreeMap<_, _>>();
        let truth = bind_v23_incidence_holdout_truth(&neighbors, &pages, 16).unwrap();
        assert_eq!(truth.len(), 128);
        assert!(
            truth
                .iter()
                .all(|query| query.oracle_pages == (0..8).collect::<Vec<_>>())
        );
    }

    #[test]
    fn v23_incidence_eval_oracle_does_not_pad_existing_exact_eight_superset() {
        let assignments = vec![
            vec![0, 10],
            vec![1, 11],
            vec![2],
            vec![3],
            vec![4],
            vec![5],
            vec![6],
            vec![7],
            vec![8],
            vec![9],
        ];
        assert_eq!(
            super::exact_coverage_candidates(&assignments, 28_282).unwrap(),
            (0..12).collect::<Vec<_>>()
        );
    }

    #[test]
    fn v23_incidence_eval_holdout_truth_artifact_recomputes_layout_and_bindings() {
        let truth = canonical_truth(32, 128);
        let authority = V23IncidenceHoldoutTruthAuthority {
            development_result_sha256: "1".repeat(64),
            neighbors_sha256: "2".repeat(64),
            page_roster_sha256: "3".repeat(64),
        };
        let cell = V23IncidenceCell::registered_ladder()[0];
        let artifact = V23IncidenceHoldoutTruthArtifact {
            schema: "borsuk-v23-incidence-holdout-truth-v2".to_string(),
            claim_eligible: false,
            authority: authority.clone(),
            sealed_cell: cell,
            layout: recompute_v23_incidence_layout_quality(&truth).unwrap(),
            truth,
        };
        assert_eq!(
            canonical_v23_incidence_holdout_truth_bytes(&artifact, &authority, cell)
                .unwrap()
                .last(),
            Some(&b'\n')
        );
        let mut changed = artifact.clone();
        changed.authority.neighbors_sha256 = "4".repeat(64);
        assert!(canonical_v23_incidence_holdout_truth_bytes(&changed, &authority, cell).is_err());
        let mut changed = artifact;
        changed.truth[0].oracle_pages.pop();
        assert!(canonical_v23_incidence_holdout_truth_bytes(&changed, &authority, cell).is_err());
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
            beam_width: 32,
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

        let mut below_retention = plane.clone();
        for leaf in &mut below_retention.leaves {
            leaf.prefixes[0].retained_assignments = 0;
            leaf.prefixes[0].retained_mass_ppm = 0;
        }
        let below = evaluate_v23_incidence_cell(
            &tree,
            &below_retention,
            cell,
            &[query],
            &truth,
            16,
            &latency,
        )
        .unwrap();
        assert!(!below.retention_passed);
        assert_eq!(below.quality, result.quality);

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
                beam_width: 32,
            }
        );
        assert_eq!(
            ladder[17],
            V23IncidenceCell {
                cap: 2048,
                arm: PostingAssignmentArm::TwoBeamLeaves,
                beam_width: 128,
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
    fn v23_tree_beam_preflight_serving_projection_is_exact_at_maximum_cell() {
        let projection = project_v23_incidence_serving_bytes(100_000_000, 2048).unwrap();
        assert_eq!(projection.projected_pages, 283_104);
        assert_eq!(projection.decoded_tree_bytes, 64 * 1024 * 1024);
        assert_eq!(projection.beam_workspace_bytes, 4_096);
        assert_eq!(projection.posting_bytes, 805_306_368);
        assert_eq!(projection.touched_workspace_bytes, 1_048_576);
        assert_eq!(projection.total_bytes, 1_776_959_108);
        assert!(project_v23_incidence_serving_bytes(u64::MAX, 2048).is_err());
        assert!(project_v23_incidence_serving_bytes(100_000_000, 4096).is_err());
    }

    #[test]
    fn v23_incidence_eval_canonical_result_recomputes_every_gate_and_latency_artifact() {
        let samples = vec![15_000_000_u64; 10_000];
        let latency = encode_v23_incidence_latency_samples(&samples).unwrap();
        let mut result = V23IncidenceCampaignResult::passing_fixture(&latency);
        result.posting_two_blake3 = "not-a-digest".to_string();
        assert!(canonical_fixture(&result, &latency).is_err());
        let mut result = V23IncidenceCampaignResult::passing_fixture(&latency);
        let bytes = canonical_fixture(&result, &latency).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["schema"], "borsuk-v23-incidence-result-v2");
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

    #[test]
    fn v23_tree_beam_evaluation_contract_versions_router_and_work() {
        let cell = V23IncidenceCell {
            cap: 512,
            arm: PostingAssignmentArm::OneLeaf,
            beam_width: 32,
        };
        assert_eq!(V23IncidenceCell::registered_ladder()[0], cell);

        let latency = encode_v23_incidence_latency_samples(&vec![15_000_000; 10_000]).unwrap();
        let result = V23IncidenceCampaignResult::passing_fixture(&latency);
        assert_eq!(result.schema, "borsuk-v23-incidence-result-v2");
        assert_eq!(result.query_router, "centroid-tree-beam-v1");
        assert_eq!(
            result.campaign.development[0].scored_centroids_per_query,
            766
        );
        assert_eq!(
            result.campaign.development[0].distance_dimensions_per_query,
            73_536
        );
    }

    #[test]
    fn v23_tree_beam_evaluation_rejects_schema_router_work_and_unknown_field_drift() {
        let latency = encode_v23_incidence_latency_samples(&vec![15_000_000; 10_000]).unwrap();
        let base = V23IncidenceCampaignResult::passing_fixture(&latency);

        let mut changed = base.clone();
        changed.schema = "borsuk-v23-incidence-result-v1".to_string();
        assert!(canonical_fixture(&changed, &latency).is_err());
        let mut changed = base.clone();
        changed.query_router = "exhaustive-leaf-scan".to_string();
        assert!(canonical_fixture(&changed, &latency).is_err());
        let mut changed = base.clone();
        changed.campaign.development[0].scored_centroids_per_query += 1;
        assert!(canonical_fixture(&changed, &latency).is_err());
        let mut changed = base.clone();
        changed.campaign.development[0].distance_dimensions_per_query += 96;
        assert!(canonical_fixture(&changed, &latency).is_err());
        let mut changed = base.clone();
        changed.campaign.development[0].cell.beam_width = 31;
        assert!(canonical_fixture(&changed, &latency).is_err());

        let bytes = canonical_fixture(&base, &latency).unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("legacy_alias".to_string(), serde_json::json!(true));
        assert!(serde_json::from_value::<V23IncidenceCampaignResult>(value).is_err());

        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value["campaign"]["development"][0]
            .as_object_mut()
            .unwrap()
            .insert("probes".to_string(), serde_json::json!(32));
        assert!(serde_json::from_value::<V23IncidenceCampaignResult>(value).is_err());
    }

    #[test]
    fn v23_incidence_screen_classification_has_tree_beam_precedence() {
        assert_eq!(
            classify_v23_incidence_screen(false, false),
            V23IncidenceScreenClass::LeafIncidenceQualityRejected
        );
        assert_eq!(
            classify_v23_incidence_screen(true, false),
            V23IncidenceScreenClass::TreeBeamSelectorRejected
        );
        for exhaustive_passed in [false, true] {
            assert_eq!(
                classify_v23_incidence_screen(exhaustive_passed, true),
                V23IncidenceScreenClass::TreeBeamScreenPassed
            );
        }
    }

    #[test]
    fn v23_incidence_screen_result_is_canonical_claim_ineligible_and_exact() {
        let result = V23IncidenceScreenResult::passing_fixture();
        let expected_authority = result.authority.clone();
        let bytes =
            canonical_v23_incidence_screen_result_bytes(&result, &expected_authority).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value["schema"],
            "borsuk-v23-incidence-development-screen-v1"
        );
        assert_eq!(value["claim_eligible"], false);
        assert_eq!(value["page_body_reads"], 0);
        assert_eq!(value["holdout_rows_read"], 0);
        assert_eq!(value["classification"], "tree-beam-screen-passed");
        assert_eq!(result.tree_beam.len(), 18);
        assert_eq!(result.exhaustive_control.len(), 18);
        assert_eq!(
            result.selected_cell,
            Some(V23IncidenceCell::registered_ladder()[0])
        );
        assert_eq!(
            result
                .authority
                .objects
                .iter()
                .map(|object| object.role.as_str())
                .collect::<Vec<_>>(),
            [
                "tree-receipt",
                "incidence-tree",
                "posting-receipt",
                "incidence-postings-one",
                "incidence-postings-two",
                "d2-report",
                "query-parquet",
            ]
        );
        assert_eq!(result.tree_beam[0].scored_centroids_per_query, 766);
        assert_eq!(
            result.exhaustive_control[0].scored_centroids_per_query,
            65_536
        );

        let mut changed = result.clone();
        changed.claim_eligible = true;
        assert!(
            canonical_v23_incidence_screen_result_bytes(&changed, &expected_authority).is_err()
        );
        let mut changed = result.clone();
        changed.classification = V23IncidenceScreenClass::TreeBeamSelectorRejected;
        assert!(
            canonical_v23_incidence_screen_result_bytes(&changed, &expected_authority).is_err()
        );
        let mut changed = result;
        changed.page_body_reads = 1;
        assert!(
            canonical_v23_incidence_screen_result_bytes(&changed, &expected_authority).is_err()
        );

        let result = V23IncidenceScreenResult::passing_fixture();
        let expected_authority = result.authority.clone();
        let mut changed = result.clone();
        changed.authority.objects[0].digest = "0".repeat(64);
        assert!(
            canonical_v23_incidence_screen_result_bytes(&changed, &expected_authority).is_err()
        );
        let mut changed = result.clone();
        changed.tree_beam[0].scored_centroids_per_query += 1;
        assert!(
            canonical_v23_incidence_screen_result_bytes(&changed, &expected_authority).is_err()
        );
        let mut changed = result.clone();
        changed.exhaustive_control[0].distance_dimensions_per_query -= 96;
        assert!(
            canonical_v23_incidence_screen_result_bytes(&changed, &expected_authority).is_err()
        );
        let mut changed = result;
        changed.selected_cell = Some(V23IncidenceCell::registered_ladder()[1]);
        assert!(
            canonical_v23_incidence_screen_result_bytes(&changed, &expected_authority).is_err()
        );
    }

    #[test]
    fn v23_incidence_screen_evaluates_complete_beam_and_exhaustive_ladders() {
        let tree = screen_tree();
        let one = screen_posting_plane(PostingAssignmentArm::OneLeaf);
        let two = screen_posting_plane(PostingAssignmentArm::TwoBeamLeaves);
        let mut query = [0.0_f32; 96];
        query[0] = 1.0;
        let queries = vec![query; 32];
        let truth = canonical_truth(0, 32);
        let authority = V23IncidenceScreenResult::passing_fixture().authority;
        let result = evaluate_v23_incidence_development_screen_test_shape(
            &tree,
            &one,
            &two,
            &queries,
            &truth,
            16,
            authority.clone(),
        )
        .unwrap();
        assert_eq!(result.tree_beam.len(), 18);
        assert_eq!(result.exhaustive_control.len(), 18);
        assert_eq!(result.tree_beam[0].scored_centroids_per_query, 190);
        assert_eq!(result.exhaustive_control[0].scored_centroids_per_query, 128);
        assert!(
            result
                .tree_beam
                .iter()
                .chain(&result.exhaustive_control)
                .flat_map(|cell| &cell.selections)
                .all(|selection| selection.page_ordinals.len() == 8)
        );
        assert_eq!(result.tree_beam[0].distance_dimensions_per_query, 190 * 96);
        assert_eq!(
            result.exhaustive_control[0].distance_dimensions_per_query,
            128 * 96
        );
    }
}
