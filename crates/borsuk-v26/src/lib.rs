//! Fail-fast contracts for the prerelease BORSUK V26 page layout.

#![allow(
    missing_docs,
    reason = "unpublished internal prerelease contract crate; not a compatibility surface"
)]

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
};

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

mod local;
mod tree;

pub use local::{
    V26ArrowColdVectors, V26ArrowFileIdentity, V26CandidateCoverRequest, V26CentroidRouterRequest,
    V26ColdVectorManifest, V26ColdVectorRead, V26ExactGlobalRequest, V26LayoutBuildOutput,
    V26LayoutBuildRequest, V26LayoutEvaluationRequest, V26LocalObjectPath,
    V26PageModeRouterRequest, V26Pq8CoverRequest, V26Pq16GlobalPreflightRequest,
    V26Pq16GlobalPreflightResult, V26Pq16IndexManifest, V26Pq16RerankRequest,
    V26Pq16ServingBenchmarkRequest, V26Pq16ServingBenchmarkResult, V26Pq16ServingBuildOutput,
    V26Pq16ServingBuildRequest, V26Pq16ServingRuntime, V26Pq16ServingRuntimeRequest,
    V26PqWidthLadderRequest, V26ServingLatencySample, V26SimHashPq16IndexManifest,
    V26SimHashPreflightArmResult, V26SimHashPreflightAuthority, V26SimHashPreflightRequest,
    V26SimHashPreflightResult, V26SimHashPreflightSample, V26TreeRouterRequest,
    V26TruthBuildRequest, build_v26_simhash_pq16_multi_index_from_arrow,
    canonical_v26_layout_build_output_bytes, canonical_v26_pq16_global_preflight_result_bytes,
    canonical_v26_pq16_serving_benchmark_result_bytes,
    canonical_v26_pq16_serving_build_output_bytes, canonical_v26_simhash_preflight_result_bytes,
    evaluate_v26_exact_global, evaluate_v26_layout_oracle, evaluate_v26_simhash_preflight,
    open_v26_pq16_serving_runtime, read_v26_pq16_index_arrow, read_v26_simhash_pq16_index_arrow,
    run_v26_candidate_row_cover, run_v26_centroid_router, run_v26_exact_global,
    run_v26_layout_build, run_v26_layout_build_directory, run_v26_page_mode_router,
    run_v26_pq_width_ladder, run_v26_pq8_candidate_cover, run_v26_pq16_exact_rerank,
    run_v26_pq16_global_preflight, run_v26_pq16_serving_benchmark, run_v26_pq16_serving_build,
    run_v26_simhash_preflight, run_v26_tree_router, run_v26_tree_router_diagnostic,
    run_v26_truth_build, select_v26_pq16_global_pages_from_arrow, select_v26_pq16_pages_from_arrow,
    select_v26_simhash_pq16_pages_from_arrow, v26_construction_schema, v26_page_assignments_schema,
    v26_query_schema, v26_tree_schema, v26_truth_schema, validate_v26_layout_build_output,
    write_v26_cold_vectors_arrow, write_v26_pq16_index_arrow, write_v26_simhash_pq16_index_arrow,
};

pub use tree::{
    V26ConstructionRow, V26Node, V26RowPages, V26Tree, build_v26_dual_tree_layout,
    rank_v26_tree_pages, route_v26_pages, validate_v26_dual_tree_layout,
};

const V26_LAYOUT_SCHEMA: &str = "borsuk-v26-dual-tree-layout-v2";
const V26_PRIMARY_SEED: u64 = 0x5632_362d_5452_4545;
const V26_REPLICA_SEED: u64 = 0x5632_362d_5245_504c;
pub(crate) const V26_PAGE_CAPACITY_LADDER: [u32; 9] =
    [704, 768, 896, 1_024, 1_408, 2_048, 2_816, 4_096, 8_192];
pub(crate) const V26_SERVING_PAGE_BUDGET: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26Error(String);

impl std::fmt::Display for V26Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for V26Error {}

pub type Result<T> = std::result::Result<T, V26Error>;

fn invalid(message: &str) -> V26Error {
    V26Error(message.to_owned())
}

pub fn exact_v26_layout_oracle_pages(
    assignments: &[Vec<u32>],
    page_budget: usize,
) -> Result<Vec<u32>> {
    if assignments.len() != 10
        || page_budget == 0
        || page_budget > V26_SERVING_PAGE_BUDGET
        || assignments.iter().any(|pages| {
            pages.is_empty() || pages.len() > 2 || pages.windows(2).any(|pair| pair[0] >= pair[1])
        })
    {
        return Err(invalid("V26 truth assignments differ"));
    }
    let mut page_masks = BTreeMap::<u32, u16>::new();
    for (neighbor, pages) in assignments.iter().enumerate() {
        for page in pages {
            *page_masks.entry(*page).or_default() |= 1_u16 << neighbor;
        }
    }
    let maximum_pages = page_budget.min(page_masks.len());
    let mut states = vec![None::<([u32; V26_SERVING_PAGE_BUDGET], usize)>; 1 << assignments.len()];
    states[0] = Some(([0; V26_SERVING_PAGE_BUDGET], 0));
    for (page, mask) in page_masks {
        for covered in (0..states.len()).rev() {
            let Some((mut pages, count)) = states[covered] else {
                continue;
            };
            if count == maximum_pages {
                continue;
            }
            let combined = covered | usize::from(mask);
            pages[count] = page;
            let next_count = count + 1;
            if states[combined]
                .as_ref()
                .is_none_or(|(prior, prior_count)| {
                    next_count < *prior_count
                        || (next_count == *prior_count
                            && pages[..next_count] < prior[..*prior_count])
                })
            {
                states[combined] = Some((pages, next_count));
            }
        }
    }
    states
        .into_iter()
        .enumerate()
        .filter_map(|(mask, pages)| pages.map(|pages| (mask.count_ones(), pages)))
        .max_by(
            |(left_hits, (left_pages, left_count)), (right_hits, (right_pages, right_count))| {
                left_hits
                    .cmp(right_hits)
                    .then_with(|| right_count.cmp(left_count))
                    .then_with(|| right_pages[..*right_count].cmp(&left_pages[..*left_count]))
            },
        )
        .map(|(_, (pages, count))| pages[..count].to_vec())
        .filter(|pages| !pages.is_empty())
        .ok_or_else(|| invalid("V26 layout oracle differs"))
}

fn exact_v26_candidate_cover_pages(
    assignments: &[Vec<u32>],
    candidate_pages: &[u32],
    page_budget: usize,
) -> Result<Vec<u32>> {
    if assignments.len() != 10
        || page_budget == 0
        || page_budget > 10
        || candidate_pages.is_empty()
        || candidate_pages.windows(2).any(|pair| pair[0] >= pair[1])
        || assignments.iter().any(|pages| {
            pages.is_empty() || pages.len() > 2 || pages.windows(2).any(|pair| pair[0] >= pair[1])
        })
    {
        return Err(invalid("V26 tree router diagnostic candidates differ"));
    }
    let candidates = candidate_pages.iter().copied().collect::<BTreeSet<_>>();
    let mut page_masks = BTreeMap::<u32, u16>::new();
    for (neighbor, pages) in assignments.iter().enumerate() {
        for page in pages.iter().filter(|page| candidates.contains(page)) {
            *page_masks.entry(*page).or_default() |= 1_u16 << neighbor;
        }
    }
    let maximum_pages = page_budget.min(page_masks.len());
    let mut states = vec![None::<([u32; 10], usize)>; 1 << assignments.len()];
    states[0] = Some(([0; 10], 0));
    for (page, mask) in page_masks {
        for covered in (0..states.len()).rev() {
            let Some((mut pages, count)) = states[covered] else {
                continue;
            };
            if count == maximum_pages {
                continue;
            }
            let combined = covered | usize::from(mask);
            pages[count] = page;
            let next_count = count + 1;
            if states[combined]
                .as_ref()
                .is_none_or(|(prior, prior_count)| {
                    next_count < *prior_count
                        || (next_count == *prior_count
                            && pages[..next_count] < prior[..*prior_count])
                })
            {
                states[combined] = Some((pages, next_count));
            }
        }
    }
    states
        .into_iter()
        .enumerate()
        .filter_map(|(mask, pages)| pages.map(|pages| (mask.count_ones(), pages)))
        .max_by(
            |(left_hits, (left_pages, left_count)), (right_hits, (right_pages, right_count))| {
                left_hits
                    .cmp(right_hits)
                    .then_with(|| right_count.cmp(left_count))
                    .then_with(|| right_pages[..*right_count].cmp(&left_pages[..*left_count]))
            },
        )
        .map(|(_, (pages, count))| pages[..count].to_vec())
        .ok_or_else(|| invalid("V26 tree router diagnostic cover differs"))
}

fn v26_layout_hits(assignments: &[Vec<u32>], selected_pages: &[u32]) -> u32 {
    assignments
        .iter()
        .filter(|pages| {
            pages
                .iter()
                .any(|page| selected_pages.binary_search(page).is_ok())
        })
        .count() as u32
}

fn v26_ppm(numerator: u64, denominator: u64) -> Result<u64> {
    numerator
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_div(denominator))
        .ok_or_else(|| invalid("V26 metric arithmetic differs"))
}

fn exact_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26ObjectIdentity {
    pub role: String,
    pub uri: String,
    pub digest_algorithm: String,
    pub digest: String,
    pub encoded_bytes: u64,
    pub generation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26LayoutAuthority {
    pub schema: String,
    pub generation: String,
    pub source_commit: String,
    pub source_archive_sha256: String,
    pub binary: V26ObjectIdentity,
    pub construction_rows: V26ObjectIdentity,
    pub primary_seed: u64,
    pub replica_seed: u64,
    pub page_capacity: u32,
    pub expected_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26LayoutReceipt {
    pub authority: V26LayoutAuthority,
    pub inputs: Vec<V26ObjectIdentity>,
    pub outputs: Vec<V26ObjectIdentity>,
    pub row_count: u64,
    pub leaves_per_tree: u32,
    pub page_count: u32,
    pub projection_steps: u64,
    pub worker_count: u32,
    pub elapsed_ns: u64,
    pub cpu_ns: u64,
    pub peak_rss_bytes: u64,
    pub peak_psi_full_avg10_milli_percent: u64,
    pub swap_start_bytes: u64,
    pub swap_end_bytes: u64,
    pub query_role_opens: u64,
    pub page_body_reads: u64,
    pub claim_eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26QueryTruth {
    pub query_ordinal: u32,
    pub neighbor_source_ordinals: Vec<u64>,
    pub ground_truth_page_assignments: Vec<Vec<u32>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26LayoutSample {
    pub query_ordinal: u32,
    pub selected_pages: Vec<u32>,
    pub hits: u32,
    pub recall_ppm: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct V26ExternalQuery {
    pub query_ordinal: u32,
    pub vector: [f32; 96],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26ExternalTruth {
    pub query_ordinal: u32,
    pub neighbor_source_ordinals: Vec<u64>,
    pub neighbor_distance_bits: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26RankedRowEvidence {
    pub source_ordinal: u64,
    pub primary_page: u32,
    pub replica_page: u32,
    pub distance_bits: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26ExactGlobalSample {
    pub query_ordinal: u32,
    pub ranked_row_limit: u32,
    pub candidate_rows: u64,
    pub selected_pages: Vec<u32>,
    pub first_ten_ranked_rows: Vec<V26RankedRowEvidence>,
    pub hits: u32,
    pub oracle_hits: u32,
    pub recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26ExactGlobalRankResult {
    pub ranked_row_limit: u32,
    pub aggregate_recall_ppm: u64,
    pub minimum_query_recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
    pub passed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26ExactGlobalResult {
    pub schema: String,
    pub query_count: u32,
    pub rank_results: Vec<V26ExactGlobalRankResult>,
    pub disposition: V26Disposition,
    pub page_body_reads: u64,
    pub claim_eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26TreeRouterSample {
    pub query_ordinal: u32,
    pub selected_pages: Vec<u32>,
    pub hits: u32,
    pub oracle_hits: u32,
    pub recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26TreeRouterResult {
    pub schema: String,
    pub query_count: u32,
    pub aggregate_recall_ppm: u64,
    pub minimum_query_recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
    pub disposition: V26Disposition,
    pub page_body_reads: u64,
    pub claim_eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26TreeRouterWidthSample {
    pub query_ordinal: u32,
    pub candidate_page_limit: u32,
    pub selected_pages: Vec<u32>,
    pub hits: u32,
    pub oracle_hits: u32,
    pub recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26TreeRouterWidthResult {
    pub candidate_page_limit: u32,
    pub aggregate_recall_ppm: u64,
    pub minimum_query_recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
    pub passed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26PageModeSample {
    pub query_ordinal: u32,
    pub mode_count: u32,
    pub candidate_page_limit: u32,
    pub selected_pages: Vec<u32>,
    pub hits: u32,
    pub oracle_hits: u32,
    pub recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26PageModeResult {
    pub mode_count: u32,
    pub candidate_page_limit: u32,
    pub aggregate_recall_ppm: u64,
    pub minimum_query_recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
    pub passed: bool,
}

#[derive(Debug, Clone, Copy)]
struct V26RankedRow {
    source_ordinal: u64,
    distance: f32,
}

impl PartialEq for V26RankedRow {
    fn eq(&self, other: &Self) -> bool {
        self.source_ordinal == other.source_ordinal
            && self.distance.to_bits() == other.distance.to_bits()
    }
}

impl Eq for V26RankedRow {}

impl PartialOrd for V26RankedRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for V26RankedRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.source_ordinal.cmp(&other.source_ordinal))
    }
}

pub fn build_v26_external_truth_rows(
    rows: &[V26ConstructionRow],
    queries: &[V26ExternalQuery],
) -> Result<Vec<V26ExternalTruth>> {
    if rows.len() < 10 || queries.is_empty() {
        return Err(invalid("V26 external truth inventory differs"));
    }
    let mut source_ordinals = BTreeSet::new();
    for row in rows {
        validate_v26_vector(&row.vector)?;
        if !source_ordinals.insert(row.source_ordinal) {
            return Err(invalid("V26 external truth construction inventory differs"));
        }
    }
    if source_ordinals
        .iter()
        .copied()
        .ne(0..u64::try_from(rows.len()).unwrap())
    {
        return Err(invalid("V26 external truth construction inventory differs"));
    }
    queries
        .par_iter()
        .enumerate()
        .map(|(query_index, query)| {
            validate_v26_vector(&query.vector)?;
            if usize::try_from(query.query_ordinal).ok() != Some(query_index) {
                return Err(invalid("V26 external truth query order differs"));
            }
            let mut heap = BinaryHeap::with_capacity(10);
            for row in rows {
                let dot = query
                    .vector
                    .iter()
                    .zip(row.vector)
                    .map(|(left, right)| left * right)
                    .sum::<f32>();
                let ranked = V26RankedRow {
                    source_ordinal: row.source_ordinal,
                    distance: 1.0 - dot,
                };
                if !ranked.distance.is_finite() {
                    return Err(invalid("V26 external truth distance differs"));
                }
                if heap.len() < 10 {
                    heap.push(ranked);
                } else if ranked < *heap.peek().unwrap() {
                    heap.pop();
                    heap.push(ranked);
                }
            }
            let mut ranked = heap.into_vec();
            ranked.sort();
            Ok(V26ExternalTruth {
                query_ordinal: query.query_ordinal,
                neighbor_source_ordinals: ranked.iter().map(|row| row.source_ordinal).collect(),
                neighbor_distance_bits: ranked.iter().map(|row| row.distance.to_bits()).collect(),
            })
        })
        .collect()
}

fn validate_v26_vector(vector: &[f32; 96]) -> Result<()> {
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V26 vector finiteness differs"));
    }
    let norm = vector.iter().map(|value| value * value).sum::<f32>();
    if !norm.is_finite() || (norm - 1.0).abs() > 1.0e-4 {
        return Err(invalid("V26 vector normalization differs"));
    }
    Ok(())
}

fn select_v26_ranked_pages(
    ranked_rows: &[V26RankedRow],
    assignments: &BTreeMap<u64, V26RowPages>,
    page_budget: usize,
) -> Result<Vec<u32>> {
    if ranked_rows.is_empty() || page_budget != 8 {
        return Err(invalid("V26 ranked page request differs"));
    }
    let mut page_scores = BTreeMap::<u32, u64>::new();
    let mut prior = None;
    for (rank_index, row) in ranked_rows.iter().enumerate() {
        if !row.distance.is_finite()
            || prior.is_some_and(|(distance, source): (f32, u64)| {
                row.distance.total_cmp(&distance).is_lt()
                    || row.distance.total_cmp(&distance).is_eq() && row.source_ordinal <= source
            })
        {
            return Err(invalid("V26 ranked row order differs"));
        }
        prior = Some((row.distance, row.source_ordinal));
        let assignment = assignments
            .get(&row.source_ordinal)
            .ok_or_else(|| invalid("V26 ranked row page binding differs"))?;
        if assignment.primary_page == assignment.replica_page {
            return Err(invalid("V26 ranked row page binding differs"));
        }
        let rank =
            u64::try_from(rank_index + 1).map_err(|_| invalid("V26 ranked row rank overflows"))?;
        let weight = (1_u64 << 32) / rank;
        for page in [assignment.primary_page, assignment.replica_page] {
            let score = page_scores.entry(page).or_default();
            *score = score
                .checked_add(weight)
                .ok_or_else(|| invalid("V26 ranked page score overflows"))?;
        }
    }
    let mut pages = page_scores.into_iter().collect::<Vec<_>>();
    pages.sort_by(|(left_page, left_score), (right_page, right_score)| {
        right_score
            .cmp(left_score)
            .then_with(|| left_page.cmp(right_page))
    });
    Ok(pages
        .into_iter()
        .take(page_budget)
        .map(|(page, _)| page)
        .collect())
}

pub fn evaluate_v26_exact_global_external_rows(
    rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
    queries: &[V26ExternalQuery],
    truths: &[V26QueryTruth],
    ranked_row_limits: &[u32],
    page_budget: u32,
) -> Result<Vec<V26ExactGlobalSample>> {
    const LIMITS: [u32; 6] = [10, 32, 128, 512, 2_048, 4_096];
    if rows.is_empty()
        || rows.len() != assignments.len()
        || queries.is_empty()
        || queries.len() != truths.len()
        || ranked_row_limits != LIMITS
        || page_budget != 8
    {
        return Err(invalid("V26 exact-global request differs"));
    }
    let mut rows_by_source = BTreeMap::new();
    for row in rows {
        validate_v26_vector(&row.vector)?;
        if rows_by_source.insert(row.source_ordinal, row).is_some() {
            return Err(invalid("V26 construction source ordinal repeats"));
        }
    }
    if rows_by_source
        .keys()
        .copied()
        .ne(0..u64::try_from(rows.len()).unwrap())
    {
        return Err(invalid("V26 construction source inventory differs"));
    }
    let mut pages_by_source = BTreeMap::new();
    for assignment in assignments {
        if assignment.primary_page == assignment.replica_page
            || pages_by_source
                .insert(assignment.source_ordinal, *assignment)
                .is_some()
        {
            return Err(invalid("V26 page assignment authority differs"));
        }
    }
    if rows_by_source.keys().ne(pages_by_source.keys()) {
        return Err(invalid("V26 construction page inventory differs"));
    }

    let retained_limit = usize::try_from(*LIMITS.last().unwrap()).unwrap();
    let per_query_samples = queries
        .par_iter()
        .zip(truths.par_iter())
        .enumerate()
        .map(
            |(query_index, (query, truth))| -> Result<Vec<V26ExactGlobalSample>> {
                validate_v26_vector(&query.vector)?;
                if usize::try_from(query.query_ordinal).ok() != Some(query_index)
                    || truth.query_ordinal != query.query_ordinal
                    || truth.neighbor_source_ordinals.len() != 10
                    || truth.ground_truth_page_assignments.len() != 10
                    || truth
                        .neighbor_source_ordinals
                        .iter()
                        .copied()
                        .collect::<BTreeSet<_>>()
                        .len()
                        != 10
                {
                    return Err(invalid("V26 exact-global query authority differs"));
                }
                for (neighbor, expected_pages) in truth
                    .neighbor_source_ordinals
                    .iter()
                    .zip(&truth.ground_truth_page_assignments)
                {
                    let assignment = pages_by_source
                        .get(neighbor)
                        .ok_or_else(|| invalid("V26 truth neighbor source differs"))?;
                    let mut observed = vec![assignment.primary_page, assignment.replica_page];
                    observed.sort_unstable();
                    if &observed != expected_pages {
                        return Err(invalid("V26 truth neighbor page binding differs"));
                    }
                }
                let oracle_pages = exact_v26_layout_oracle_pages(
                    &truth.ground_truth_page_assignments,
                    usize::try_from(page_budget).unwrap(),
                )?;
                let oracle_hits =
                    v26_layout_hits(&truth.ground_truth_page_assignments, &oracle_pages);
                let mut heap = BinaryHeap::with_capacity(retained_limit);
                let mut candidate_rows = 0_u64;
                for row in rows_by_source.values() {
                    candidate_rows += 1;
                    let dot = query
                        .vector
                        .iter()
                        .zip(row.vector)
                        .map(|(left, right)| left * right)
                        .sum::<f32>();
                    let ranked = V26RankedRow {
                        source_ordinal: row.source_ordinal,
                        distance: 1.0 - dot,
                    };
                    if !ranked.distance.is_finite() {
                        return Err(invalid("V26 exact-global distance differs"));
                    }
                    if heap.len() < retained_limit {
                        heap.push(ranked);
                    } else if ranked < *heap.peek().unwrap() {
                        heap.pop();
                        heap.push(ranked);
                    }
                }
                let mut ranked = heap.into_vec();
                ranked.sort();
                let first_ten_ranked_rows = ranked
                    .iter()
                    .take(10)
                    .map(|row| {
                        let pages = pages_by_source.get(&row.source_ordinal).unwrap();
                        V26RankedRowEvidence {
                            source_ordinal: row.source_ordinal,
                            primary_page: pages.primary_page,
                            replica_page: pages.replica_page,
                            distance_bits: row.distance.to_bits(),
                        }
                    })
                    .collect::<Vec<_>>();
                let mut samples = Vec::with_capacity(LIMITS.len());
                for limit in ranked_row_limits {
                    let retained = &ranked[..usize::try_from(*limit).unwrap().min(ranked.len())];
                    let mut selected_pages = select_v26_ranked_pages(
                        retained,
                        &pages_by_source,
                        usize::try_from(page_budget).unwrap(),
                    )?;
                    selected_pages.sort_unstable();
                    let hits =
                        v26_layout_hits(&truth.ground_truth_page_assignments, &selected_pages);
                    samples.push(V26ExactGlobalSample {
                        query_ordinal: query.query_ordinal,
                        ranked_row_limit: *limit,
                        candidate_rows,
                        selected_pages,
                        first_ten_ranked_rows: first_ten_ranked_rows.clone(),
                        hits,
                        oracle_hits,
                        recall_ppm: v26_ppm(u64::from(hits), 10)?,
                        oracle_attainment_ppm: v26_ppm(u64::from(hits), u64::from(oracle_hits))?,
                    });
                }
                Ok(samples)
            },
        )
        .collect::<Result<Vec<_>>>()?;
    Ok(per_query_samples.into_iter().flatten().collect())
}

pub fn evaluate_v26_tree_router(
    primary: &V26Tree,
    replica: &V26Tree,
    queries: &[V26ExternalQuery],
    truths: &[V26QueryTruth],
    page_budget: usize,
) -> Result<(Vec<V26TreeRouterSample>, V26TreeRouterResult)> {
    if queries.len() != 512 || truths.len() != queries.len() || page_budget != 8 {
        return Err(invalid("V26 tree router request differs"));
    }
    let samples = queries
        .par_iter()
        .zip(truths.par_iter())
        .enumerate()
        .map(|(query_index, (query, truth))| {
            if usize::try_from(query.query_ordinal).ok() != Some(query_index)
                || truth.query_ordinal != query.query_ordinal
                || truth.neighbor_source_ordinals.len() != 10
                || truth.ground_truth_page_assignments.len() != 10
            {
                return Err(invalid("V26 tree router query authority differs"));
            }
            let selected_pages = route_v26_pages(primary, replica, &query.vector, page_budget)?;
            let oracle_pages =
                exact_v26_layout_oracle_pages(&truth.ground_truth_page_assignments, page_budget)?;
            let hits = v26_layout_hits(&truth.ground_truth_page_assignments, &selected_pages);
            let oracle_hits = v26_layout_hits(&truth.ground_truth_page_assignments, &oracle_pages);
            Ok(V26TreeRouterSample {
                query_ordinal: query.query_ordinal,
                selected_pages,
                hits,
                oracle_hits,
                recall_ppm: v26_ppm(u64::from(hits), 10)?,
                oracle_attainment_ppm: v26_ppm(u64::from(hits), u64::from(oracle_hits))?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let total_hits = samples.iter().try_fold(0_u64, |sum, sample| {
        sum.checked_add(u64::from(sample.hits))
            .ok_or_else(|| invalid("V26 tree router metric overflows"))
    })?;
    let total_oracle_hits = samples.iter().try_fold(0_u64, |sum, sample| {
        sum.checked_add(u64::from(sample.oracle_hits))
            .ok_or_else(|| invalid("V26 tree router metric overflows"))
    })?;
    let aggregate_recall_ppm = v26_ppm(total_hits, queries.len() as u64 * 10)?;
    let minimum_query_recall_ppm = samples
        .iter()
        .map(|sample| sample.recall_ppm)
        .min()
        .ok_or_else(|| invalid("V26 tree router samples are absent"))?;
    let oracle_attainment_ppm = v26_ppm(total_hits, total_oracle_hits)?;
    let passed = aggregate_recall_ppm >= 975_000
        && minimum_query_recall_ppm >= 800_000
        && oracle_attainment_ppm >= 995_000;
    let result = V26TreeRouterResult {
        schema: "borsuk-v26-tree-router-result-v1".to_owned(),
        query_count: u32::try_from(queries.len())
            .map_err(|_| invalid("V26 tree router query count overflows"))?,
        aggregate_recall_ppm,
        minimum_query_recall_ppm,
        oracle_attainment_ppm,
        disposition: if passed {
            V26Disposition::BoundedLayoutCandidate
        } else {
            V26Disposition::TreeRouterRejected
        },
        page_body_reads: 0,
        claim_eligible: false,
    };
    Ok((samples, result))
}

pub fn diagnose_v26_tree_router_candidate_widths(
    primary: &V26Tree,
    replica: &V26Tree,
    queries: &[V26ExternalQuery],
    truths: &[V26QueryTruth],
) -> Result<(Vec<V26TreeRouterWidthSample>, Vec<V26TreeRouterWidthResult>)> {
    if queries.len() != 512 || truths.len() != queries.len() {
        return Err(invalid("V26 tree router diagnostic request differs"));
    }
    let first_ranking = rank_v26_tree_pages(primary, replica, &queries[0].vector)?;
    if first_ranking.len() < 10 {
        return Err(invalid("V26 tree router diagnostic inventory differs"));
    }
    let total_pages = first_ranking.len();
    let mut widths = [8_usize, 16, 32, 64, 128, 256, 512, 1_024, 2_048]
        .into_iter()
        .filter(|width| *width < total_pages)
        .collect::<Vec<_>>();
    widths.push(total_pages);

    let per_query = queries
        .par_iter()
        .zip(truths.par_iter())
        .enumerate()
        .map(|(query_index, (query, truth))| {
            if usize::try_from(query.query_ordinal).ok() != Some(query_index)
                || truth.query_ordinal != query.query_ordinal
                || truth.neighbor_source_ordinals.len() != 10
                || truth.ground_truth_page_assignments.len() != 10
            {
                return Err(invalid(
                    "V26 tree router diagnostic query authority differs",
                ));
            }
            let ranked = rank_v26_tree_pages(primary, replica, &query.vector)?;
            if ranked.len() != total_pages {
                return Err(invalid("V26 tree router diagnostic inventory differs"));
            }
            let oracle_pages =
                exact_v26_layout_oracle_pages(&truth.ground_truth_page_assignments, 10)?;
            let oracle_hits = v26_layout_hits(&truth.ground_truth_page_assignments, &oracle_pages);
            widths
                .iter()
                .map(|width| {
                    let mut candidates = ranked[..*width].to_vec();
                    candidates.sort_unstable();
                    let selected_pages = exact_v26_candidate_cover_pages(
                        &truth.ground_truth_page_assignments,
                        &candidates,
                        10,
                    )?;
                    let hits =
                        v26_layout_hits(&truth.ground_truth_page_assignments, &selected_pages);
                    Ok(V26TreeRouterWidthSample {
                        query_ordinal: query.query_ordinal,
                        candidate_page_limit: u32::try_from(*width)
                            .map_err(|_| invalid("V26 tree router diagnostic width overflows"))?,
                        selected_pages,
                        hits,
                        oracle_hits,
                        recall_ppm: v26_ppm(u64::from(hits), 10)?,
                        oracle_attainment_ppm: v26_ppm(u64::from(hits), u64::from(oracle_hits))?,
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    let samples = per_query.into_iter().flatten().collect::<Vec<_>>();
    let results = widths
        .iter()
        .map(|width| {
            let width = u32::try_from(*width)
                .map_err(|_| invalid("V26 tree router diagnostic width overflows"))?;
            let width_samples = samples
                .iter()
                .filter(|sample| sample.candidate_page_limit == width)
                .collect::<Vec<_>>();
            if width_samples.len() != queries.len() {
                return Err(invalid("V26 tree router diagnostic sample count differs"));
            }
            let total_hits = width_samples.iter().try_fold(0_u64, |sum, sample| {
                sum.checked_add(u64::from(sample.hits))
                    .ok_or_else(|| invalid("V26 tree router diagnostic metric overflows"))
            })?;
            let total_oracle_hits = width_samples.iter().try_fold(0_u64, |sum, sample| {
                sum.checked_add(u64::from(sample.oracle_hits))
                    .ok_or_else(|| invalid("V26 tree router diagnostic metric overflows"))
            })?;
            let aggregate_recall_ppm = v26_ppm(total_hits, queries.len() as u64 * 10)?;
            let minimum_query_recall_ppm = width_samples
                .iter()
                .map(|sample| sample.recall_ppm)
                .min()
                .ok_or_else(|| invalid("V26 tree router diagnostic samples are absent"))?;
            let oracle_attainment_ppm = v26_ppm(total_hits, total_oracle_hits)?;
            Ok(V26TreeRouterWidthResult {
                candidate_page_limit: width,
                aggregate_recall_ppm,
                minimum_query_recall_ppm,
                oracle_attainment_ppm,
                passed: aggregate_recall_ppm >= 975_000
                    && minimum_query_recall_ppm >= 800_000
                    && oracle_attainment_ppm >= 995_000,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((samples, results))
}

fn build_v26_page_centroids(
    primary: &V26Tree,
    replica: &V26Tree,
    rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
) -> Result<BTreeMap<u32, [f32; 96]>> {
    if rows.is_empty() || rows.len() != assignments.len() {
        return Err(invalid(
            "V26 centroid router construction inventory differs",
        ));
    }
    let mut probe = [0.0_f32; 96];
    probe[0] = 1.0;
    let page_inventory = rank_v26_tree_pages(primary, replica, &probe)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let mut sums = BTreeMap::<u32, ([f64; 96], u64)>::new();
    for (index, (row, assignment)) in rows.iter().zip(assignments).enumerate() {
        if usize::try_from(row.source_ordinal).ok() != Some(index)
            || assignment.source_ordinal != row.source_ordinal
            || assignment.primary_page == assignment.replica_page
            || !page_inventory.contains(&assignment.primary_page)
            || !page_inventory.contains(&assignment.replica_page)
        {
            return Err(invalid("V26 centroid router row binding differs"));
        }
        validate_v26_vector(&row.vector)?;
        for page in [assignment.primary_page, assignment.replica_page] {
            let (sum, count) = sums.entry(page).or_insert(([0.0; 96], 0));
            for (coordinate, value) in sum.iter_mut().zip(row.vector) {
                *coordinate += f64::from(value);
            }
            *count = count
                .checked_add(1)
                .ok_or_else(|| invalid("V26 centroid router page count overflows"))?;
        }
    }
    if sums.len() != page_inventory.len() {
        return Err(invalid("V26 centroid router page inventory differs"));
    }
    sums.into_iter()
        .map(|(page, (sum, count))| {
            if count == 0 {
                return Err(invalid("V26 centroid router page is empty"));
            }
            let norm = sum.iter().map(|value| value * value).sum::<f64>().sqrt();
            if !norm.is_finite() || norm == 0.0 {
                return Err(invalid("V26 centroid router page centroid differs"));
            }
            let centroid = std::array::from_fn(|dimension| (sum[dimension] / norm) as f32);
            validate_v26_vector(&centroid)?;
            Ok((page, centroid))
        })
        .collect()
}

pub(crate) const V26_PAGE_MODE_LADDER: [u32; 4] = [2, 4, 8, 16];
type V26PageModes = BTreeMap<u32, Vec<[f32; 96]>>;
type V26PageModeInventory = BTreeMap<u32, V26PageModes>;
pub(crate) const V26_PQ_WIDTH_LADDER: [usize; 4] = [8, 16, 24, 32];

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V26PqCodebook {
    width: usize,
    subspace_width: usize,
    centroids: Vec<Vec<f32>>,
}

impl V26PqCodebook {
    pub(crate) fn encode(&self, vector: &[f32; 96]) -> Result<Vec<u8>> {
        validate_v26_vector(vector)?;
        if !V26_PQ_WIDTH_LADDER.contains(&self.width)
            || self.subspace_width != 96 / self.width
            || self.centroids.len() != self.width
            || self
                .centroids
                .iter()
                .any(|centroids| centroids.len() != 256 * self.subspace_width)
        {
            return Err(invalid("V26 PQ codebook authority differs"));
        }
        (0..self.width)
            .map(|subspace| {
                let start = subspace * self.subspace_width;
                let query = &vector[start..start + self.subspace_width];
                let centroids = &self.centroids[subspace];
                let best = (0..256)
                    .map(|centroid| {
                        let values = &centroids
                            [centroid * self.subspace_width..(centroid + 1) * self.subspace_width];
                        let distance = query
                            .iter()
                            .zip(values)
                            .map(|(left, right)| {
                                let delta = left - right;
                                delta * delta
                            })
                            .sum::<f32>();
                        (distance, centroid)
                    })
                    .min_by(|left, right| {
                        left.0
                            .total_cmp(&right.0)
                            .then_with(|| left.1.cmp(&right.1))
                    })
                    .ok_or_else(|| invalid("V26 PQ centroid inventory differs"))?;
                if !best.0.is_finite() {
                    return Err(invalid("V26 PQ encoding distance differs"));
                }
                Ok(u8::try_from(best.1).unwrap())
            })
            .collect()
    }
}

pub(crate) fn fit_v26_pq_codebook(rows: &[[f32; 96]], width: usize) -> Result<V26PqCodebook> {
    if rows.len() < 256 || !V26_PQ_WIDTH_LADDER.contains(&width) || !96_usize.is_multiple_of(width)
    {
        return Err(invalid("V26 PQ fitting inventory differs"));
    }
    for row in rows {
        validate_v26_vector(row)?;
    }
    let subspace_width = 96 / width;
    let sample_count = rows.len().min(8_192);
    let sample = (0..sample_count)
        .map(|index| &rows[index * rows.len() / sample_count])
        .collect::<Vec<_>>();
    let centroids = (0..width)
        .into_par_iter()
        .map(|subspace| {
            let start = subspace * subspace_width;
            let mut centroids = (0..256)
                .flat_map(|centroid| {
                    sample[centroid * sample.len() / 256][start..start + subspace_width]
                        .iter()
                        .copied()
                })
                .collect::<Vec<_>>();
            for _ in 0..4 {
                let mut sums = vec![0.0_f64; 256 * subspace_width];
                let mut counts = vec![0_u32; 256];
                for row in &sample {
                    let values = &row[start..start + subspace_width];
                    let nearest = (0..256)
                        .map(|centroid| {
                            let center = &centroids
                                [centroid * subspace_width..(centroid + 1) * subspace_width];
                            let distance = values
                                .iter()
                                .zip(center)
                                .map(|(left, right)| {
                                    let delta = left - right;
                                    delta * delta
                                })
                                .sum::<f32>();
                            (distance, centroid)
                        })
                        .min_by(|left, right| {
                            left.0
                                .total_cmp(&right.0)
                                .then_with(|| left.1.cmp(&right.1))
                        })
                        .unwrap()
                        .1;
                    for dimension in 0..subspace_width {
                        sums[nearest * subspace_width + dimension] += f64::from(values[dimension]);
                    }
                    counts[nearest] += 1;
                }
                for centroid in 0..256 {
                    if counts[centroid] == 0 {
                        continue;
                    }
                    for dimension in 0..subspace_width {
                        centroids[centroid * subspace_width + dimension] =
                            (sums[centroid * subspace_width + dimension]
                                / f64::from(counts[centroid])) as f32;
                    }
                }
            }
            centroids
        })
        .collect::<Vec<_>>();
    Ok(V26PqCodebook {
        width,
        subspace_width,
        centroids,
    })
}

pub(crate) fn projected_v26_pq_resident_bytes(
    rows: u64,
    page_capacity: u32,
    width: usize,
) -> Result<u64> {
    if rows == 0
        || page_capacity == 0
        || !V26_PQ_WIDTH_LADDER.contains(&width)
        || !96_usize.is_multiple_of(width)
    {
        return Err(invalid("V26 PQ projection request differs"));
    }
    let occurrence_width = u64::try_from(width)
        .map_err(|_| invalid("V26 PQ width overflows"))?
        .checked_add(4)
        .ok_or_else(|| invalid("V26 PQ occurrence width overflows"))?;
    let occurrences = rows
        .checked_mul(2)
        .and_then(|value| value.checked_mul(occurrence_width))
        .ok_or_else(|| invalid("V26 PQ occurrence projection overflows"))?;
    let page_count = rows
        .div_ceil(u64::from(page_capacity))
        .checked_mul(2)
        .ok_or_else(|| invalid("V26 PQ page projection overflows"))?;
    let offsets = page_count
        .checked_add(1)
        .and_then(|value| value.checked_mul(8))
        .ok_or_else(|| invalid("V26 PQ offset projection overflows"))?;
    occurrences
        .checked_add(offsets)
        .and_then(|value| value.checked_add(8 * 256 * 12 * 4))
        .and_then(|value| value.checked_add(512 * 1_024 * 1_024))
        .ok_or_else(|| invalid("V26 PQ resident projection overflows"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct V26PqOccurrence {
    code: Vec<u8>,
    partner_page: u32,
}

#[derive(Debug, Clone, Copy)]
struct V26PqRankedOccurrence {
    pages: [u32; 2],
    distance: f32,
    occurrence_ordinal: u32,
}

impl PartialEq for V26PqRankedOccurrence {
    fn eq(&self, other: &Self) -> bool {
        self.pages == other.pages
            && self.distance.to_bits() == other.distance.to_bits()
            && self.occurrence_ordinal == other.occurrence_ordinal
    }
}

impl Eq for V26PqRankedOccurrence {}

impl PartialOrd for V26PqRankedOccurrence {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for V26PqRankedOccurrence {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.pages.cmp(&other.pages))
            .then_with(|| self.occurrence_ordinal.cmp(&other.occurrence_ordinal))
    }
}

fn prepare_v26_pq_tables(codebook: &V26PqCodebook, query: &[f32; 96]) -> Result<Vec<[f32; 256]>> {
    validate_v26_vector(query)?;
    if codebook.centroids.len() != codebook.width {
        return Err(invalid("V26 PQ codebook authority differs"));
    }
    let tables = (0..codebook.width)
        .map(|subspace| {
            let start = subspace * codebook.subspace_width;
            let query = &query[start..start + codebook.subspace_width];
            std::array::from_fn(|centroid| {
                query
                    .iter()
                    .zip(
                        &codebook.centroids[subspace][centroid * codebook.subspace_width
                            ..(centroid + 1) * codebook.subspace_width],
                    )
                    .map(|(left, right)| {
                        let delta = left - right;
                        delta * delta
                    })
                    .sum::<f32>()
            })
        })
        .collect::<Vec<_>>();
    if tables
        .iter()
        .flatten()
        .any(|distance| !distance.is_finite())
    {
        return Err(invalid("V26 PQ query table differs"));
    }
    Ok(tables)
}

fn build_v26_pq_page_occurrences(
    rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
    codebook: &V26PqCodebook,
) -> Result<BTreeMap<u32, Vec<V26PqOccurrence>>> {
    if rows.is_empty() || rows.len() != assignments.len() {
        return Err(invalid("V26 PQ materialization request differs"));
    }
    let mut pages = BTreeMap::<u32, Vec<V26PqOccurrence>>::new();
    for (index, (row, assignment)) in rows.iter().zip(assignments).enumerate() {
        if usize::try_from(row.source_ordinal).ok() != Some(index)
            || assignment.source_ordinal != row.source_ordinal
            || assignment.primary_page == assignment.replica_page
        {
            return Err(invalid("V26 PQ materialization binding differs"));
        }
        let code = codebook.encode(&row.vector)?;
        pages
            .entry(assignment.primary_page)
            .or_default()
            .push(V26PqOccurrence {
                code: code.clone(),
                partner_page: assignment.replica_page,
            });
        pages
            .entry(assignment.replica_page)
            .or_default()
            .push(V26PqOccurrence {
                code,
                partner_page: assignment.primary_page,
            });
    }
    Ok(pages)
}

fn rank_v26_pq_occurrences(
    pages: &BTreeMap<u32, Vec<V26PqOccurrence>>,
    candidate_pages: &[u32],
    tables: &[[f32; 256]],
) -> Result<Vec<V26PqRankedOccurrence>> {
    if candidate_pages.is_empty()
        || candidate_pages.windows(2).any(|pair| pair[0] >= pair[1])
        || !V26_PQ_WIDTH_LADDER.contains(&tables.len())
    {
        return Err(invalid("V26 PQ candidate scan request differs"));
    }
    let candidates = candidate_pages.iter().copied().collect::<BTreeSet<_>>();
    let mut heap = BinaryHeap::with_capacity(10);
    let mut occurrence_ordinal = 0_u32;
    for page in candidate_pages {
        for occurrence in pages
            .get(page)
            .ok_or_else(|| invalid("V26 PQ candidate page is absent"))?
        {
            if occurrence.partner_page == *page || occurrence.code.len() != tables.len() {
                return Err(invalid("V26 PQ occurrence binding differs"));
            }
            if candidates.contains(&occurrence.partner_page) && *page > occurrence.partner_page {
                continue;
            }
            let distance = occurrence
                .code
                .iter()
                .enumerate()
                .map(|(subspace, code)| tables[subspace][usize::from(*code)])
                .sum::<f32>();
            let ranked = V26PqRankedOccurrence {
                pages: [
                    (*page).min(occurrence.partner_page),
                    (*page).max(occurrence.partner_page),
                ],
                distance,
                occurrence_ordinal,
            };
            occurrence_ordinal = occurrence_ordinal
                .checked_add(1)
                .ok_or_else(|| invalid("V26 PQ occurrence ordinal overflows"))?;
            if heap.len() < 10 {
                heap.push(ranked);
            } else if ranked < *heap.peek().unwrap() {
                heap.pop();
                heap.push(ranked);
            }
        }
    }
    if heap.len() != 10 {
        return Err(invalid("V26 PQ candidate row inventory differs"));
    }
    let mut ranked = heap.into_vec();
    ranked.sort();
    Ok(ranked)
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V26PqWidthEvaluation {
    pub(crate) code_width: usize,
    pub(crate) projected_resident_bytes_100m: u64,
    pub(crate) samples: Vec<V26TreeRouterSample>,
    pub(crate) result: V26TreeRouterResult,
}

pub(crate) fn evaluate_v26_pq_width_ladder(
    primary: &V26Tree,
    replica: &V26Tree,
    rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
    queries: &[V26ExternalQuery],
    truths: &[V26QueryTruth],
    candidate_page_limit: usize,
) -> Result<Vec<V26PqWidthEvaluation>> {
    if queries.len() != 512 || truths.len() != 512 || rows.len() != assignments.len() {
        return Err(invalid("V26 PQ width ladder request differs"));
    }
    let vectors = rows.iter().map(|row| row.vector).collect::<Vec<_>>();
    V26_PQ_WIDTH_LADDER
        .into_iter()
        .map(|width| {
            let codebook = fit_v26_pq_codebook(&vectors, width)?;
            let pages = build_v26_pq_page_occurrences(rows, assignments, &codebook)?;
            let samples = queries
                .par_iter()
                .zip(truths.par_iter())
                .enumerate()
                .map(|(query_index, (query, truth))| {
                    if usize::try_from(query.query_ordinal).ok() != Some(query_index)
                        || truth.query_ordinal != query.query_ordinal
                        || truth.neighbor_source_ordinals.len() != 10
                        || truth.ground_truth_page_assignments.len() != 10
                    {
                        return Err(invalid("V26 PQ width query authority differs"));
                    }
                    let ranked_candidates = tree::rank_v26_tree_page_prefix(
                        primary,
                        replica,
                        &query.vector,
                        candidate_page_limit,
                    )?;
                    let mut candidate_pages = ranked_candidates.clone();
                    candidate_pages.sort_unstable();
                    let tables = prepare_v26_pq_tables(&codebook, &query.vector)?;
                    let ranked = rank_v26_pq_occurrences(&pages, &candidate_pages, &tables)?;
                    let ranked_assignments = ranked
                        .iter()
                        .map(|row| row.pages.to_vec())
                        .collect::<Vec<_>>();
                    let mut selected_pages = exact_v26_layout_oracle_pages(&ranked_assignments, 8)?;
                    for page in ranked_candidates {
                        if selected_pages.len() == 8 {
                            break;
                        }
                        if !selected_pages.contains(&page) {
                            selected_pages.push(page);
                        }
                    }
                    if selected_pages.len() != 8 {
                        return Err(invalid("V26 PQ width selected page inventory differs"));
                    }
                    selected_pages.sort_unstable();
                    let oracle_pages =
                        exact_v26_layout_oracle_pages(&truth.ground_truth_page_assignments, 8)?;
                    let hits =
                        v26_layout_hits(&truth.ground_truth_page_assignments, &selected_pages);
                    let oracle_hits =
                        v26_layout_hits(&truth.ground_truth_page_assignments, &oracle_pages);
                    Ok(V26TreeRouterSample {
                        query_ordinal: query.query_ordinal,
                        selected_pages,
                        hits,
                        oracle_hits,
                        recall_ppm: v26_ppm(u64::from(hits), 10)?,
                        oracle_attainment_ppm: v26_ppm(u64::from(hits), u64::from(oracle_hits))?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let total_hits = samples.iter().try_fold(0_u64, |sum, sample| {
                sum.checked_add(u64::from(sample.hits))
                    .ok_or_else(|| invalid("V26 PQ width metric overflows"))
            })?;
            let total_oracle_hits = samples.iter().try_fold(0_u64, |sum, sample| {
                sum.checked_add(u64::from(sample.oracle_hits))
                    .ok_or_else(|| invalid("V26 PQ width metric overflows"))
            })?;
            let aggregate_recall_ppm = v26_ppm(total_hits, queries.len() as u64 * 10)?;
            let minimum_query_recall_ppm = samples
                .iter()
                .map(|sample| sample.recall_ppm)
                .min()
                .ok_or_else(|| invalid("V26 PQ width samples are absent"))?;
            let oracle_attainment_ppm = v26_ppm(total_hits, total_oracle_hits)?;
            let passed = aggregate_recall_ppm >= 975_000
                && minimum_query_recall_ppm >= 800_000
                && oracle_attainment_ppm >= 995_000;
            Ok(V26PqWidthEvaluation {
                code_width: width,
                projected_resident_bytes_100m: projected_v26_pq_resident_bytes(
                    100_000_000,
                    2_816,
                    width,
                )?,
                samples,
                result: V26TreeRouterResult {
                    schema: "borsuk-v26-pq-width-candidate-cover-result-v1".to_owned(),
                    query_count: 512,
                    aggregate_recall_ppm,
                    minimum_query_recall_ppm,
                    oracle_attainment_ppm,
                    disposition: if passed {
                        V26Disposition::BoundedLayoutCandidate
                    } else {
                        V26Disposition::RankReducerRejected
                    },
                    page_body_reads: 0,
                    claim_eligible: false,
                },
            })
        })
        .collect()
}

const V26_PQ16_RERANK_LADDER: [usize; 5] = [10, 32, 128, 512, 2_048];

fn projected_v26_pq16_rerank_resident_bytes(rows: u64, page_capacity: u32) -> Result<u64> {
    if rows == 0 || page_capacity == 0 {
        return Err(invalid("V26 PQ16 rerank projection request differs"));
    }
    let codes_and_postings = rows
        .checked_mul(16 + 2 * 4)
        .ok_or_else(|| invalid("V26 PQ16 rerank projection overflows"))?;
    let offsets = rows
        .div_ceil(u64::from(page_capacity))
        .checked_mul(2)
        .and_then(|pages| pages.checked_add(1))
        .and_then(|offsets| offsets.checked_mul(8))
        .ok_or_else(|| invalid("V26 PQ16 rerank offset projection overflows"))?;
    codes_and_postings
        .checked_add(offsets)
        .and_then(|value| value.checked_add(16 * 256 * 6 * 4))
        .and_then(|value| value.checked_add(512 * 1_024 * 1_024))
        .ok_or_else(|| invalid("V26 PQ16 rerank projection overflows"))
}

pub(crate) fn v26_squared_l2(left: &[f32; 96], right: &[f32; 96]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let delta = left - right;
            delta * delta
        })
        .sum()
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct V26PqRankedRow {
    pub(crate) source_ordinal: u64,
    pub(crate) distance: f32,
}

impl PartialEq for V26PqRankedRow {
    fn eq(&self, other: &Self) -> bool {
        self.source_ordinal == other.source_ordinal
            && self.distance.to_bits() == other.distance.to_bits()
    }
}

impl Eq for V26PqRankedRow {}

impl PartialOrd for V26PqRankedRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for V26PqRankedRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.source_ordinal.cmp(&other.source_ordinal))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V26Pq16RerankEvaluation {
    pub(crate) ranked_row_limit: usize,
    pub(crate) projected_resident_bytes_100m: u64,
    pub(crate) samples: Vec<V26TreeRouterSample>,
    pub(crate) result: V26TreeRouterResult,
}

#[cfg(test)]
fn rank_v26_pq16_candidate_rows(
    codes: &[Vec<u8>],
    assignments: &[V26RowPages],
    candidate_pages: &[u32],
    tables: &[[f32; 256]],
) -> Result<Vec<V26PqRankedRow>> {
    if codes.len() != assignments.len()
        || candidate_pages.is_empty()
        || candidate_pages.windows(2).any(|pair| pair[0] >= pair[1])
        || tables.len() != 16
    {
        return Err(invalid("V26 PQ16 rerank request differs"));
    }
    let candidates = candidate_pages.iter().copied().collect::<BTreeSet<_>>();
    let mut heap = BinaryHeap::with_capacity(V26_PQ16_RERANK_LADDER[4]);
    for (index, (code, assignment)) in codes.iter().zip(assignments).enumerate() {
        if usize::try_from(assignment.source_ordinal).ok() != Some(index) || code.len() != 16 {
            return Err(invalid("V26 PQ16 rerank binding differs"));
        }
        if !candidates.contains(&assignment.primary_page)
            && !candidates.contains(&assignment.replica_page)
        {
            continue;
        }
        let distance = code
            .iter()
            .enumerate()
            .map(|(subspace, code)| tables[subspace][usize::from(*code)])
            .sum::<f32>();
        if !distance.is_finite() {
            return Err(invalid("V26 PQ16 rerank distance differs"));
        }
        let ranked = V26PqRankedRow {
            source_ordinal: assignment.source_ordinal,
            distance,
        };
        if heap.len() < V26_PQ16_RERANK_LADDER[4] {
            heap.push(ranked);
        } else if ranked < *heap.peek().unwrap() {
            heap.pop();
            heap.push(ranked);
        }
    }
    if heap.len() != V26_PQ16_RERANK_LADDER[4] {
        return Err(invalid("V26 PQ16 candidate inventory differs"));
    }
    let mut ranked = heap.into_vec();
    ranked.sort();
    Ok(ranked)
}

fn summarize_v26_pq16_samples(samples: &[V26TreeRouterSample]) -> Result<V26TreeRouterResult> {
    if samples.len() != 512 {
        return Err(invalid("V26 PQ16 sample inventory differs"));
    }
    let total_hits = samples.iter().try_fold(0_u64, |sum, sample| {
        sum.checked_add(u64::from(sample.hits))
            .ok_or_else(|| invalid("V26 PQ16 metric overflows"))
    })?;
    let total_oracle_hits = samples.iter().try_fold(0_u64, |sum, sample| {
        sum.checked_add(u64::from(sample.oracle_hits))
            .ok_or_else(|| invalid("V26 PQ16 metric overflows"))
    })?;
    let aggregate_recall_ppm = v26_ppm(total_hits, 5_120)?;
    let minimum_query_recall_ppm = samples
        .iter()
        .map(|sample| sample.recall_ppm)
        .min()
        .ok_or_else(|| invalid("V26 PQ16 samples are absent"))?;
    let oracle_attainment_ppm = v26_ppm(total_hits, total_oracle_hits)?;
    let passed = aggregate_recall_ppm >= 975_000
        && minimum_query_recall_ppm >= 800_000
        && oracle_attainment_ppm >= 995_000;
    Ok(V26TreeRouterResult {
        schema: "borsuk-v26-pq16-exact-rerank-result-v1".to_owned(),
        query_count: 512,
        aggregate_recall_ppm,
        minimum_query_recall_ppm,
        oracle_attainment_ppm,
        disposition: if passed {
            V26Disposition::BoundedLayoutCandidate
        } else {
            V26Disposition::RankReducerRejected
        },
        page_body_reads: 0,
        claim_eligible: false,
    })
}

pub(crate) fn evaluate_v26_pq16_exact_rerank_ladder(
    primary: &V26Tree,
    replica: &V26Tree,
    rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
    queries: &[V26ExternalQuery],
    truths: &[V26QueryTruth],
    candidate_page_limit: usize,
) -> Result<Vec<V26Pq16RerankEvaluation>> {
    if queries.len() != 512 || truths.len() != 512 || rows.len() != assignments.len() {
        return Err(invalid("V26 PQ16 ladder authority differs"));
    }
    let index = build_v26_pq16_packed_index(rows, assignments)?;
    let per_query = queries
        .par_iter()
        .zip(truths.par_iter())
        .enumerate()
        .map(|(query_index, (query, truth))| {
            if usize::try_from(query.query_ordinal).ok() != Some(query_index)
                || truth.query_ordinal != query.query_ordinal
                || truth.neighbor_source_ordinals.len() != 10
                || truth.ground_truth_page_assignments.len() != 10
            {
                return Err(invalid("V26 PQ16 query authority differs"));
            }
            let ranked_candidates = tree::rank_v26_tree_page_prefix(
                primary,
                replica,
                &query.vector,
                candidate_page_limit,
            )?;
            let mut candidate_pages = ranked_candidates.clone();
            candidate_pages.sort_unstable();
            let ranked = rank_v26_pq16_packed_candidates(
                &index,
                &candidate_pages,
                &query.vector,
                V26_PQ16_RERANK_LADDER[4],
            )?;
            V26_PQ16_RERANK_LADDER
                .into_iter()
                .map(|ranked_row_limit| {
                    let mut exact = ranked[..ranked_row_limit]
                        .iter()
                        .map(|candidate| {
                            let row = &rows[usize::try_from(candidate.source_ordinal).unwrap()];
                            let distance = v26_squared_l2(&row.vector, &query.vector);
                            V26PqRankedRow {
                                source_ordinal: candidate.source_ordinal,
                                distance,
                            }
                        })
                        .collect::<Vec<_>>();
                    if exact
                        .iter()
                        .any(|candidate| !candidate.distance.is_finite())
                    {
                        return Err(invalid("V26 PQ16 exact distance differs"));
                    }
                    exact.sort();
                    let ranked_assignments = exact[..10]
                        .iter()
                        .map(|candidate| {
                            let assignment =
                                assignments[usize::try_from(candidate.source_ordinal).unwrap()];
                            vec![assignment.primary_page, assignment.replica_page]
                        })
                        .collect::<Vec<_>>();
                    let mut selected_pages = exact_v26_layout_oracle_pages(
                        &ranked_assignments,
                        V26_SERVING_PAGE_BUDGET,
                    )?;
                    for page in &ranked_candidates {
                        if selected_pages.len() == V26_SERVING_PAGE_BUDGET {
                            break;
                        }
                        if !selected_pages.contains(page) {
                            selected_pages.push(*page);
                        }
                    }
                    if selected_pages.len() != V26_SERVING_PAGE_BUDGET {
                        return Err(invalid("V26 PQ16 selected page inventory differs"));
                    }
                    selected_pages.sort_unstable();
                    let oracle_pages = exact_v26_layout_oracle_pages(
                        &truth.ground_truth_page_assignments,
                        V26_SERVING_PAGE_BUDGET,
                    )?;
                    let hits =
                        v26_layout_hits(&truth.ground_truth_page_assignments, &selected_pages);
                    let oracle_hits =
                        v26_layout_hits(&truth.ground_truth_page_assignments, &oracle_pages);
                    Ok(V26TreeRouterSample {
                        query_ordinal: query.query_ordinal,
                        selected_pages,
                        hits,
                        oracle_hits,
                        recall_ppm: v26_ppm(u64::from(hits), 10)?,
                        oracle_attainment_ppm: v26_ppm(u64::from(hits), u64::from(oracle_hits))?,
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    V26_PQ16_RERANK_LADDER
        .into_iter()
        .enumerate()
        .map(|(arm_index, ranked_row_limit)| {
            let samples = per_query
                .iter()
                .map(|query| query[arm_index].clone())
                .collect::<Vec<_>>();
            Ok(V26Pq16RerankEvaluation {
                ranked_row_limit,
                projected_resident_bytes_100m: projected_v26_pq16_rerank_resident_bytes(
                    100_000_000,
                    2_816,
                )?,
                result: summarize_v26_pq16_samples(&samples)?,
                samples,
            })
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq)]
pub struct V26PackedPq16Index {
    codebook: V26PqCodebook,
    pub(crate) codes: Vec<u8>,
    pub(crate) page_offsets: Vec<u64>,
    pub(crate) posting_rows: Vec<u32>,
    pub(crate) projected_resident_bytes_100m: u64,
}

const V26_SIMHASH_BITS: usize = 16;
const V26_SIMHASH_BUCKETS: usize = 1 << V26_SIMHASH_BITS;
const V26_SIMHASH_SEED: u64 = 0x5632_362d_5349_4d48;

/// Row-preserving PQ16 records grouped by a deterministic 16-bit SimHash.
#[derive(Debug, Clone, PartialEq)]
pub struct V26SimHashPq16MultiIndex {
    codebook: V26PqCodebook,
    pub(crate) page_count: u32,
    pub(crate) bucket_offsets: Vec<u64>,
    pub(crate) source_ordinals: Vec<u32>,
    pub(crate) codes: Vec<u8>,
    pub(crate) projected_resident_bytes_100m: u64,
}

const V26_DUAL_PQ_KEY_SUBSPACES: [[usize; 2]; 2] = [[0, 8], [4, 12]];

/// Source-order PQ16 codes plus two distance-aligned 16-bit ordinal indexes.
#[derive(Debug, Clone, PartialEq)]
pub struct V26DualPqKeyIndex {
    codebook: V26PqCodebook,
    pub(crate) bucket_offsets: [Vec<u64>; 2],
    pub(crate) source_ordinals: [Vec<u32>; 2],
    pub(crate) codes: Vec<u8>,
    pub(crate) projected_resident_bytes_100m: u64,
}

fn projected_v26_dual_pq_key_resident_bytes(rows: u64) -> Result<u64> {
    if rows == 0 || rows > u64::from(u32::MAX) {
        return Err(invalid("V26 dual PQ-key projection request differs"));
    }
    rows.checked_mul(16)
        .and_then(|bytes| bytes.checked_add(rows * 2 * 4))
        .and_then(|bytes| bytes.checked_add(2 * 65_537 * 8))
        .and_then(|bytes| bytes.checked_add(16 * 256 * 6 * 4))
        .and_then(|bytes| bytes.checked_add(512 * 1_024 * 1_024))
        .and_then(|bytes| bytes.checked_add(8))
        .ok_or_else(|| invalid("V26 dual PQ-key projection overflows"))
}

fn v26_dual_pq_key(code: &[u8; 16], plane: usize) -> u16 {
    let [low, high] = V26_DUAL_PQ_KEY_SUBSPACES[plane];
    u16::from_le_bytes([code[low], code[high]])
}

/// Builds two stable counting indexes directly from authenticated source-order PQ16 codes.
pub fn build_v26_dual_pq_key_index(packed: &V26PackedPq16Index) -> Result<V26DualPqKeyIndex> {
    const BUCKETS: usize = 65_536;
    let codes = packed.codes.as_chunks::<16>();
    if !codes.1.is_empty() || codes.0.is_empty() || codes.0.len() > u32::MAX as usize {
        return Err(invalid("V26 dual PQ-key build request differs"));
    }
    let mut bucket_offsets = [Vec::new(), Vec::new()];
    let mut source_ordinals = [Vec::new(), Vec::new()];
    for plane in 0..2 {
        let mut counts = vec![0_usize; BUCKETS];
        for code in codes.0 {
            let bucket = usize::from(v26_dual_pq_key(code, plane));
            counts[bucket] = counts[bucket]
                .checked_add(1)
                .ok_or_else(|| invalid("V26 dual PQ-key bucket count overflows"))?;
        }
        let mut offsets = Vec::with_capacity(BUCKETS + 1);
        offsets.push(0_u64);
        for count in counts {
            offsets.push(
                offsets
                    .last()
                    .copied()
                    .unwrap()
                    .checked_add(u64::try_from(count).unwrap())
                    .ok_or_else(|| invalid("V26 dual PQ-key bucket offset overflows"))?,
            );
        }
        let mut positions = offsets[..BUCKETS]
            .iter()
            .map(|offset| usize::try_from(*offset).unwrap())
            .collect::<Vec<_>>();
        let mut ordinals = vec![0_u32; codes.0.len()];
        for (source_ordinal, code) in codes.0.iter().enumerate() {
            let bucket = usize::from(v26_dual_pq_key(code, plane));
            let position = positions[bucket];
            positions[bucket] += 1;
            ordinals[position] = u32::try_from(source_ordinal).unwrap();
        }
        bucket_offsets[plane] = offsets;
        source_ordinals[plane] = ordinals;
    }
    Ok(V26DualPqKeyIndex {
        codebook: packed.codebook.clone(),
        bucket_offsets,
        source_ordinals,
        codes: packed.codes.clone(),
        projected_resident_bytes_100m: projected_v26_dual_pq_key_resident_bytes(100_000_000)?,
    })
}

/// Ranks the union of the nearest fixed PQ-key buckets by the complete PQ16 distance.
pub(crate) fn rank_v26_dual_pq_key_candidates(
    index: &V26DualPqKeyIndex,
    query: &[f32; 96],
    key_limit_per_plane: usize,
    ranked_row_limit: usize,
) -> Result<Vec<V26PqRankedRow>> {
    const BUCKETS: usize = 65_536;
    if key_limit_per_plane == 0
        || key_limit_per_plane > BUCKETS
        || ranked_row_limit == 0
        || ranked_row_limit > V26_PQ16_RERANK_LADDER[4]
        || !index.codes.len().is_multiple_of(16)
        || index.codes.len() / 16 < ranked_row_limit
        || index.bucket_offsets.iter().any(|offsets| {
            offsets.len() != BUCKETS + 1
                || offsets.first() != Some(&0)
                || offsets.last().copied() != Some(u64::try_from(index.codes.len() / 16).unwrap())
                || offsets.windows(2).any(|pair| pair[0] > pair[1])
        })
        || index
            .source_ordinals
            .iter()
            .any(|ordinals| ordinals.len() * 16 != index.codes.len())
    {
        return Err(invalid("V26 dual PQ-key query request differs"));
    }
    let tables = prepare_v26_pq_tables(&index.codebook, query)?;
    let mut candidates = Vec::new();
    for plane in 0..2 {
        let [low, high] = V26_DUAL_PQ_KEY_SUBSPACES[plane];
        let mut keys = (0_u32..u32::try_from(BUCKETS).unwrap())
            .map(|key| {
                let [low_code, high_code] = u16::try_from(key).unwrap().to_le_bytes();
                (
                    tables[low][usize::from(low_code)] + tables[high][usize::from(high_code)],
                    u16::try_from(key).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        keys.sort_unstable_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        for (_, key) in keys.into_iter().take(key_limit_per_plane) {
            let start = usize::try_from(index.bucket_offsets[plane][usize::from(key)]).unwrap();
            let end = usize::try_from(index.bucket_offsets[plane][usize::from(key) + 1]).unwrap();
            candidates.extend_from_slice(&index.source_ordinals[plane][start..end]);
        }
    }
    candidates.sort_unstable();
    candidates.dedup();
    if candidates.len() < ranked_row_limit {
        return Err(invalid("V26 dual PQ-key candidate inventory differs"));
    }
    let mut ranked = BinaryHeap::with_capacity(ranked_row_limit);
    for source_ordinal in candidates {
        let start = usize::try_from(source_ordinal).unwrap() * 16;
        let code: &[u8; 16] = index.codes[start..start + 16].try_into().unwrap();
        let distance = code
            .iter()
            .enumerate()
            .map(|(subspace, code)| tables[subspace][usize::from(*code)])
            .sum::<f32>();
        if !distance.is_finite() {
            return Err(invalid("V26 dual PQ-key distance differs"));
        }
        let value = V26PqRankedRow {
            source_ordinal: u64::from(source_ordinal),
            distance,
        };
        if ranked.len() < ranked_row_limit {
            ranked.push(value);
        } else if value < *ranked.peek().unwrap() {
            ranked.pop();
            ranked.push(value);
        }
    }
    let mut ranked = ranked.into_vec();
    ranked.sort();
    Ok(ranked)
}

fn v26_splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn v26_simhash_signature(vector: &[f32; 96]) -> Result<u16> {
    validate_v26_vector(vector)?;
    let mut signature = 0_u16;
    for bit in 0..V26_SIMHASH_BITS {
        let mut projection = 0.0_f64;
        for (dimension, coordinate) in vector.iter().enumerate() {
            let key = V26_SIMHASH_SEED
                ^ u64::try_from(bit).unwrap().rotate_left(17)
                ^ u64::try_from(dimension).unwrap();
            let sign = if v26_splitmix64(key) >> 63 == 0 {
                -1.0
            } else {
                1.0
            };
            projection += f64::from(*coordinate) * sign;
        }
        if projection >= 0.0 {
            signature |= 1 << bit;
        }
    }
    Ok(signature)
}

fn projected_v26_simhash_pq16_resident_bytes(rows: u64) -> Result<u64> {
    if rows == 0 || rows > u64::from(u32::MAX) {
        return Err(invalid("V26 SimHash PQ16 projection request differs"));
    }
    rows.checked_mul(4 + 16)
        .and_then(|bytes| bytes.checked_add(u64::try_from(V26_SIMHASH_BUCKETS + 1).unwrap() * 8))
        .and_then(|bytes| bytes.checked_add(16 * 256 * 6 * 4))
        .and_then(|bytes| bytes.checked_add(8))
        .and_then(|bytes| bytes.checked_add(512 * 1_024 * 1_024))
        .ok_or_else(|| invalid("V26 SimHash PQ16 projection overflows"))
}

/// Builds the deterministic SimHash/PQ16 multi-index from normalized rows.
pub fn build_v26_simhash_pq16_multi_index(
    packed: &V26PackedPq16Index,
    rows: &[V26ConstructionRow],
) -> Result<V26SimHashPq16MultiIndex> {
    const CODE_BYTES: usize = 16;
    if rows.is_empty()
        || rows.len() > u32::MAX as usize
        || packed.codes.len() != rows.len() * CODE_BYTES
        || packed.page_offsets.len() < 2
    {
        return Err(invalid("V26 SimHash PQ16 build request differs"));
    }
    let mut signatures = Vec::with_capacity(rows.len());
    for (row_index, row) in rows.iter().enumerate() {
        if usize::try_from(row.source_ordinal).ok() != Some(row_index) {
            return Err(invalid("V26 SimHash PQ16 row authority differs"));
        }
        signatures.push(v26_simhash_signature(&row.vector)?);
    }
    build_v26_simhash_pq16_multi_index_from_signatures(packed, &signatures)
}

pub(crate) fn build_v26_simhash_pq16_multi_index_from_signatures(
    packed: &V26PackedPq16Index,
    signatures: &[u16],
) -> Result<V26SimHashPq16MultiIndex> {
    const CODE_BYTES: usize = 16;
    if signatures.is_empty()
        || signatures.len() > u32::MAX as usize
        || packed.codes.len() != signatures.len() * CODE_BYTES
        || packed.page_offsets.len() < 2
    {
        return Err(invalid("V26 SimHash PQ16 signature inventory differs"));
    }
    let mut counts = vec![0_usize; V26_SIMHASH_BUCKETS];
    for signature in signatures {
        let bucket = usize::from(*signature);
        counts[bucket] = counts[bucket]
            .checked_add(1)
            .ok_or_else(|| invalid("V26 SimHash PQ16 bucket count overflows"))?;
    }
    let mut bucket_offsets = Vec::with_capacity(V26_SIMHASH_BUCKETS + 1);
    bucket_offsets.push(0_u64);
    for count in counts {
        let next = bucket_offsets
            .last()
            .copied()
            .unwrap()
            .checked_add(u64::try_from(count).unwrap())
            .ok_or_else(|| invalid("V26 SimHash PQ16 bucket offset overflows"))?;
        bucket_offsets.push(next);
    }
    let mut positions = bucket_offsets[..V26_SIMHASH_BUCKETS]
        .iter()
        .map(|offset| usize::try_from(*offset).unwrap())
        .collect::<Vec<_>>();
    let mut source_ordinals = vec![0_u32; signatures.len()];
    let mut codes = vec![0_u8; packed.codes.len()];
    for (row_index, signature) in signatures.iter().copied().enumerate() {
        let bucket = usize::from(signature);
        let position = positions[bucket];
        positions[bucket] += 1;
        source_ordinals[position] = u32::try_from(row_index).unwrap();
        codes[position * CODE_BYTES..(position + 1) * CODE_BYTES]
            .copy_from_slice(&packed.codes[row_index * CODE_BYTES..(row_index + 1) * CODE_BYTES]);
    }
    Ok(V26SimHashPq16MultiIndex {
        codebook: packed.codebook.clone(),
        page_count: u32::try_from(packed.page_offsets.len() - 1).unwrap(),
        bucket_offsets,
        source_ordinals,
        codes,
        projected_resident_bytes_100m: projected_v26_simhash_pq16_resident_bytes(100_000_000)?,
    })
}

pub(crate) fn rank_v26_simhash_pq16_candidates(
    index: &V26SimHashPq16MultiIndex,
    query: &[f32; 96],
    bucket_limit: usize,
    ranked_row_limit: usize,
) -> Result<Vec<V26PqRankedRow>> {
    const CODE_BYTES: usize = 16;
    if bucket_limit == 0
        || bucket_limit > V26_SIMHASH_BUCKETS
        || ranked_row_limit == 0
        || ranked_row_limit > V26_PQ16_RERANK_LADDER[4]
        || index.bucket_offsets.len() != V26_SIMHASH_BUCKETS + 1
        || index.source_ordinals.len() * CODE_BYTES != index.codes.len()
        || index.source_ordinals.len() < ranked_row_limit
        || index.bucket_offsets.first() != Some(&0)
        || index.bucket_offsets.last().copied()
            != Some(u64::try_from(index.source_ordinals.len()).unwrap())
        || index
            .bucket_offsets
            .windows(2)
            .any(|pair| pair[0] > pair[1])
    {
        return Err(invalid("V26 SimHash PQ16 query request differs"));
    }
    let buckets = rank_v26_simhash_buckets(query)?;
    let tables = prepare_v26_pq_tables(&index.codebook, query)?;
    let mut ranked = BinaryHeap::with_capacity(ranked_row_limit);
    for bucket in buckets.into_iter().take(bucket_limit) {
        let start = usize::try_from(index.bucket_offsets[usize::from(bucket)]).unwrap();
        let end = usize::try_from(index.bucket_offsets[usize::from(bucket) + 1]).unwrap();
        for position in start..end {
            let code = &index.codes[position * CODE_BYTES..(position + 1) * CODE_BYTES];
            let distance = code
                .iter()
                .enumerate()
                .map(|(subspace, code)| tables[subspace][usize::from(*code)])
                .sum::<f32>();
            if !distance.is_finite() {
                return Err(invalid("V26 SimHash PQ16 distance differs"));
            }
            let value = V26PqRankedRow {
                source_ordinal: u64::from(index.source_ordinals[position]),
                distance,
            };
            if ranked.len() < ranked_row_limit {
                ranked.push(value);
            } else if value < *ranked.peek().unwrap() {
                ranked.pop();
                ranked.push(value);
            }
        }
    }
    let mut ranked = ranked.into_vec();
    ranked.sort();
    if ranked.len() != ranked_row_limit {
        return Err(invalid("V26 SimHash PQ16 candidate inventory differs"));
    }
    Ok(ranked)
}

fn rank_v26_simhash_buckets(query: &[f32; 96]) -> Result<Vec<u16>> {
    let query_signature = v26_simhash_signature(query)?;
    let mut buckets = (0_u32..u32::try_from(V26_SIMHASH_BUCKETS).unwrap())
        .map(|bucket| {
            (
                (u32::from(query_signature) ^ bucket).count_ones(),
                u16::try_from(bucket).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    buckets.sort_unstable();
    Ok(buckets.into_iter().map(|(_, bucket)| bucket).collect())
}

pub(crate) fn v26_simhash_rows_scanned(
    index: &V26SimHashPq16MultiIndex,
    query: &[f32; 96],
    bucket_limit: usize,
) -> Result<u64> {
    if bucket_limit == 0
        || bucket_limit > V26_SIMHASH_BUCKETS
        || index.bucket_offsets.len() != V26_SIMHASH_BUCKETS + 1
    {
        return Err(invalid("V26 SimHash bucket scan request differs"));
    }
    rank_v26_simhash_buckets(query)?
        .into_iter()
        .take(bucket_limit)
        .try_fold(0_u64, |rows, bucket| {
            let start = index.bucket_offsets[usize::from(bucket)];
            let end = index.bucket_offsets[usize::from(bucket) + 1];
            rows.checked_add(end - start)
                .ok_or_else(|| invalid("V26 SimHash bucket scan overflows"))
        })
}

pub fn build_v26_pq16_packed_index(
    rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
) -> Result<V26PackedPq16Index> {
    if rows.is_empty() || rows.len() != assignments.len() || rows.len() > u32::MAX as usize {
        return Err(invalid("V26 packed PQ16 build request differs"));
    }
    let vectors = rows.iter().map(|row| row.vector).collect::<Vec<_>>();
    let codebook = fit_v26_pq_codebook(&vectors, 16)?;
    let mut codes = Vec::with_capacity(rows.len() * 16);
    let page_count = assignments
        .iter()
        .flat_map(|assignment| [assignment.primary_page, assignment.replica_page])
        .max()
        .and_then(|page| page.checked_add(1))
        .ok_or_else(|| invalid("V26 packed PQ16 page inventory differs"))?;
    let mut postings = vec![Vec::<u32>::new(); usize::try_from(page_count).unwrap()];
    for (index, (row, assignment)) in rows.iter().zip(assignments).enumerate() {
        if usize::try_from(row.source_ordinal).ok() != Some(index)
            || assignment.source_ordinal != row.source_ordinal
            || assignment.primary_page == assignment.replica_page
        {
            return Err(invalid("V26 packed PQ16 binding differs"));
        }
        codes.extend(codebook.encode(&row.vector)?);
        let row_id = u32::try_from(index).unwrap();
        postings[usize::try_from(assignment.primary_page).unwrap()].push(row_id);
        postings[usize::try_from(assignment.replica_page).unwrap()].push(row_id);
    }
    if postings.iter().any(Vec::is_empty) {
        return Err(invalid("V26 packed PQ16 page inventory differs"));
    }
    let mut page_offsets = Vec::with_capacity(postings.len() + 1);
    let mut posting_rows = Vec::with_capacity(rows.len() * 2);
    page_offsets.push(0);
    for posting in postings {
        posting_rows.extend(posting);
        page_offsets.push(u64::try_from(posting_rows.len()).unwrap());
    }
    Ok(V26PackedPq16Index {
        codebook,
        codes,
        page_offsets,
        posting_rows,
        projected_resident_bytes_100m: projected_v26_pq16_rerank_resident_bytes(
            100_000_000,
            2_816,
        )?,
    })
}

pub(crate) fn rank_v26_pq16_packed_candidates(
    index: &V26PackedPq16Index,
    candidate_pages: &[u32],
    query: &[f32; 96],
    ranked_row_limit: usize,
) -> Result<Vec<V26PqRankedRow>> {
    rank_v26_pq16_parallel_occurrence_candidates(index, candidate_pages, query, ranked_row_limit)
}

pub(crate) fn rank_v26_pq16_global_candidates(
    index: &V26PackedPq16Index,
    query: &[f32; 96],
    ranked_row_limit: usize,
) -> Result<Vec<V26PqRankedRow>> {
    const ROWS_PER_BLOCK: usize = 65_536;
    const CODE_BYTES: usize = 16;

    if ranked_row_limit == 0
        || ranked_row_limit > V26_PQ16_RERANK_LADDER[4]
        || !index.codes.len().is_multiple_of(CODE_BYTES)
        || index.codes.len() / CODE_BYTES < ranked_row_limit
    {
        return Err(invalid("V26 global PQ16 query request differs"));
    }
    let tables = prepare_v26_pq_tables(&index.codebook, query)?;
    let ranked = index
        .codes
        .par_chunks(ROWS_PER_BLOCK * CODE_BYTES)
        .enumerate()
        .fold(
            || Ok(BinaryHeap::with_capacity(ranked_row_limit)),
            |ranked, (block_index, block)| -> Result<BinaryHeap<V26PqRankedRow>> {
                let mut ranked = ranked?;
                let first_row = block_index
                    .checked_mul(ROWS_PER_BLOCK)
                    .ok_or_else(|| invalid("V26 global PQ16 row offset overflows"))?;
                for (row_offset, code) in block.as_chunks::<CODE_BYTES>().0.iter().enumerate() {
                    let distance = code
                        .iter()
                        .enumerate()
                        .map(|(subspace, code)| tables[subspace][usize::from(*code)])
                        .sum::<f32>();
                    if !distance.is_finite() {
                        return Err(invalid("V26 global PQ16 distance differs"));
                    }
                    let value = V26PqRankedRow {
                        source_ordinal: u64::try_from(first_row + row_offset).unwrap(),
                        distance,
                    };
                    if ranked.len() < ranked_row_limit {
                        ranked.push(value);
                    } else if value < *ranked.peek().unwrap() {
                        ranked.pop();
                        ranked.push(value);
                    }
                }
                Ok(ranked)
            },
        )
        .reduce(
            || Ok(BinaryHeap::with_capacity(ranked_row_limit)),
            |left, right| {
                let mut left = left?;
                for value in right? {
                    if left.len() < ranked_row_limit {
                        left.push(value);
                    } else if value < *left.peek().unwrap() {
                        left.pop();
                        left.push(value);
                    }
                }
                Ok(left)
            },
        )?;
    let mut ranked = ranked.into_vec();
    ranked.sort();
    if ranked.len() != ranked_row_limit {
        return Err(invalid("V26 global PQ16 candidate inventory differs"));
    }
    Ok(ranked)
}

#[cfg(test)]
pub(crate) fn rank_v26_pq16_linear_occurrence_candidates(
    index: &V26PackedPq16Index,
    candidate_pages: &[u32],
    query: &[f32; 96],
    ranked_row_limit: usize,
) -> Result<Vec<V26PqRankedRow>> {
    if candidate_pages.is_empty()
        || candidate_pages.windows(2).any(|pair| pair[0] >= pair[1])
        || ranked_row_limit == 0
        || ranked_row_limit > V26_PQ16_RERANK_LADDER[4]
        || !index.codes.len().is_multiple_of(16)
    {
        return Err(invalid("V26 packed PQ16 query request differs"));
    }
    let tables = prepare_v26_pq_tables(&index.codebook, query)?;
    let occurrence_limit = ranked_row_limit
        .checked_mul(2)
        .ok_or_else(|| invalid("V26 packed PQ16 occurrence limit overflows"))?;
    let mut ranked = BinaryHeap::with_capacity(occurrence_limit);
    for page in candidate_pages {
        let page = usize::try_from(*page).unwrap();
        let start = *index
            .page_offsets
            .get(page)
            .ok_or_else(|| invalid("V26 packed PQ16 candidate page differs"))?;
        let end = *index
            .page_offsets
            .get(page + 1)
            .ok_or_else(|| invalid("V26 packed PQ16 candidate page differs"))?;
        let start = usize::try_from(start).unwrap();
        let end = usize::try_from(end).unwrap();
        if start >= end {
            return Err(invalid("V26 packed PQ16 candidate page differs"));
        }
        for row_id in &index.posting_rows[start..end] {
            let code_start = usize::try_from(*row_id)
                .unwrap()
                .checked_mul(16)
                .ok_or_else(|| invalid("V26 packed PQ16 code offset overflows"))?;
            let code = index
                .codes
                .get(code_start..code_start + 16)
                .ok_or_else(|| invalid("V26 packed PQ16 row differs"))?;
            let distance = code
                .iter()
                .enumerate()
                .map(|(subspace, code)| tables[subspace][usize::from(*code)])
                .sum::<f32>();
            if !distance.is_finite() {
                return Err(invalid("V26 packed PQ16 distance differs"));
            }
            let value = V26PqRankedRow {
                source_ordinal: u64::from(*row_id),
                distance,
            };
            if ranked.len() < occurrence_limit {
                ranked.push(value);
            } else if value < *ranked.peek().unwrap() {
                ranked.pop();
                ranked.push(value);
            }
        }
    }
    let mut ranked = ranked.into_vec();
    ranked.sort();
    ranked.dedup_by_key(|row| row.source_ordinal);
    if ranked.len() < ranked_row_limit {
        return Err(invalid("V26 packed PQ16 candidate inventory differs"));
    }
    ranked.truncate(ranked_row_limit);
    Ok(ranked)
}

pub(crate) fn rank_v26_pq16_parallel_occurrence_candidates(
    index: &V26PackedPq16Index,
    candidate_pages: &[u32],
    query: &[f32; 96],
    ranked_row_limit: usize,
) -> Result<Vec<V26PqRankedRow>> {
    if candidate_pages.is_empty()
        || candidate_pages.windows(2).any(|pair| pair[0] >= pair[1])
        || ranked_row_limit == 0
        || ranked_row_limit > V26_PQ16_RERANK_LADDER[4]
        || !index.codes.len().is_multiple_of(16)
    {
        return Err(invalid("V26 packed PQ16 query request differs"));
    }
    let tables = prepare_v26_pq_tables(&index.codebook, query)?;
    let occurrence_limit = ranked_row_limit
        .checked_mul(2)
        .ok_or_else(|| invalid("V26 packed PQ16 occurrence limit overflows"))?;
    let page_rankings = candidate_pages
        .par_iter()
        .map(|page| -> Result<Vec<V26PqRankedRow>> {
            let page = usize::try_from(*page).unwrap();
            let start = *index
                .page_offsets
                .get(page)
                .ok_or_else(|| invalid("V26 packed PQ16 candidate page differs"))?;
            let end = *index
                .page_offsets
                .get(page + 1)
                .ok_or_else(|| invalid("V26 packed PQ16 candidate page differs"))?;
            let start = usize::try_from(start).unwrap();
            let end = usize::try_from(end).unwrap();
            if start >= end {
                return Err(invalid("V26 packed PQ16 candidate page differs"));
            }
            let mut ranked = BinaryHeap::with_capacity(occurrence_limit);
            for row_id in &index.posting_rows[start..end] {
                let code_start = usize::try_from(*row_id)
                    .unwrap()
                    .checked_mul(16)
                    .ok_or_else(|| invalid("V26 packed PQ16 code offset overflows"))?;
                let code = index
                    .codes
                    .get(code_start..code_start + 16)
                    .ok_or_else(|| invalid("V26 packed PQ16 row differs"))?;
                let distance = code
                    .iter()
                    .enumerate()
                    .map(|(subspace, code)| tables[subspace][usize::from(*code)])
                    .sum::<f32>();
                if !distance.is_finite() {
                    return Err(invalid("V26 packed PQ16 distance differs"));
                }
                let value = V26PqRankedRow {
                    source_ordinal: u64::from(*row_id),
                    distance,
                };
                if ranked.len() < occurrence_limit {
                    ranked.push(value);
                } else if value < *ranked.peek().unwrap() {
                    ranked.pop();
                    ranked.push(value);
                }
            }
            Ok(ranked.into_vec())
        })
        .collect::<Result<Vec<_>>>()?;
    let mut ranked = BinaryHeap::with_capacity(occurrence_limit);
    for value in page_rankings.into_iter().flatten() {
        if ranked.len() < occurrence_limit {
            ranked.push(value);
        } else if value < *ranked.peek().unwrap() {
            ranked.pop();
            ranked.push(value);
        }
    }
    let mut ranked = ranked.into_vec();
    ranked.sort();
    ranked.dedup_by_key(|row| row.source_ordinal);
    if ranked.len() < ranked_row_limit {
        return Err(invalid("V26 packed PQ16 candidate inventory differs"));
    }
    ranked.truncate(ranked_row_limit);
    Ok(ranked)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26Pq16ServingSelection {
    pub selected_pages: Vec<u32>,
    pub exact_rows_read: u32,
    pub cold_batches_read: u32,
    pub cold_read_workers: u32,
    pub page_body_reads: u32,
}

pub fn select_v26_pq16_packed_pages(
    index: &V26PackedPq16Index,
    candidate_pages: &[u32],
    query: &[f32; 96],
    cold_rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
) -> Result<V26Pq16ServingSelection> {
    if cold_rows.len() != assignments.len() || index.codes.len() != cold_rows.len() * 16 {
        return Err(invalid("V26 PQ16 serving authority differs"));
    }
    let approximate = rank_v26_pq16_packed_candidates(index, candidate_pages, query, 512)?;
    let mut exact = approximate
        .iter()
        .map(|candidate| {
            let index = usize::try_from(candidate.source_ordinal).unwrap();
            let row = cold_rows
                .get(index)
                .ok_or_else(|| invalid("V26 PQ16 cold row differs"))?;
            let assignment = assignments
                .get(index)
                .ok_or_else(|| invalid("V26 PQ16 assignment differs"))?;
            if row.source_ordinal != candidate.source_ordinal
                || assignment.source_ordinal != candidate.source_ordinal
            {
                return Err(invalid("V26 PQ16 cold-row binding differs"));
            }
            let distance = v26_squared_l2(&row.vector, query);
            if !distance.is_finite() {
                return Err(invalid("V26 PQ16 exact distance differs"));
            }
            Ok((
                V26PqRankedRow {
                    source_ordinal: candidate.source_ordinal,
                    distance,
                },
                [assignment.primary_page, assignment.replica_page],
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    exact.sort_by_key(|entry| entry.0);
    let ranked_assignments = exact[..10]
        .iter()
        .map(|(_, pages)| pages.to_vec())
        .collect::<Vec<_>>();
    let mut selected_pages =
        exact_v26_layout_oracle_pages(&ranked_assignments, V26_SERVING_PAGE_BUDGET)?;
    for page in candidate_pages {
        if selected_pages.len() == V26_SERVING_PAGE_BUDGET {
            break;
        }
        if !selected_pages.contains(page) {
            selected_pages.push(*page);
        }
    }
    if selected_pages.len() != V26_SERVING_PAGE_BUDGET {
        return Err(invalid("V26 PQ16 serving page inventory differs"));
    }
    selected_pages.sort_unstable();
    Ok(V26Pq16ServingSelection {
        selected_pages,
        exact_rows_read: 512,
        cold_batches_read: 0,
        cold_read_workers: 0,
        page_body_reads: 0,
    })
}

#[cfg(test)]
pub(crate) fn select_v26_pq16_global_packed_pages(
    index: &V26PackedPq16Index,
    query: &[f32; 96],
    cold_rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
    ranked_row_limit: usize,
) -> Result<V26Pq16ServingSelection> {
    if cold_rows.len() != assignments.len() || index.codes.len() != cold_rows.len() * 16 {
        return Err(invalid("V26 global PQ16 serving authority differs"));
    }
    let approximate = rank_v26_pq16_global_candidates(index, query, ranked_row_limit)?;
    let mut exact = approximate
        .iter()
        .map(|candidate| {
            let row_index = usize::try_from(candidate.source_ordinal).unwrap();
            let row = cold_rows
                .get(row_index)
                .ok_or_else(|| invalid("V26 global PQ16 cold row differs"))?;
            let assignment = assignments
                .get(row_index)
                .ok_or_else(|| invalid("V26 global PQ16 assignment differs"))?;
            if row.source_ordinal != candidate.source_ordinal
                || assignment.source_ordinal != candidate.source_ordinal
            {
                return Err(invalid("V26 global PQ16 cold-row binding differs"));
            }
            let distance = v26_squared_l2(&row.vector, query);
            if !distance.is_finite() {
                return Err(invalid("V26 global PQ16 exact distance differs"));
            }
            Ok((
                V26PqRankedRow {
                    source_ordinal: candidate.source_ordinal,
                    distance,
                },
                [assignment.primary_page, assignment.replica_page],
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    exact.sort_by_key(|entry| entry.0);
    let ranked_assignments = exact[..10]
        .iter()
        .map(|(_, pages)| pages.to_vec())
        .collect::<Vec<_>>();
    let mut selected_pages =
        exact_v26_layout_oracle_pages(&ranked_assignments, V26_SERVING_PAGE_BUDGET)?;
    for (_, pages) in &exact {
        for page in pages {
            if selected_pages.len() == V26_SERVING_PAGE_BUDGET {
                break;
            }
            if !selected_pages.contains(page) {
                selected_pages.push(*page);
            }
        }
    }
    if selected_pages.len() != V26_SERVING_PAGE_BUDGET {
        return Err(invalid("V26 global PQ16 serving page inventory differs"));
    }
    selected_pages.sort_unstable();
    Ok(V26Pq16ServingSelection {
        selected_pages,
        exact_rows_read: u32::try_from(ranked_row_limit)
            .map_err(|_| invalid("V26 global PQ16 ranked-row limit overflows"))?,
        cold_batches_read: 0,
        cold_read_workers: 0,
        page_body_reads: 0,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V26Pq8Occurrence {
    pub(crate) code: [u8; 8],
    pub(crate) partner_page: u32,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct V26Pq8RankedOccurrence {
    pub(crate) pages: [u32; 2],
    pub(crate) distance: f32,
    occurrence_ordinal: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V26Pq8Codebook {
    centroids: Vec<Vec<f32>>,
}

impl V26Pq8Codebook {
    pub(crate) fn encode(&self, vector: &[f32; 96]) -> Result<[u8; 8]> {
        validate_v26_vector(vector)?;
        if self.centroids.len() != 8
            || self
                .centroids
                .iter()
                .any(|centroids| centroids.len() != 256 * 12)
        {
            return Err(invalid("V26 PQ8 codebook authority differs"));
        }
        let mut code = [0_u8; 8];
        for (subspace, output) in code.iter_mut().enumerate() {
            let query = &vector[subspace * 12..(subspace + 1) * 12];
            let centroids = &self.centroids[subspace];
            let best = (0..256)
                .map(|centroid| {
                    let values = &centroids[centroid * 12..(centroid + 1) * 12];
                    let distance = query
                        .iter()
                        .zip(values)
                        .map(|(left, right)| {
                            let delta = left - right;
                            delta * delta
                        })
                        .sum::<f32>();
                    (distance, centroid)
                })
                .min_by(|left, right| {
                    left.0
                        .total_cmp(&right.0)
                        .then_with(|| left.1.cmp(&right.1))
                })
                .ok_or_else(|| invalid("V26 PQ8 centroid inventory differs"))?;
            if !best.0.is_finite() {
                return Err(invalid("V26 PQ8 encoding distance differs"));
            }
            *output = u8::try_from(best.1).unwrap();
        }
        Ok(code)
    }
}

pub(crate) fn fit_v26_pq8_codebook(rows: &[[f32; 96]]) -> Result<V26Pq8Codebook> {
    if rows.len() < 256 {
        return Err(invalid("V26 PQ8 fitting inventory differs"));
    }
    for row in rows {
        validate_v26_vector(row)?;
    }
    let sample_count = rows.len().min(8_192);
    let sample = (0..sample_count)
        .map(|index| &rows[index * rows.len() / sample_count])
        .collect::<Vec<_>>();
    let centroids = (0..8)
        .into_par_iter()
        .map(|subspace| {
            let start = subspace * 12;
            let mut centroids = (0..256)
                .flat_map(|centroid| {
                    sample[centroid * sample.len() / 256][start..start + 12]
                        .iter()
                        .copied()
                })
                .collect::<Vec<_>>();
            for _ in 0..4 {
                let mut sums = vec![[0.0_f64; 12]; 256];
                let mut counts = vec![0_u32; 256];
                for row in &sample {
                    let values = &row[start..start + 12];
                    let nearest = (0..256)
                        .map(|centroid| {
                            let center = &centroids[centroid * 12..(centroid + 1) * 12];
                            let distance = values
                                .iter()
                                .zip(center)
                                .map(|(left, right)| {
                                    let delta = left - right;
                                    delta * delta
                                })
                                .sum::<f32>();
                            (distance, centroid)
                        })
                        .min_by(|left, right| {
                            left.0
                                .total_cmp(&right.0)
                                .then_with(|| left.1.cmp(&right.1))
                        })
                        .unwrap()
                        .1;
                    for dimension in 0..12 {
                        sums[nearest][dimension] += f64::from(values[dimension]);
                    }
                    counts[nearest] += 1;
                }
                for centroid in 0..256 {
                    if counts[centroid] == 0 {
                        continue;
                    }
                    for dimension in 0..12 {
                        centroids[centroid * 12 + dimension] =
                            (sums[centroid][dimension] / f64::from(counts[centroid])) as f32;
                    }
                }
            }
            centroids
        })
        .collect::<Vec<_>>();
    Ok(V26Pq8Codebook { centroids })
}

pub(crate) fn prepare_v26_pq8_tables(
    codebook: &V26Pq8Codebook,
    query: &[f32; 96],
) -> Result<[[f32; 256]; 8]> {
    validate_v26_vector(query)?;
    if codebook.centroids.len() != 8
        || codebook
            .centroids
            .iter()
            .any(|centroids| centroids.len() != 256 * 12)
    {
        return Err(invalid("V26 PQ8 codebook authority differs"));
    }
    let tables = std::array::from_fn(|subspace| {
        let query = &query[subspace * 12..(subspace + 1) * 12];
        std::array::from_fn(|centroid| {
            query
                .iter()
                .zip(&codebook.centroids[subspace][centroid * 12..(centroid + 1) * 12])
                .map(|(left, right)| {
                    let delta = left - right;
                    delta * delta
                })
                .sum::<f32>()
        })
    });
    if tables
        .iter()
        .flatten()
        .any(|distance| !distance.is_finite())
    {
        return Err(invalid("V26 PQ8 query table differs"));
    }
    Ok(tables)
}

pub(crate) fn build_v26_pq8_page_occurrences(
    rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
    codebook: &V26Pq8Codebook,
) -> Result<BTreeMap<u32, Vec<V26Pq8Occurrence>>> {
    if rows.is_empty() || rows.len() != assignments.len() {
        return Err(invalid("V26 PQ8 materialization request differs"));
    }
    let mut pages = BTreeMap::<u32, Vec<V26Pq8Occurrence>>::new();
    for (index, (row, assignment)) in rows.iter().zip(assignments).enumerate() {
        if usize::try_from(row.source_ordinal).ok() != Some(index)
            || assignment.source_ordinal != row.source_ordinal
            || assignment.primary_page == assignment.replica_page
        {
            return Err(invalid("V26 PQ8 materialization binding differs"));
        }
        let code = codebook.encode(&row.vector)?;
        pages
            .entry(assignment.primary_page)
            .or_default()
            .push(V26Pq8Occurrence {
                code,
                partner_page: assignment.replica_page,
            });
        pages
            .entry(assignment.replica_page)
            .or_default()
            .push(V26Pq8Occurrence {
                code,
                partner_page: assignment.primary_page,
            });
    }
    Ok(pages)
}

pub(crate) fn evaluate_v26_pq8_candidate_cover(
    primary: &V26Tree,
    replica: &V26Tree,
    rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
    queries: &[V26ExternalQuery],
    truths: &[V26QueryTruth],
    candidate_page_limit: usize,
) -> Result<(Vec<V26TreeRouterSample>, V26TreeRouterResult)> {
    if queries.len() != 512 || truths.len() != queries.len() || rows.len() != assignments.len() {
        return Err(invalid("V26 PQ8 candidate cover request differs"));
    }
    let vectors = rows.iter().map(|row| row.vector).collect::<Vec<_>>();
    let codebook = fit_v26_pq8_codebook(&vectors)?;
    let pages = build_v26_pq8_page_occurrences(rows, assignments, &codebook)?;
    let samples = queries
        .par_iter()
        .zip(truths.par_iter())
        .enumerate()
        .map(|(query_index, (query, truth))| {
            if usize::try_from(query.query_ordinal).ok() != Some(query_index)
                || truth.query_ordinal != query.query_ordinal
                || truth.neighbor_source_ordinals.len() != 10
                || truth.ground_truth_page_assignments.len() != 10
            {
                return Err(invalid("V26 PQ8 candidate cover query authority differs"));
            }
            let ranked_candidates = tree::rank_v26_tree_page_prefix(
                primary,
                replica,
                &query.vector,
                candidate_page_limit,
            )?;
            let mut candidate_pages = ranked_candidates.clone();
            candidate_pages.sort_unstable();
            let tables = prepare_v26_pq8_tables(&codebook, &query.vector)?;
            let ranked = rank_v26_pq8_occurrences(&pages, &candidate_pages, &tables)?;
            let ranked_assignments = ranked
                .iter()
                .map(|row| row.pages.to_vec())
                .collect::<Vec<_>>();
            let mut selected_pages = exact_v26_layout_oracle_pages(&ranked_assignments, 8)?;
            for page in ranked_candidates {
                if selected_pages.len() == 8 {
                    break;
                }
                if !selected_pages.contains(&page) {
                    selected_pages.push(page);
                }
            }
            if selected_pages.len() != 8 {
                return Err(invalid("V26 PQ8 selected page inventory differs"));
            }
            selected_pages.sort_unstable();
            let oracle_pages =
                exact_v26_layout_oracle_pages(&truth.ground_truth_page_assignments, 8)?;
            let hits = v26_layout_hits(&truth.ground_truth_page_assignments, &selected_pages);
            let oracle_hits = v26_layout_hits(&truth.ground_truth_page_assignments, &oracle_pages);
            Ok(V26TreeRouterSample {
                query_ordinal: query.query_ordinal,
                selected_pages,
                hits,
                oracle_hits,
                recall_ppm: v26_ppm(u64::from(hits), 10)?,
                oracle_attainment_ppm: v26_ppm(u64::from(hits), u64::from(oracle_hits))?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let total_hits = samples.iter().try_fold(0_u64, |sum, sample| {
        sum.checked_add(u64::from(sample.hits))
            .ok_or_else(|| invalid("V26 PQ8 candidate cover metric overflows"))
    })?;
    let total_oracle_hits = samples.iter().try_fold(0_u64, |sum, sample| {
        sum.checked_add(u64::from(sample.oracle_hits))
            .ok_or_else(|| invalid("V26 PQ8 candidate cover metric overflows"))
    })?;
    let aggregate_recall_ppm = v26_ppm(total_hits, queries.len() as u64 * 10)?;
    let minimum_query_recall_ppm = samples
        .iter()
        .map(|sample| sample.recall_ppm)
        .min()
        .ok_or_else(|| invalid("V26 PQ8 candidate cover samples are absent"))?;
    let oracle_attainment_ppm = v26_ppm(total_hits, total_oracle_hits)?;
    let passed = aggregate_recall_ppm >= 975_000
        && minimum_query_recall_ppm >= 800_000
        && oracle_attainment_ppm >= 995_000;
    Ok((
        samples,
        V26TreeRouterResult {
            schema: "borsuk-v26-pq8-candidate-cover-result-v1".to_owned(),
            query_count: 512,
            aggregate_recall_ppm,
            minimum_query_recall_ppm,
            oracle_attainment_ppm,
            disposition: if passed {
                V26Disposition::BoundedLayoutCandidate
            } else {
                V26Disposition::RankReducerRejected
            },
            page_body_reads: 0,
            claim_eligible: false,
        },
    ))
}

impl PartialEq for V26Pq8RankedOccurrence {
    fn eq(&self, other: &Self) -> bool {
        self.pages == other.pages
            && self.distance.to_bits() == other.distance.to_bits()
            && self.occurrence_ordinal == other.occurrence_ordinal
    }
}

impl Eq for V26Pq8RankedOccurrence {}

impl PartialOrd for V26Pq8RankedOccurrence {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for V26Pq8RankedOccurrence {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.pages.cmp(&other.pages))
            .then_with(|| self.occurrence_ordinal.cmp(&other.occurrence_ordinal))
    }
}

pub(crate) fn projected_v26_pq8_resident_bytes(rows: u64, page_capacity: u32) -> Result<u64> {
    if rows == 0 || page_capacity == 0 {
        return Err(invalid("V26 PQ8 projection request differs"));
    }
    let occurrences = rows
        .checked_mul(2)
        .and_then(|value| value.checked_mul(12))
        .ok_or_else(|| invalid("V26 PQ8 occurrence projection overflows"))?;
    let page_count = rows
        .div_ceil(u64::from(page_capacity))
        .checked_mul(2)
        .ok_or_else(|| invalid("V26 PQ8 page projection overflows"))?;
    let offsets = page_count
        .checked_add(1)
        .and_then(|value| value.checked_mul(8))
        .ok_or_else(|| invalid("V26 PQ8 offset projection overflows"))?;
    occurrences
        .checked_add(offsets)
        .and_then(|value| value.checked_add(8 * 256 * 12 * 4))
        .and_then(|value| value.checked_add(512 * 1_024 * 1_024))
        .ok_or_else(|| invalid("V26 PQ8 resident projection overflows"))
}

pub(crate) fn rank_v26_pq8_occurrences(
    pages: &BTreeMap<u32, Vec<V26Pq8Occurrence>>,
    candidate_pages: &[u32],
    tables: &[[f32; 256]; 8],
) -> Result<Vec<V26Pq8RankedOccurrence>> {
    if candidate_pages.is_empty()
        || candidate_pages.windows(2).any(|pair| pair[0] >= pair[1])
        || tables
            .iter()
            .flatten()
            .any(|distance| !distance.is_finite())
    {
        return Err(invalid("V26 PQ8 candidate scan request differs"));
    }
    let candidates = candidate_pages.iter().copied().collect::<BTreeSet<_>>();
    let mut heap = BinaryHeap::with_capacity(10);
    let mut occurrence_ordinal = 0_u32;
    for page in candidate_pages {
        let occurrences = pages
            .get(page)
            .ok_or_else(|| invalid("V26 PQ8 candidate page is absent"))?;
        for occurrence in occurrences {
            if occurrence.partner_page == *page {
                return Err(invalid("V26 PQ8 occurrence page binding differs"));
            }
            if candidates.contains(&occurrence.partner_page) && *page > occurrence.partner_page {
                continue;
            }
            let distance = occurrence
                .code
                .iter()
                .enumerate()
                .map(|(subspace, code)| tables[subspace][usize::from(*code)])
                .sum::<f32>();
            if !distance.is_finite() {
                return Err(invalid("V26 PQ8 occurrence distance differs"));
            }
            let ranked = V26Pq8RankedOccurrence {
                pages: [
                    (*page).min(occurrence.partner_page),
                    (*page).max(occurrence.partner_page),
                ],
                distance,
                occurrence_ordinal,
            };
            occurrence_ordinal = occurrence_ordinal
                .checked_add(1)
                .ok_or_else(|| invalid("V26 PQ8 occurrence ordinal overflows"))?;
            if heap.len() < 10 {
                heap.push(ranked);
            } else if ranked < *heap.peek().unwrap() {
                heap.pop();
                heap.push(ranked);
            }
        }
    }
    if heap.len() != 10 {
        return Err(invalid("V26 PQ8 candidate row inventory differs"));
    }
    let mut ranked = heap.into_vec();
    ranked.sort();
    Ok(ranked)
}

pub(crate) fn rank_v26_candidate_rows(
    primary: &V26Tree,
    replica: &V26Tree,
    rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
    query: &[f32; 96],
    candidate_page_limit: usize,
    retained_row_limit: usize,
) -> Result<Vec<V26RankedRow>> {
    if rows.is_empty()
        || rows.len() != assignments.len()
        || retained_row_limit == 0
        || retained_row_limit > rows.len()
    {
        return Err(invalid("V26 candidate row request differs"));
    }
    validate_v26_vector(query)?;
    let candidates =
        tree::rank_v26_tree_page_prefix(primary, replica, query, candidate_page_limit)?
            .into_iter()
            .collect::<BTreeSet<_>>();
    let mut heap = BinaryHeap::with_capacity(retained_row_limit);
    for (index, (row, assignment)) in rows.iter().zip(assignments).enumerate() {
        if usize::try_from(row.source_ordinal).ok() != Some(index)
            || assignment.source_ordinal != row.source_ordinal
            || assignment.primary_page == assignment.replica_page
        {
            return Err(invalid("V26 candidate row binding differs"));
        }
        validate_v26_vector(&row.vector)?;
        if !candidates.contains(&assignment.primary_page)
            && !candidates.contains(&assignment.replica_page)
        {
            continue;
        }
        let dot = query
            .iter()
            .zip(row.vector)
            .map(|(left, right)| left * right)
            .sum::<f32>();
        let ranked = V26RankedRow {
            source_ordinal: row.source_ordinal,
            distance: 1.0 - dot,
        };
        if !ranked.distance.is_finite() {
            return Err(invalid("V26 candidate row distance differs"));
        }
        if heap.len() < retained_row_limit {
            heap.push(ranked);
        } else if ranked < *heap.peek().unwrap() {
            heap.pop();
            heap.push(ranked);
        }
    }
    if heap.len() != retained_row_limit {
        return Err(invalid("V26 candidate row inventory differs"));
    }
    let mut ranked = heap.into_vec();
    ranked.sort();
    Ok(ranked)
}

pub(crate) fn evaluate_v26_candidate_row_cover(
    primary: &V26Tree,
    replica: &V26Tree,
    rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
    queries: &[V26ExternalQuery],
    truths: &[V26QueryTruth],
    limits: (usize, usize),
) -> Result<(Vec<V26TreeRouterSample>, V26TreeRouterResult)> {
    let (candidate_page_limit, page_budget) = limits;
    if queries.len() != 512
        || truths.len() != queries.len()
        || rows.len() != assignments.len()
        || page_budget == 0
    {
        return Err(invalid("V26 candidate cover request differs"));
    }
    let samples = queries
        .par_iter()
        .zip(truths.par_iter())
        .enumerate()
        .map(|(query_index, (query, truth))| {
            if usize::try_from(query.query_ordinal).ok() != Some(query_index)
                || truth.query_ordinal != query.query_ordinal
                || truth.neighbor_source_ordinals.len() != 10
                || truth.ground_truth_page_assignments.len() != 10
            {
                return Err(invalid("V26 candidate cover query authority differs"));
            }
            let ranked_candidates = tree::rank_v26_tree_page_prefix(
                primary,
                replica,
                &query.vector,
                candidate_page_limit,
            )?;
            let candidates = ranked_candidates.iter().copied().collect::<BTreeSet<_>>();
            let ranked = rank_v26_candidate_rows(
                primary,
                replica,
                rows,
                assignments,
                &query.vector,
                candidate_page_limit,
                10,
            )?;
            let ranked_assignments = ranked
                .iter()
                .map(|row| {
                    let assignment =
                        assignments
                            .get(usize::try_from(row.source_ordinal).map_err(|_| {
                                invalid("V26 candidate cover source ordinal overflows")
                            })?)
                            .filter(|assignment| assignment.source_ordinal == row.source_ordinal)
                            .ok_or_else(|| invalid("V26 candidate cover row binding differs"))?;
                    let pages = [assignment.primary_page, assignment.replica_page]
                        .into_iter()
                        .filter(|page| candidates.contains(page))
                        .collect::<Vec<_>>();
                    if pages.is_empty() {
                        return Err(invalid("V26 candidate cover page binding differs"));
                    }
                    Ok(pages)
                })
                .collect::<Result<Vec<_>>>()?;
            let mut selected_pages =
                exact_v26_layout_oracle_pages(&ranked_assignments, page_budget)?;
            for page in ranked_candidates {
                if selected_pages.len() == page_budget {
                    break;
                }
                if !selected_pages.contains(&page) {
                    selected_pages.push(page);
                }
            }
            if selected_pages.len() != page_budget {
                return Err(invalid("V26 candidate cover page inventory differs"));
            }
            selected_pages.sort_unstable();
            let oracle_pages =
                exact_v26_layout_oracle_pages(&truth.ground_truth_page_assignments, page_budget)?;
            let hits = v26_layout_hits(&truth.ground_truth_page_assignments, &selected_pages);
            let oracle_hits = v26_layout_hits(&truth.ground_truth_page_assignments, &oracle_pages);
            Ok(V26TreeRouterSample {
                query_ordinal: query.query_ordinal,
                selected_pages,
                hits,
                oracle_hits,
                recall_ppm: v26_ppm(u64::from(hits), 10)?,
                oracle_attainment_ppm: v26_ppm(u64::from(hits), u64::from(oracle_hits))?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let total_hits = samples.iter().try_fold(0_u64, |sum, sample| {
        sum.checked_add(u64::from(sample.hits))
            .ok_or_else(|| invalid("V26 candidate cover metric overflows"))
    })?;
    let total_oracle_hits = samples.iter().try_fold(0_u64, |sum, sample| {
        sum.checked_add(u64::from(sample.oracle_hits))
            .ok_or_else(|| invalid("V26 candidate cover metric overflows"))
    })?;
    let aggregate_recall_ppm = v26_ppm(total_hits, queries.len() as u64 * 10)?;
    let minimum_query_recall_ppm = samples
        .iter()
        .map(|sample| sample.recall_ppm)
        .min()
        .ok_or_else(|| invalid("V26 candidate cover samples are absent"))?;
    let oracle_attainment_ppm = v26_ppm(total_hits, total_oracle_hits)?;
    let passed = aggregate_recall_ppm >= 975_000
        && minimum_query_recall_ppm >= 800_000
        && oracle_attainment_ppm >= 995_000;
    Ok((
        samples,
        V26TreeRouterResult {
            schema: "borsuk-v26-candidate-row-cover-result-v1".to_owned(),
            query_count: 512,
            aggregate_recall_ppm,
            minimum_query_recall_ppm,
            oracle_attainment_ppm,
            disposition: if passed {
                V26Disposition::BoundedLayoutCandidate
            } else {
                V26Disposition::RankReducerRejected
            },
            page_body_reads: 0,
            claim_eligible: false,
        },
    ))
}

fn v26_page_mode_sign(page: u32, level: u32, cluster: u32, dimension: usize) -> f64 {
    let mut value = u64::from(page).wrapping_mul(0xd6e8_feb8_6659_fd93)
        ^ u64::from(level).wrapping_mul(0xa5a3_564e_27f8_864d)
        ^ u64::from(cluster).wrapping_mul(0x94d0_49bb_1331_11eb)
        ^ (dimension as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ 0x5632_362d_4d4f_4445;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    if (value ^ (value >> 31)) & 1 == 0 {
        -1.0
    } else {
        1.0
    }
}

fn v26_page_mode_centroid(rows: &[&V26ConstructionRow]) -> Result<[f32; 96]> {
    if rows.is_empty() {
        return Err(invalid("V26 page mode is empty"));
    }
    let mut sum = [0.0_f64; 96];
    for row in rows {
        for (coordinate, value) in sum.iter_mut().zip(row.vector) {
            *coordinate += f64::from(value);
        }
    }
    let norm = sum.iter().map(|value| value * value).sum::<f64>().sqrt();
    let centroid = if norm.is_finite() && norm > 0.0 {
        std::array::from_fn(|dimension| (sum[dimension] / norm) as f32)
    } else {
        rows[0].vector
    };
    validate_v26_vector(&centroid)?;
    Ok(centroid)
}

pub(crate) fn build_v26_page_mode_centroids(
    rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
) -> Result<V26PageModeInventory> {
    if rows.is_empty() || rows.len() != assignments.len() {
        return Err(invalid("V26 page mode construction inventory differs"));
    }
    let mut page_rows = BTreeMap::<u32, Vec<&V26ConstructionRow>>::new();
    for (index, (row, assignment)) in rows.iter().zip(assignments).enumerate() {
        if usize::try_from(row.source_ordinal).ok() != Some(index)
            || assignment.source_ordinal != row.source_ordinal
            || assignment.primary_page == assignment.replica_page
        {
            return Err(invalid("V26 page mode row binding differs"));
        }
        validate_v26_vector(&row.vector)?;
        page_rows
            .entry(assignment.primary_page)
            .or_default()
            .push(row);
        page_rows
            .entry(assignment.replica_page)
            .or_default()
            .push(row);
    }
    page_rows
        .into_iter()
        .map(|(page, page_rows)| {
            let mut clusters = vec![page_rows];
            let mut ladder = BTreeMap::new();
            for (level, mode_count) in V26_PAGE_MODE_LADDER.into_iter().enumerate() {
                let level = u32::try_from(level + 1)
                    .map_err(|_| invalid("V26 page mode level overflows"))?;
                let mut split: Vec<Vec<&V26ConstructionRow>> =
                    Vec::with_capacity(mode_count as usize);
                for (cluster_ordinal, cluster) in clusters.into_iter().enumerate() {
                    if cluster.len() == 1 {
                        split.push(cluster.clone());
                        split.push(cluster);
                        continue;
                    }
                    let cluster_ordinal = u32::try_from(cluster_ordinal)
                        .map_err(|_| invalid("V26 page mode cluster overflows"))?;
                    let mut projected = cluster
                        .into_iter()
                        .map(|row| {
                            let projection = row
                                .vector
                                .iter()
                                .enumerate()
                                .map(|(dimension, value)| {
                                    f64::from(*value)
                                        * v26_page_mode_sign(
                                            page,
                                            level,
                                            cluster_ordinal,
                                            dimension,
                                        )
                                })
                                .sum::<f64>();
                            (projection, row)
                        })
                        .collect::<Vec<_>>();
                    projected.sort_by(|left, right| {
                        left.0
                            .total_cmp(&right.0)
                            .then_with(|| left.1.source_ordinal.cmp(&right.1.source_ordinal))
                    });
                    let right = projected.split_off(projected.len() / 2);
                    split.push(projected.into_iter().map(|(_, row)| row).collect());
                    split.push(right.into_iter().map(|(_, row)| row).collect());
                }
                let mut centroids = split
                    .iter()
                    .map(|cluster| v26_page_mode_centroid(cluster))
                    .collect::<Result<Vec<_>>>()?;
                centroids.sort_by(|left, right| {
                    left.iter()
                        .zip(right)
                        .find_map(|(left, right)| {
                            let ordering = left.total_cmp(right);
                            (ordering != Ordering::Equal).then_some(ordering)
                        })
                        .unwrap_or(Ordering::Equal)
                });
                if centroids.len() != mode_count as usize {
                    return Err(invalid("V26 page mode inventory differs"));
                }
                ladder.insert(mode_count, centroids);
                clusters = split;
            }
            Ok((page, ladder))
        })
        .collect()
}

pub(crate) fn evaluate_v26_centroid_router(
    primary: &V26Tree,
    replica: &V26Tree,
    rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
    queries: &[V26ExternalQuery],
    truths: &[V26QueryTruth],
    candidate_page_limit: usize,
) -> Result<(Vec<V26TreeRouterSample>, V26TreeRouterResult)> {
    let page_budget = 8;
    if queries.len() != 512 || truths.len() != queries.len() || candidate_page_limit < page_budget {
        return Err(invalid("V26 centroid router request differs"));
    }
    let centroids = build_v26_page_centroids(primary, replica, rows, assignments)?;
    let samples = queries
        .par_iter()
        .zip(truths.par_iter())
        .enumerate()
        .map(|(query_index, (query, truth))| {
            if usize::try_from(query.query_ordinal).ok() != Some(query_index)
                || truth.query_ordinal != query.query_ordinal
                || truth.neighbor_source_ordinals.len() != 10
                || truth.ground_truth_page_assignments.len() != 10
            {
                return Err(invalid("V26 centroid router query authority differs"));
            }
            let candidates = tree::rank_v26_tree_page_prefix(
                primary,
                replica,
                &query.vector,
                candidate_page_limit,
            )?;
            let mut ranked = candidates
                .into_iter()
                .map(|page| {
                    let centroid = centroids
                        .get(&page)
                        .ok_or_else(|| invalid("V26 centroid router page differs"))?;
                    let dot = query
                        .vector
                        .iter()
                        .zip(centroid)
                        .map(|(left, right)| left * right)
                        .sum::<f32>();
                    let distance = 1.0 - dot;
                    if !distance.is_finite() {
                        return Err(invalid("V26 centroid router distance differs"));
                    }
                    Ok((distance, page))
                })
                .collect::<Result<Vec<_>>>()?;
            ranked.sort_by(|left, right| {
                left.0
                    .total_cmp(&right.0)
                    .then_with(|| left.1.cmp(&right.1))
            });
            let mut selected_pages = ranked
                .into_iter()
                .take(page_budget)
                .map(|(_, page)| page)
                .collect::<Vec<_>>();
            if selected_pages.len() != page_budget {
                return Err(invalid("V26 centroid router selected pages differ"));
            }
            selected_pages.sort_unstable();
            let oracle_pages =
                exact_v26_layout_oracle_pages(&truth.ground_truth_page_assignments, page_budget)?;
            let hits = v26_layout_hits(&truth.ground_truth_page_assignments, &selected_pages);
            let oracle_hits = v26_layout_hits(&truth.ground_truth_page_assignments, &oracle_pages);
            Ok(V26TreeRouterSample {
                query_ordinal: query.query_ordinal,
                selected_pages,
                hits,
                oracle_hits,
                recall_ppm: v26_ppm(u64::from(hits), 10)?,
                oracle_attainment_ppm: v26_ppm(u64::from(hits), u64::from(oracle_hits))?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let total_hits = samples.iter().try_fold(0_u64, |sum, sample| {
        sum.checked_add(u64::from(sample.hits))
            .ok_or_else(|| invalid("V26 centroid router metric overflows"))
    })?;
    let total_oracle_hits = samples.iter().try_fold(0_u64, |sum, sample| {
        sum.checked_add(u64::from(sample.oracle_hits))
            .ok_or_else(|| invalid("V26 centroid router metric overflows"))
    })?;
    let aggregate_recall_ppm = v26_ppm(total_hits, queries.len() as u64 * 10)?;
    let minimum_query_recall_ppm = samples
        .iter()
        .map(|sample| sample.recall_ppm)
        .min()
        .ok_or_else(|| invalid("V26 centroid router samples are absent"))?;
    let oracle_attainment_ppm = v26_ppm(total_hits, total_oracle_hits)?;
    let passed = aggregate_recall_ppm >= 975_000
        && minimum_query_recall_ppm >= 800_000
        && oracle_attainment_ppm >= 995_000;
    Ok((
        samples,
        V26TreeRouterResult {
            schema: "borsuk-v26-centroid-router-result-v1".to_owned(),
            query_count: 512,
            aggregate_recall_ppm,
            minimum_query_recall_ppm,
            oracle_attainment_ppm,
            disposition: if passed {
                V26Disposition::BoundedLayoutCandidate
            } else {
                V26Disposition::TreeRouterRejected
            },
            page_body_reads: 0,
            claim_eligible: false,
        },
    ))
}

pub(crate) fn evaluate_v26_page_mode_router(
    primary: &V26Tree,
    replica: &V26Tree,
    rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
    queries: &[V26ExternalQuery],
    truths: &[V26QueryTruth],
    candidate_page_limit: usize,
) -> Result<(Vec<V26PageModeSample>, Vec<V26PageModeResult>)> {
    let page_budget = 8;
    if queries.len() != 512 || truths.len() != queries.len() || candidate_page_limit < page_budget {
        return Err(invalid("V26 page mode router request differs"));
    }
    let page_modes = build_v26_page_mode_centroids(rows, assignments)?;
    let candidate_page_limit = u32::try_from(candidate_page_limit)
        .map_err(|_| invalid("V26 page mode candidate limit overflows"))?;
    let per_query = queries
        .par_iter()
        .zip(truths.par_iter())
        .enumerate()
        .map(|(query_index, (query, truth))| {
            if usize::try_from(query.query_ordinal).ok() != Some(query_index)
                || truth.query_ordinal != query.query_ordinal
                || truth.neighbor_source_ordinals.len() != 10
                || truth.ground_truth_page_assignments.len() != 10
            {
                return Err(invalid("V26 page mode query authority differs"));
            }
            let candidates = tree::rank_v26_tree_page_prefix(
                primary,
                replica,
                &query.vector,
                candidate_page_limit as usize,
            )?;
            V26_PAGE_MODE_LADDER
                .into_iter()
                .map(|mode_count| {
                    let mut ranked = candidates
                        .iter()
                        .map(|page| {
                            let modes = page_modes
                                .get(page)
                                .and_then(|ladder| ladder.get(&mode_count))
                                .ok_or_else(|| invalid("V26 page mode inventory differs"))?;
                            let distance = modes
                                .iter()
                                .map(|mode| {
                                    1.0_f32
                                        - query
                                            .vector
                                            .iter()
                                            .zip(mode)
                                            .map(|(left, right)| left * right)
                                            .sum::<f32>()
                                })
                                .min_by(f32::total_cmp)
                                .ok_or_else(|| invalid("V26 page mode is empty"))?;
                            if !distance.is_finite() {
                                return Err(invalid("V26 page mode distance differs"));
                            }
                            Ok((distance, *page))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    ranked.sort_by(|left, right| {
                        left.0
                            .total_cmp(&right.0)
                            .then_with(|| left.1.cmp(&right.1))
                    });
                    let mut selected_pages = ranked
                        .into_iter()
                        .take(page_budget)
                        .map(|(_, page)| page)
                        .collect::<Vec<_>>();
                    if selected_pages.len() != page_budget {
                        return Err(invalid("V26 page mode selected pages differ"));
                    }
                    selected_pages.sort_unstable();
                    let oracle_pages = exact_v26_layout_oracle_pages(
                        &truth.ground_truth_page_assignments,
                        page_budget,
                    )?;
                    let hits =
                        v26_layout_hits(&truth.ground_truth_page_assignments, &selected_pages);
                    let oracle_hits =
                        v26_layout_hits(&truth.ground_truth_page_assignments, &oracle_pages);
                    Ok(V26PageModeSample {
                        query_ordinal: query.query_ordinal,
                        mode_count,
                        candidate_page_limit,
                        selected_pages,
                        hits,
                        oracle_hits,
                        recall_ppm: v26_ppm(u64::from(hits), 10)?,
                        oracle_attainment_ppm: v26_ppm(u64::from(hits), u64::from(oracle_hits))?,
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    let samples = per_query.into_iter().flatten().collect::<Vec<_>>();
    let results = V26_PAGE_MODE_LADDER
        .into_iter()
        .map(|mode_count| {
            let arm = samples
                .iter()
                .filter(|sample| sample.mode_count == mode_count)
                .collect::<Vec<_>>();
            if arm.len() != queries.len() {
                return Err(invalid("V26 page mode sample inventory differs"));
            }
            let total_hits = arm.iter().try_fold(0_u64, |sum, sample| {
                sum.checked_add(u64::from(sample.hits))
                    .ok_or_else(|| invalid("V26 page mode metric overflows"))
            })?;
            let total_oracle_hits = arm.iter().try_fold(0_u64, |sum, sample| {
                sum.checked_add(u64::from(sample.oracle_hits))
                    .ok_or_else(|| invalid("V26 page mode metric overflows"))
            })?;
            let aggregate_recall_ppm = v26_ppm(total_hits, queries.len() as u64 * 10)?;
            let minimum_query_recall_ppm = arm
                .iter()
                .map(|sample| sample.recall_ppm)
                .min()
                .ok_or_else(|| invalid("V26 page mode samples are absent"))?;
            let oracle_attainment_ppm = v26_ppm(total_hits, total_oracle_hits)?;
            Ok(V26PageModeResult {
                mode_count,
                candidate_page_limit,
                aggregate_recall_ppm,
                minimum_query_recall_ppm,
                oracle_attainment_ppm,
                passed: aggregate_recall_ppm >= 975_000
                    && minimum_query_recall_ppm >= 800_000
                    && oracle_attainment_ppm >= 995_000,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((samples, results))
}

pub fn canonical_v26_tree_router_result_bytes(
    result: &V26TreeRouterResult,
    primary: &V26Tree,
    replica: &V26Tree,
    queries: &[V26ExternalQuery],
    truths: &[V26QueryTruth],
    samples: &[V26TreeRouterSample],
) -> Result<Vec<u8>> {
    let (expected_samples, expected_result) =
        evaluate_v26_tree_router(primary, replica, queries, truths, 8)?;
    if samples != expected_samples || result != &expected_result {
        return Err(invalid("V26 tree router result authority differs"));
    }
    let value = serde_json::json!({"result": result, "samples": samples});
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V26 tree router serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum V26Disposition {
    AuthorityStop,
    LayoutRejected,
    RankReducerRejected,
    TreeRouterRejected,
    BoundedLayoutCandidate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26LayoutResult {
    pub schema: String,
    pub query_count: u32,
    pub aggregate_recall_ppm: u64,
    pub minimum_query_recall_ppm: u64,
    pub disposition: V26Disposition,
    pub page_body_reads: u64,
    pub claim_eligible: bool,
}

pub(crate) fn projected_steps(rows: u64, leaves: u64, page_capacity: u32) -> Result<u64> {
    if leaves <= 1 {
        return Ok(0);
    }
    let left_leaves = leaves / 2;
    let right_leaves = leaves - left_leaves;
    let left_rows = (rows - right_leaves).min(
        left_leaves
            .checked_mul(u64::from(page_capacity))
            .ok_or_else(|| invalid("V26 projection work overflows"))?,
    );
    let right_rows = rows - left_rows;
    let own = rows
        .checked_mul(16 * 96)
        .ok_or_else(|| invalid("V26 projection work overflows"))?;
    let left = projected_steps(left_rows, left_leaves, page_capacity)?;
    let right = projected_steps(right_rows, right_leaves, page_capacity)?;
    own.checked_add(left)
        .and_then(|partial| partial.checked_add(right))
        .ok_or_else(|| invalid("V26 projection work overflows"))
}

fn validate_identity(
    identity: &V26ObjectIdentity,
    expected_role: &str,
    generation: &str,
) -> Result<()> {
    if identity.role != expected_role
        || identity.generation != generation
        || identity.digest_algorithm != "sha256"
        || !exact_lower_hex(&identity.digest, 64)
        || identity.encoded_bytes == 0
        || !identity.uri.starts_with("s3://")
    {
        return Err(invalid("V26 object identity differs"));
    }
    Ok(())
}

pub(crate) fn validate_layout_authority(authority: &V26LayoutAuthority) -> Result<()> {
    if authority.schema != V26_LAYOUT_SCHEMA
        || authority.generation.is_empty()
        || !exact_lower_hex(&authority.source_commit, 40)
        || !exact_lower_hex(&authority.source_archive_sha256, 64)
        || authority.primary_seed != V26_PRIMARY_SEED
        || authority.replica_seed != V26_REPLICA_SEED
        || !V26_PAGE_CAPACITY_LADDER.contains(&authority.page_capacity)
        || authority.expected_rows == 0
    {
        return Err(invalid("V26 layout authority differs"));
    }
    validate_identity(
        &authority.binary,
        "v26-layout-binary",
        &authority.generation,
    )?;
    validate_identity(
        &authority.construction_rows,
        "construction-parquet",
        &authority.generation,
    )?;
    let mut uris = BTreeSet::new();
    if [&authority.binary, &authority.construction_rows]
        .into_iter()
        .any(|identity| !uris.insert(identity.uri.as_str()))
    {
        return Err(invalid("V26 authority URI roles overlap"));
    }
    Ok(())
}

fn validate_receipt(receipt: &V26LayoutReceipt) -> Result<()> {
    let authority = &receipt.authority;
    validate_layout_authority(authority)?;
    if receipt.row_count != authority.expected_rows {
        return Err(invalid("V26 layout authority differs"));
    }
    let leaves = receipt
        .row_count
        .div_ceil(u64::from(authority.page_capacity));
    let leaves_u32 = u32::try_from(leaves).map_err(|_| invalid("V26 page count overflows"))?;
    if receipt.leaves_per_tree != leaves_u32
        || receipt.page_count
            != leaves_u32
                .checked_mul(2)
                .ok_or_else(|| invalid("V26 page count overflows"))?
        || receipt.projection_steps
            != projected_steps(receipt.row_count, leaves, authority.page_capacity)?
                .checked_mul(2)
                .ok_or_else(|| invalid("V26 projection work overflows"))?
        || receipt.worker_count == 0
        || receipt.elapsed_ns == 0
        || receipt.cpu_ns == 0
        || receipt.peak_rss_bytes == 0
        || receipt.peak_psi_full_avg10_milli_percent > 500
        || receipt.swap_end_bytes != receipt.swap_start_bytes
        || receipt.query_role_opens != 0
        || receipt.page_body_reads != 0
        || receipt.claim_eligible
    {
        return Err(invalid("V26 layout receipt differs"));
    }

    let input_roles = ["construction-parquet", "layout-manifest"];
    let output_roles = [
        "page-assignments-parquet",
        "primary-tree-parquet",
        "replica-tree-parquet",
    ];
    if receipt.inputs.len() != input_roles.len() || receipt.outputs.len() != output_roles.len() {
        return Err(invalid("V26 object inventory differs"));
    }
    if receipt.inputs[0] != authority.construction_rows {
        return Err(invalid("V26 construction input authority differs"));
    }
    for (identity, role) in receipt.inputs.iter().zip(input_roles) {
        validate_identity(identity, role, &authority.generation)?;
    }
    for (identity, role) in receipt.outputs.iter().zip(output_roles) {
        validate_identity(identity, role, &authority.generation)?;
    }
    let mut uris = BTreeSet::new();
    if receipt
        .inputs
        .iter()
        .chain(&receipt.outputs)
        .any(|identity| !uris.insert(identity.uri.as_str()))
    {
        return Err(invalid("V26 object URI roles overlap"));
    }
    Ok(())
}

fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, canonical_json_value(value)))
                .collect(),
        ),
        scalar => scalar,
    }
}

pub fn canonical_v26_layout_receipt_bytes(receipt: &V26LayoutReceipt) -> Result<Vec<u8>> {
    validate_receipt(receipt)?;
    let value = serde_json::to_value(receipt)
        .map_err(|error| V26Error(format!("V26 layout receipt serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| V26Error(format!("V26 layout receipt serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub fn canonical_v26_object_identity_bytes(identity: &V26ObjectIdentity) -> Result<Vec<u8>> {
    validate_identity(identity, &identity.role, &identity.generation)?;
    let value = serde_json::to_value(identity)
        .map_err(|error| invalid(&format!("V26 identity serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V26 identity serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub fn canonical_v26_layout_result_bytes(
    result: &V26LayoutResult,
    truths: &[V26QueryTruth],
    samples: &[V26LayoutSample],
) -> Result<Vec<u8>> {
    canonical_v26_layout_result_bytes_with_page_budget(result, truths, samples, 8)
}

pub(crate) fn canonical_v26_layout_result_bytes_with_page_budget(
    result: &V26LayoutResult,
    truths: &[V26QueryTruth],
    samples: &[V26LayoutSample],
    page_budget: usize,
) -> Result<Vec<u8>> {
    if page_budget == 0 {
        return Err(invalid("V26 layout page budget differs"));
    }
    if result.schema != "borsuk-v26-layout-result-v1"
        || result.query_count != 512
        || truths.len() != 512
        || samples.len() != truths.len()
        || result.page_body_reads != 0
        || result.claim_eligible
    {
        return Err(invalid("V26 layout result authority differs"));
    }
    let mut total_hits = 0_u64;
    let mut minimum_recall = 1_000_000_u64;
    for (query_index, (truth, sample)) in truths.iter().zip(samples).enumerate() {
        if usize::try_from(truth.query_ordinal).ok() != Some(query_index)
            || sample.query_ordinal != truth.query_ordinal
            || truth.neighbor_source_ordinals.len() != 10
            || truth
                .neighbor_source_ordinals
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
                != 10
            || truth.ground_truth_page_assignments.len() != 10
        {
            return Err(invalid("V26 layout truth authority differs"));
        }
        let selected =
            exact_v26_layout_oracle_pages(&truth.ground_truth_page_assignments, page_budget)?;
        let hits = v26_layout_hits(&truth.ground_truth_page_assignments, &selected);
        let recall = v26_ppm(u64::from(hits), 10)?;
        if sample.selected_pages != selected || sample.hits != hits || sample.recall_ppm != recall {
            return Err(invalid("V26 layout sample differs"));
        }
        total_hits = total_hits
            .checked_add(u64::from(hits))
            .ok_or_else(|| invalid("V26 metric arithmetic differs"))?;
        minimum_recall = minimum_recall.min(recall);
    }
    let aggregate = v26_ppm(total_hits, truths.len() as u64 * 10)?;
    let expected_disposition = if aggregate >= 995_000 && minimum_recall >= 800_000 {
        V26Disposition::BoundedLayoutCandidate
    } else {
        V26Disposition::LayoutRejected
    };
    if result.aggregate_recall_ppm != aggregate
        || result.minimum_query_recall_ppm != minimum_recall
        || result.disposition != expected_disposition
    {
        return Err(invalid("V26 layout result metrics differ"));
    }
    let value = serde_json::json!({"result": result, "samples": samples});
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| V26Error(format!("V26 layout result serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub fn canonical_v26_exact_global_result_bytes(
    result: &V26ExactGlobalResult,
    rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
    queries: &[V26ExternalQuery],
    truths: &[V26QueryTruth],
    samples: &[V26ExactGlobalSample],
) -> Result<Vec<u8>> {
    const LIMITS: [u32; 6] = [10, 32, 128, 512, 2_048, 4_096];
    let expected_samples =
        evaluate_v26_exact_global_external_rows(rows, assignments, queries, truths, &LIMITS, 8)?;
    if result.schema != "borsuk-v26-cumulative-exact-global-result-v1"
        || result.query_count != 512
        || queries.len() != 512
        || truths.len() != queries.len()
        || samples.len() != queries.len() * LIMITS.len()
        || result.rank_results.len() != LIMITS.len()
        || result.page_body_reads != 0
        || result.claim_eligible
        || samples != expected_samples
    {
        return Err(invalid("V26 exact-global result authority differs"));
    }
    let pages_by_source = assignments
        .iter()
        .map(|assignment| (assignment.source_ordinal, *assignment))
        .collect::<BTreeMap<_, _>>();
    if pages_by_source.len() != assignments.len()
        || assignments
            .iter()
            .any(|row| row.primary_page == row.replica_page)
    {
        return Err(invalid("V26 exact-global assignment authority differs"));
    }

    let mut total_hits = [0_u64; 6];
    let mut total_oracle_hits = [0_u64; 6];
    let mut minimum_recall = [1_000_000_u64; 6];
    for (query_index, (query, truth)) in queries.iter().zip(truths).enumerate() {
        if usize::try_from(query.query_ordinal).ok() != Some(query_index)
            || truth.query_ordinal != query.query_ordinal
        {
            return Err(invalid("V26 exact-global query result authority differs"));
        }
        let oracle_pages = exact_v26_layout_oracle_pages(&truth.ground_truth_page_assignments, 8)?;
        let expected_oracle_hits =
            v26_layout_hits(&truth.ground_truth_page_assignments, &oracle_pages);
        let query_samples = &samples[query_index * LIMITS.len()..][..LIMITS.len()];
        let head = &query_samples[0].first_ten_ranked_rows;
        if head.len() != 10
            || head
                .iter()
                .map(|row| row.source_ordinal)
                .collect::<BTreeSet<_>>()
                .len()
                != 10
        {
            return Err(invalid("V26 exact-global ranked head authority differs"));
        }
        let mut injected_ranked = Vec::with_capacity(10);
        for evidence in head {
            let observed = pages_by_source
                .get(&evidence.source_ordinal)
                .ok_or_else(|| invalid("V26 exact-global ranked head binding differs"))?;
            if evidence.primary_page != observed.primary_page
                || evidence.replica_page != observed.replica_page
            {
                return Err(invalid("V26 exact-global ranked head binding differs"));
            }
            injected_ranked.push(V26RankedRow {
                source_ordinal: evidence.source_ordinal,
                distance: f32::from_bits(evidence.distance_bits),
            });
        }
        let mut injected_pages = select_v26_ranked_pages(&injected_ranked, &pages_by_source, 8)?;
        injected_pages.sort_unstable();

        for (limit_index, (sample, limit)) in query_samples.iter().zip(LIMITS).enumerate() {
            let selected_is_sorted = sample
                .selected_pages
                .windows(2)
                .all(|pair| pair[0] < pair[1]);
            let hits =
                v26_layout_hits(&truth.ground_truth_page_assignments, &sample.selected_pages);
            let recall = v26_ppm(u64::from(hits), 10)?;
            let attainment = v26_ppm(u64::from(hits), u64::from(expected_oracle_hits))?;
            if sample.query_ordinal != query.query_ordinal
                || sample.ranked_row_limit != limit
                || sample.candidate_rows == 0
                || sample.selected_pages.is_empty()
                || sample.selected_pages.len() > 8
                || !selected_is_sorted
                || sample.first_ten_ranked_rows != *head
                || sample.hits != hits
                || sample.oracle_hits != expected_oracle_hits
                || sample.recall_ppm != recall
                || sample.oracle_attainment_ppm != attainment
                || limit_index == 0 && sample.selected_pages != injected_pages
            {
                return Err(invalid("V26 exact-global sample authority differs"));
            }
            total_hits[limit_index] = total_hits[limit_index]
                .checked_add(u64::from(hits))
                .ok_or_else(|| invalid("V26 exact-global metric overflow"))?;
            total_oracle_hits[limit_index] = total_oracle_hits[limit_index]
                .checked_add(u64::from(expected_oracle_hits))
                .ok_or_else(|| invalid("V26 exact-global metric overflow"))?;
            minimum_recall[limit_index] = minimum_recall[limit_index].min(recall);
        }
    }

    let mut any_passed = false;
    for (index, (rank_result, limit)) in result.rank_results.iter().zip(LIMITS).enumerate() {
        let aggregate = v26_ppm(total_hits[index], queries.len() as u64 * 10)?;
        let attainment = v26_ppm(total_hits[index], total_oracle_hits[index])?;
        let passed = aggregate >= 975_000 && attainment >= 995_000;
        if rank_result.ranked_row_limit != limit
            || rank_result.aggregate_recall_ppm != aggregate
            || rank_result.minimum_query_recall_ppm != minimum_recall[index]
            || rank_result.oracle_attainment_ppm != attainment
            || rank_result.passed != passed
        {
            return Err(invalid("V26 exact-global rank result differs"));
        }
        any_passed |= passed;
    }
    let expected_disposition = if any_passed {
        V26Disposition::BoundedLayoutCandidate
    } else {
        V26Disposition::RankReducerRejected
    };
    if result.disposition != expected_disposition {
        return Err(invalid("V26 exact-global disposition differs"));
    }
    let value = serde_json::json!({"result": result, "samples": samples});
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V26 exact-global serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::{
        V26_PAGE_MODE_LADDER, V26_PQ_WIDTH_LADDER, V26ConstructionRow, V26Disposition,
        V26ExactGlobalRankResult, V26ExactGlobalResult, V26ExternalQuery, V26ExternalTruth,
        V26LayoutAuthority, V26LayoutReceipt, V26LayoutResult, V26LayoutSample, V26Node,
        V26ObjectIdentity, V26Pq8Occurrence, V26QueryTruth, V26RankedRow, V26RowPages, V26Tree,
        build_v26_dual_tree_layout, build_v26_external_truth_rows, build_v26_page_mode_centroids,
        build_v26_pq8_page_occurrences, build_v26_pq16_packed_index,
        canonical_v26_exact_global_result_bytes, canonical_v26_layout_receipt_bytes,
        canonical_v26_layout_result_bytes, canonical_v26_tree_router_result_bytes,
        diagnose_v26_tree_router_candidate_widths, evaluate_v26_candidate_row_cover,
        evaluate_v26_centroid_router, evaluate_v26_exact_global_external_rows,
        evaluate_v26_page_mode_router, evaluate_v26_pq_width_ladder,
        evaluate_v26_pq8_candidate_cover, evaluate_v26_pq16_exact_rerank_ladder,
        evaluate_v26_tree_router, exact_v26_layout_oracle_pages, fit_v26_pq_codebook,
        fit_v26_pq8_codebook, prepare_v26_pq_tables, prepare_v26_pq8_tables,
        projected_v26_pq_resident_bytes, projected_v26_pq8_resident_bytes, rank_v26_candidate_rows,
        rank_v26_pq8_occurrences, rank_v26_pq16_candidate_rows, rank_v26_pq16_global_candidates,
        rank_v26_pq16_linear_occurrence_candidates, rank_v26_pq16_packed_candidates,
        rank_v26_pq16_parallel_occurrence_candidates, rank_v26_tree_pages, route_v26_pages,
        select_v26_pq16_global_packed_pages, select_v26_pq16_packed_pages, select_v26_ranked_pages,
        validate_v26_dual_tree_layout,
    };

    const PRIMARY_SEED: u64 = 0x5632_362d_5452_4545;
    const REPLICA_SEED: u64 = 0x5632_362d_5245_504c;

    fn row(source_ordinal: u64) -> V26ConstructionRow {
        let mut vector = [0.0_f32; 96];
        for (dimension, coordinate) in vector.iter_mut().enumerate() {
            let raw = ((source_ordinal * 37 + dimension as u64 * 17) % 257) as i32 - 128;
            *coordinate = raw as f32 / 128.0;
        }
        V26ConstructionRow {
            source_ordinal,
            vector,
        }
    }

    fn v26_router_test_sign(seed: u64, node: u32) -> f32 {
        let mut value = seed ^ u64::from(node).wrapping_mul(0xd6e8_feb8_6659_fd93);
        value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^= value >> 31;
        if value & 1 == 0 { -1.0 } else { 1.0 }
    }

    fn v26_router_test_tree(seed: u64, page_offset: u32, margins: [f32; 7]) -> V26Tree {
        let children = [
            Some((1, 8)),
            Some((2, 5)),
            Some((3, 4)),
            None,
            None,
            Some((6, 7)),
            None,
            None,
            Some((9, 12)),
            Some((10, 11)),
            None,
            None,
            Some((13, 14)),
            None,
            None,
        ];
        let mut margin_index = 0;
        let mut page = page_offset;
        let nodes = children
            .into_iter()
            .enumerate()
            .map(|(node, children)| {
                let node_ordinal = u32::try_from(node).unwrap();
                if let Some((left, right)) = children {
                    let margin = margins[margin_index];
                    margin_index += 1;
                    V26Node {
                        node_ordinal,
                        left: Some(left),
                        right: Some(right),
                        direction_ordinal: 0,
                        threshold: v26_router_test_sign(seed, node_ordinal) + margin,
                        split_gap: 0.0,
                        leaf_page: None,
                    }
                } else {
                    let leaf = V26Node {
                        node_ordinal,
                        left: None,
                        right: None,
                        direction_ordinal: 0,
                        threshold: 0.0,
                        split_gap: 0.0,
                        leaf_page: Some(page),
                    };
                    page += 1;
                    leaf
                }
            })
            .collect();
        V26Tree {
            seed,
            root: 0,
            nodes,
        }
    }

    #[test]
    fn v26_tree_router_best_first_is_bounded_deterministic_and_query_only() {
        // Break caught: routing walks one tree depth-first, ignores global sibling margins, or
        // emits more than the fixed eight-page budget.
        let primary = v26_router_test_tree(PRIMARY_SEED, 0, [100.0, 10.0, 1.0, 2.0, 5.0, 1.0, 2.0]);
        let replica = v26_router_test_tree(REPLICA_SEED, 8, [200.0, 20.0, 3.0, 4.0, 5.0, 1.0, 2.0]);
        let mut query = [0.0_f32; 96];
        query[0] = 1.0;

        let pages = route_v26_pages(&primary, &replica, &query, 8).unwrap();

        assert_eq!(pages, vec![0, 1, 2, 3, 8, 9, 10, 11]);
        assert_eq!(
            route_v26_pages(&primary, &replica, &query, 8).unwrap(),
            pages
        );
        assert!(route_v26_pages(&primary, &replica, &query, 7).is_err());
    }

    #[test]
    fn v26_tree_router_result_recomputes_samples_gates_and_disposition() {
        // Break caught: a router result serializes selected pages or aggregate claims without
        // independently rerunning the authenticated tree traversal.
        let primary = v26_router_test_tree(PRIMARY_SEED, 0, [100.0, 10.0, 1.0, 2.0, 5.0, 1.0, 2.0]);
        let replica = v26_router_test_tree(REPLICA_SEED, 8, [200.0, 20.0, 3.0, 4.0, 5.0, 1.0, 2.0]);
        let mut vector = [0.0_f32; 96];
        vector[0] = 1.0;
        let queries = (0_u32..512)
            .map(|query_ordinal| V26ExternalQuery {
                query_ordinal,
                vector,
            })
            .collect::<Vec<_>>();
        let selected = [0_u32, 1, 2, 3, 8, 9, 10, 11];
        let truths = (0_u32..512)
            .map(|query_ordinal| V26QueryTruth {
                query_ordinal,
                neighbor_source_ordinals: (0_u64..10).collect(),
                ground_truth_page_assignments: (0..10)
                    .map(|neighbor| vec![selected[neighbor % selected.len()]])
                    .collect(),
            })
            .collect::<Vec<_>>();

        let (samples, result) =
            evaluate_v26_tree_router(&primary, &replica, &queries, &truths, 8).unwrap();
        let bytes = canonical_v26_tree_router_result_bytes(
            &result, &primary, &replica, &queries, &truths, &samples,
        )
        .unwrap();

        assert_eq!(result.aggregate_recall_ppm, 1_000_000);
        assert_eq!(result.minimum_query_recall_ppm, 1_000_000);
        assert_eq!(result.oracle_attainment_ppm, 1_000_000);
        assert_eq!(result.disposition, V26Disposition::BoundedLayoutCandidate);
        assert_eq!(bytes.last(), Some(&b'\n'));

        let mut forged_samples = samples.clone();
        forged_samples[0].selected_pages[0] = 7;
        assert!(
            canonical_v26_tree_router_result_bytes(
                &result,
                &primary,
                &replica,
                &queries,
                &truths,
                &forged_samples,
            )
            .is_err()
        );
        let mut forged_result = result.clone();
        forged_result.aggregate_recall_ppm -= 1;
        assert!(
            canonical_v26_tree_router_result_bytes(
                &forged_result,
                &primary,
                &replica,
                &queries,
                &truths,
                &samples,
            )
            .is_err()
        );
    }

    #[test]
    fn v26_tree_router_diagnostic_ranks_every_leaf_without_page_reads() {
        // Break caught: the diagnostic reuses the eight-page serving cutoff, drops a leaf, or
        // orders independent-tree frontier ties nondeterministically.
        let primary = v26_router_test_tree(PRIMARY_SEED, 0, [100.0, 10.0, 1.0, 2.0, 5.0, 1.0, 2.0]);
        let replica = v26_router_test_tree(REPLICA_SEED, 8, [200.0, 20.0, 3.0, 4.0, 5.0, 1.0, 2.0]);
        let mut query = [0.0_f32; 96];
        query[0] = 1.0;

        let ranked = rank_v26_tree_pages(&primary, &replica, &query).unwrap();

        assert_eq!(
            ranked,
            vec![0, 8, 1, 9, 2, 3, 10, 11, 4, 5, 6, 7, 12, 13, 14, 15]
        );
        assert_eq!(ranked[..8].iter().copied().collect::<BTreeSet<_>>(), {
            let mut served = route_v26_pages(&primary, &replica, &query, 8).unwrap();
            served.sort_unstable();
            served.into_iter().collect()
        });
    }

    #[test]
    fn v26_tree_router_diagnostic_locates_the_smallest_repairable_candidate_width() {
        // Break caught: the diagnostic scores the unrestricted oracle instead of restricting it
        // to the ranked candidate prefix, or silently changes the exact ten-page read budget.
        let primary = v26_router_test_tree(PRIMARY_SEED, 0, [100.0, 10.0, 1.0, 2.0, 5.0, 1.0, 2.0]);
        let replica = v26_router_test_tree(REPLICA_SEED, 8, [200.0, 20.0, 3.0, 4.0, 5.0, 1.0, 2.0]);
        let mut vector = [0.0_f32; 96];
        vector[0] = 1.0;
        let queries = (0_u32..512)
            .map(|query_ordinal| V26ExternalQuery {
                query_ordinal,
                vector,
            })
            .collect::<Vec<_>>();
        let bound_pages = [
            vec![0, 6],
            vec![6, 8],
            vec![1, 7],
            vec![7, 9],
            vec![2],
            vec![10],
            vec![3],
            vec![11],
            vec![6],
            vec![7],
        ];
        let truths = (0_u32..512)
            .map(|query_ordinal| V26QueryTruth {
                query_ordinal,
                neighbor_source_ordinals: (0_u64..10).collect(),
                ground_truth_page_assignments: bound_pages.to_vec(),
            })
            .collect::<Vec<_>>();

        let (samples, widths) =
            diagnose_v26_tree_router_candidate_widths(&primary, &replica, &queries, &truths)
                .unwrap();

        assert_eq!(widths.len(), 2);
        assert_eq!(widths[0].candidate_page_limit, 8);
        assert_eq!(widths[0].aggregate_recall_ppm, 800_000);
        assert_eq!(widths[0].minimum_query_recall_ppm, 800_000);
        assert_eq!(widths[0].oracle_attainment_ppm, 800_000);
        assert!(!widths[0].passed);
        assert_eq!(widths[1].candidate_page_limit, 16);
        assert_eq!(widths[1].aggregate_recall_ppm, 1_000_000);
        assert_eq!(widths[1].minimum_query_recall_ppm, 1_000_000);
        assert_eq!(widths[1].oracle_attainment_ppm, 1_000_000);
        assert!(widths[1].passed);
        assert_eq!(samples.len(), 1_024);
        assert_eq!(samples[0].candidate_page_limit, 8);
        assert_eq!(samples[0].hits, 8);
        assert_eq!(samples[1].candidate_page_limit, 16);
        assert_eq!(samples[1].hits, 10);
        assert!(
            samples
                .iter()
                .all(|sample| sample.selected_pages.len() <= 10)
        );
    }

    #[test]
    fn v26_centroid_router_reranks_a_bounded_frontier_without_truth_or_page_reads() {
        // Break caught: page centroids depend on query/truth data, candidate order replaces
        // centroid distance, or the reranker widens beyond its fixed frontier/eight-page budget.
        let primary = v26_router_test_tree(PRIMARY_SEED, 0, [100.0, 10.0, 1.0, 2.0, 5.0, 1.0, 2.0]);
        let replica = v26_router_test_tree(REPLICA_SEED, 8, [200.0, 20.0, 3.0, 4.0, 5.0, 1.0, 2.0]);
        let mut vector = [0.0_f32; 96];
        vector[0] = 1.0;
        let rows = (0_u64..32)
            .map(|source_ordinal| V26ConstructionRow {
                source_ordinal,
                vector,
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..32)
            .map(|source_ordinal| V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal % 8).unwrap(),
                replica_page: 8 + u32::try_from(source_ordinal % 8).unwrap(),
            })
            .collect::<Vec<_>>();
        let queries = (0_u32..512)
            .map(|query_ordinal| V26ExternalQuery {
                query_ordinal,
                vector,
            })
            .collect::<Vec<_>>();
        let truths = (0_u32..512)
            .map(|query_ordinal| V26QueryTruth {
                query_ordinal,
                neighbor_source_ordinals: (0_u64..10).collect(),
                ground_truth_page_assignments: (0_u32..10)
                    .map(|neighbor| vec![neighbor % 8, 8 + neighbor % 8])
                    .collect(),
            })
            .collect::<Vec<_>>();

        let (_, narrow) = evaluate_v26_centroid_router(
            &primary,
            &replica,
            &rows,
            &assignments,
            &queries,
            &truths,
            8,
        )
        .unwrap();
        let (samples, wide) = evaluate_v26_centroid_router(
            &primary,
            &replica,
            &rows,
            &assignments,
            &queries,
            &truths,
            16,
        )
        .unwrap();

        assert_eq!(narrow.aggregate_recall_ppm, 600_000);
        assert_eq!(narrow.disposition, V26Disposition::TreeRouterRejected);
        assert_eq!(wide.aggregate_recall_ppm, 1_000_000);
        assert_eq!(wide.minimum_query_recall_ppm, 1_000_000);
        assert_eq!(wide.oracle_attainment_ppm, 1_000_000);
        assert_eq!(wide.disposition, V26Disposition::BoundedLayoutCandidate);
        assert_eq!(samples[0].selected_pages, vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert!(
            samples
                .iter()
                .all(|sample| sample.selected_pages.len() == 8)
        );
    }

    #[test]
    fn v26_page_mode_summaries_preserve_separated_neighborhoods_without_queries() {
        // Break caught: a page is collapsed to one mean, summary construction consumes query or
        // truth data, or the preregistered nested mode ladder becomes a runtime tuning surface.
        let rows = (0_u64..32)
            .map(|source_ordinal| {
                let mut vector = [0.0_f32; 96];
                vector[0] = if source_ordinal < 16 { -1.0 } else { 1.0 };
                V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..32)
            .map(|source_ordinal| V26RowPages {
                source_ordinal,
                primary_page: 0,
                replica_page: 1,
            })
            .collect::<Vec<_>>();

        assert_eq!(V26_PAGE_MODE_LADDER, [2, 4, 8, 16]);
        let first = build_v26_page_mode_centroids(&rows, &assignments).unwrap();
        let second = build_v26_page_mode_centroids(&rows, &assignments).unwrap();

        assert_eq!(first, second);
        for page in [0, 1] {
            let ladder = first.get(&page).unwrap();
            assert_eq!(ladder.keys().copied().collect::<Vec<_>>(), [2, 4, 8, 16]);
            let two_modes = &ladder[&2];
            assert_eq!(two_modes.len(), 2);
            assert_eq!(two_modes[0][0], -1.0);
            assert_eq!(two_modes[1][0], 1.0);
            assert!(
                two_modes
                    .iter()
                    .all(|mode| { mode[1..].iter().all(|coordinate| coordinate.to_bits() == 0) })
            );
            assert!(
                ladder
                    .iter()
                    .all(|(mode_count, modes)| modes.len() == *mode_count as usize)
            );
        }
    }

    #[test]
    fn v26_page_mode_router_scores_fixed_ladder_inside_one_bounded_frontier() {
        // Break caught: a K arm widens the tree frontier, selects more than eight pages, uses
        // truth while ranking, or changes the literal promotion gates between arms.
        let primary = v26_router_test_tree(PRIMARY_SEED, 0, [100.0, 10.0, 1.0, 2.0, 5.0, 1.0, 2.0]);
        let replica = v26_router_test_tree(REPLICA_SEED, 8, [200.0, 20.0, 3.0, 4.0, 5.0, 1.0, 2.0]);
        let mut vector = [0.0_f32; 96];
        vector[0] = 1.0;
        let rows = (0_u64..128)
            .map(|source_ordinal| V26ConstructionRow {
                source_ordinal,
                vector,
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..128)
            .map(|source_ordinal| V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal % 8).unwrap(),
                replica_page: 8 + u32::try_from(source_ordinal % 8).unwrap(),
            })
            .collect::<Vec<_>>();
        let queries = (0_u32..512)
            .map(|query_ordinal| V26ExternalQuery {
                query_ordinal,
                vector,
            })
            .collect::<Vec<_>>();
        let truths = (0_u32..512)
            .map(|query_ordinal| V26QueryTruth {
                query_ordinal,
                neighbor_source_ordinals: (0_u64..10).collect(),
                ground_truth_page_assignments: (0_u32..10)
                    .map(|neighbor| vec![neighbor % 8, 8 + neighbor % 8])
                    .collect(),
            })
            .collect::<Vec<_>>();

        let (samples, results) = evaluate_v26_page_mode_router(
            &primary,
            &replica,
            &rows,
            &assignments,
            &queries,
            &truths,
            16,
        )
        .unwrap();

        assert_eq!(results.len(), 4);
        assert_eq!(samples.len(), 4 * 512);
        for (result, mode_count) in results.iter().zip(V26_PAGE_MODE_LADDER) {
            assert_eq!(result.mode_count, mode_count);
            assert_eq!(result.candidate_page_limit, 16);
            assert_eq!(result.aggregate_recall_ppm, 1_000_000);
            assert_eq!(result.minimum_query_recall_ppm, 1_000_000);
            assert_eq!(result.oracle_attainment_ppm, 1_000_000);
            assert!(result.passed);
        }
        assert!(samples.iter().all(|sample| {
            sample.selected_pages.len() == 8
                && sample
                    .selected_pages
                    .windows(2)
                    .all(|pair| pair[0] < pair[1])
        }));
    }

    #[test]
    fn v26_candidate_row_scan_is_frontier_bounded_and_retains_only_ranked_head() {
        // Break caught: the row scan widens past the fixed tree frontier, retains every scored
        // row, or resolves equal distances nondeterministically instead of by source ordinal.
        let primary = v26_router_test_tree(PRIMARY_SEED, 0, [100.0, 10.0, 1.0, 2.0, 5.0, 1.0, 2.0]);
        let replica = v26_router_test_tree(REPLICA_SEED, 8, [200.0, 20.0, 3.0, 4.0, 5.0, 1.0, 2.0]);
        let mut query = [0.0_f32; 96];
        query[0] = 1.0;
        let rows = (0_u64..128)
            .map(|source_ordinal| V26ConstructionRow {
                source_ordinal,
                vector: query,
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..128)
            .map(|source_ordinal| V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal % 8).unwrap(),
                replica_page: 8 + u32::try_from(source_ordinal % 8).unwrap(),
            })
            .collect::<Vec<_>>();

        let ranked =
            rank_v26_candidate_rows(&primary, &replica, &rows, &assignments, &query, 16, 32)
                .unwrap();

        assert_eq!(ranked.len(), 32);
        assert_eq!(
            ranked
                .iter()
                .map(|row| row.source_ordinal)
                .collect::<Vec<_>>(),
            (0_u64..32).collect::<Vec<_>>()
        );
        assert!(ranked.iter().all(|row| row.distance.to_bits() == 0));
    }

    #[test]
    fn v26_candidate_row_cover_uses_row_identity_and_exact_ten_page_cover() {
        // Break caught: candidate rows are reduced to independent page scores, partner pages
        // escape the frontier, or truth participates before the eight pages are persisted.
        let primary = v26_router_test_tree(PRIMARY_SEED, 0, [100.0, 10.0, 1.0, 2.0, 5.0, 1.0, 2.0]);
        let replica = v26_router_test_tree(REPLICA_SEED, 8, [200.0, 20.0, 3.0, 4.0, 5.0, 1.0, 2.0]);
        let mut vector = [0.0_f32; 96];
        vector[0] = 1.0;
        let rows = (0_u64..128)
            .map(|source_ordinal| V26ConstructionRow {
                source_ordinal,
                vector,
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..128)
            .map(|source_ordinal| V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal % 8).unwrap(),
                replica_page: 8 + u32::try_from(source_ordinal % 8).unwrap(),
            })
            .collect::<Vec<_>>();
        let queries = (0_u32..512)
            .map(|query_ordinal| V26ExternalQuery {
                query_ordinal,
                vector,
            })
            .collect::<Vec<_>>();
        let truths = (0_u32..512)
            .map(|query_ordinal| V26QueryTruth {
                query_ordinal,
                neighbor_source_ordinals: (0_u64..10).collect(),
                ground_truth_page_assignments: (0_u32..10)
                    .map(|neighbor| vec![neighbor % 8, 8 + neighbor % 8])
                    .collect(),
            })
            .collect::<Vec<_>>();

        let (samples, result) = evaluate_v26_candidate_row_cover(
            &primary,
            &replica,
            &rows,
            &assignments,
            &queries,
            &truths,
            (16, 10),
        )
        .unwrap();

        assert_eq!(result.schema, "borsuk-v26-candidate-row-cover-result-v1");
        assert_eq!(result.aggregate_recall_ppm, 1_000_000);
        assert_eq!(result.minimum_query_recall_ppm, 1_000_000);
        assert_eq!(result.oracle_attainment_ppm, 1_000_000);
        assert_eq!(result.disposition, V26Disposition::BoundedLayoutCandidate);
        assert!(
            samples
                .iter()
                .all(|sample| sample.selected_pages.len() == 10)
        );
    }

    #[test]
    fn v26_pq8_page_major_projection_fits_the_complete_resident_gate() {
        // Break caught: projection omits mirrored occurrences, offsets, codebook, or runtime
        // reserve and falsely admits a representation above the three-GiB serving gate.
        assert_eq!(
            projected_v26_pq8_resident_bytes(100_000_000, 2_816).unwrap(),
            2_937_537_416
        );
        assert!(
            projected_v26_pq8_resident_bytes(100_000_000, 2_816).unwrap()
                <= 3 * 1_024 * 1_024 * 1_024
        );
    }

    #[test]
    fn v26_pq8_candidate_scan_scores_each_mirrored_row_once_and_keeps_ten() {
        // Break caught: both page-major copies enter the ranking, a partner outside the fixed
        // frontier is dropped, or the hot path retains more than the bounded top ten.
        let tables = std::array::from_fn(|subspace| {
            std::array::from_fn(|code| (subspace * 256 + code) as f32)
        });
        let mut pages = BTreeMap::new();
        for row in 0_u8..12 {
            let left = u32::from(row % 3);
            let right = 3 + u32::from(row % 3);
            let code = [row; 8];
            pages
                .entry(left)
                .or_insert_with(Vec::new)
                .push(V26Pq8Occurrence {
                    code,
                    partner_page: right,
                });
            pages
                .entry(right)
                .or_insert_with(Vec::new)
                .push(V26Pq8Occurrence {
                    code,
                    partner_page: left,
                });
        }

        let ranked = rank_v26_pq8_occurrences(&pages, &[0, 1, 2, 3, 4, 5], &tables).unwrap();

        assert_eq!(ranked.len(), 10);
        assert_eq!(
            ranked.iter().map(|row| row.distance).collect::<Vec<_>>(),
            (0_u8..10)
                .map(|code| (0..8)
                    .map(|subspace| (subspace * 256 + code as usize) as f32)
                    .sum())
                .collect::<Vec<f32>>()
        );
        assert!(ranked.iter().all(|row| row.pages[0] < row.pages[1]));
    }

    #[test]
    fn v26_pq8_fit_is_deterministic_and_encodes_exactly_eight_adc_bytes() {
        // Break caught: fitting depends on scheduling/order, code width drifts, or query ADC
        // uses a different centroid geometry from construction encoding.
        let rows = (0..512)
            .map(|row| {
                let mut vector = std::array::from_fn(|dimension| {
                    (((row * 131 + dimension * 17 + 11) % 257) as f32 - 128.0) / 128.0
                });
                let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
                vector.iter_mut().for_each(|value| *value /= norm);
                vector
            })
            .collect::<Vec<[f32; 96]>>();
        let first = fit_v26_pq8_codebook(&rows).unwrap();
        let second = fit_v26_pq8_codebook(&rows).unwrap();
        assert_eq!(first, second);

        let near = first.encode(&rows[42]).unwrap();
        let far = first.encode(&rows[43]).unwrap();
        let tables = prepare_v26_pq8_tables(&first, &rows[42]).unwrap();
        let score = |code: [u8; 8]| {
            code.iter()
                .enumerate()
                .map(|(subspace, code)| tables[subspace][usize::from(*code)])
                .sum::<f32>()
        };
        assert_eq!(near.len(), 8);
        assert!(score(near) <= score(far));
    }

    #[test]
    fn v26_pq_width_ladder_has_exact_codes_and_monotonic_resident_projection() {
        // Break caught: the diagnostic silently tunes widths, emitted code width differs, or
        // projection omits the bytes that force a serving-memory tradeoff.
        let rows = (0..512)
            .map(|row| {
                let mut vector = std::array::from_fn(|dimension| {
                    (((row * 131 + dimension * 17 + 11) % 257) as f32 - 128.0) / 128.0
                });
                let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
                vector.iter_mut().for_each(|value| *value /= norm);
                vector
            })
            .collect::<Vec<[f32; 96]>>();
        assert_eq!(V26_PQ_WIDTH_LADDER, [8, 16, 24, 32]);
        let expected = [
            2_937_537_416_u64,
            4_537_537_416,
            6_137_537_416,
            7_737_537_416,
        ];
        for (width, expected_bytes) in V26_PQ_WIDTH_LADDER.into_iter().zip(expected) {
            let codebook = fit_v26_pq_codebook(&rows, width).unwrap();
            assert_eq!(codebook.encode(&rows[42]).unwrap().len(), width);
            assert_eq!(
                projected_v26_pq_resident_bytes(100_000_000, 2_816, width).unwrap(),
                expected_bytes
            );
        }
    }

    #[test]
    fn v26_pq_width_ladder_evaluates_one_frozen_query_and_cover_contract() {
        // Break caught: width arms use different frontiers/reducers/truth timing or omit exact
        // eight-page evidence, making the fidelity curve causally incomparable.
        let primary = v26_router_test_tree(PRIMARY_SEED, 0, [100.0, 10.0, 1.0, 2.0, 5.0, 1.0, 2.0]);
        let replica = v26_router_test_tree(REPLICA_SEED, 8, [200.0, 20.0, 3.0, 4.0, 5.0, 1.0, 2.0]);
        let rows = (0_u64..512)
            .map(|source_ordinal| {
                let angle = source_ordinal as f32 / 1_024.0;
                let mut vector = [0.0_f32; 96];
                vector[0] = angle.cos();
                vector[1] = angle.sin();
                V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..512)
            .map(|source_ordinal| V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal % 8).unwrap(),
                replica_page: 8 + u32::try_from(source_ordinal % 8).unwrap(),
            })
            .collect::<Vec<_>>();
        let queries = (0_u32..512)
            .map(|query_ordinal| V26ExternalQuery {
                query_ordinal,
                vector: rows[0].vector,
            })
            .collect::<Vec<_>>();
        let truths = (0_u32..512)
            .map(|query_ordinal| V26QueryTruth {
                query_ordinal,
                neighbor_source_ordinals: (0_u64..10).collect(),
                ground_truth_page_assignments: (0_u64..10)
                    .map(|source_ordinal| {
                        vec![
                            u32::try_from(source_ordinal % 8).unwrap(),
                            8 + u32::try_from(source_ordinal % 8).unwrap(),
                        ]
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();

        let arms = evaluate_v26_pq_width_ladder(
            &primary,
            &replica,
            &rows,
            &assignments,
            &queries,
            &truths,
            16,
        )
        .unwrap();

        assert_eq!(
            arms.iter().map(|arm| arm.code_width).collect::<Vec<_>>(),
            [8, 16, 24, 32]
        );
        assert!(arms.iter().all(|arm| {
            arm.samples.len() == 512
                && arm
                    .samples
                    .iter()
                    .all(|sample| sample.selected_pages.len() == 8)
        }));
    }

    #[test]
    fn v26_pq16_exact_rerank_ladder_preserves_one_resident_index_and_fixed_depths() {
        // Break caught: top-L arms use different PQ training/frontiers, retain unbounded state,
        // join truth before page selection, or silently omit the exact rerank.
        let primary = v26_router_test_tree(PRIMARY_SEED, 0, [100.0, 10.0, 1.0, 2.0, 5.0, 1.0, 2.0]);
        let replica = v26_router_test_tree(REPLICA_SEED, 8, [200.0, 20.0, 3.0, 4.0, 5.0, 1.0, 2.0]);
        let rows = (0_u64..2_113)
            .map(|source_ordinal| {
                let angle = source_ordinal as f32 / 2_048.0;
                let mut vector = [0.0_f32; 96];
                vector[0] = angle.cos();
                vector[1] = angle.sin();
                V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..2_113)
            .map(|source_ordinal| V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal % 8).unwrap(),
                replica_page: 8 + u32::try_from(source_ordinal % 8).unwrap(),
            })
            .collect::<Vec<_>>();
        let queries = (0_u32..512)
            .map(|query_ordinal| V26ExternalQuery {
                query_ordinal,
                vector: rows[usize::try_from(query_ordinal).unwrap()].vector,
            })
            .collect::<Vec<_>>();
        let truths = (0_u32..512)
            .map(|query_ordinal| {
                let start = u64::from(query_ordinal);
                V26QueryTruth {
                    query_ordinal,
                    neighbor_source_ordinals: (start..start + 10).collect(),
                    ground_truth_page_assignments: (start..start + 10)
                        .map(|source_ordinal| {
                            vec![
                                u32::try_from(source_ordinal % 8).unwrap(),
                                8 + u32::try_from(source_ordinal % 8).unwrap(),
                            ]
                        })
                        .collect(),
                }
            })
            .collect::<Vec<_>>();

        let arms = evaluate_v26_pq16_exact_rerank_ladder(
            &primary,
            &replica,
            &rows,
            &assignments,
            &queries,
            &truths,
            16,
        )
        .unwrap();

        assert_eq!(
            arms.iter()
                .map(|arm| arm.ranked_row_limit)
                .collect::<Vec<_>>(),
            [10, 32, 128, 512, 2_048]
        );
        assert!(arms.iter().all(|arm| {
            arm.projected_resident_bytes_100m == 2_937_537_416
                && arm.samples.len() == 512
                && arm
                    .samples
                    .iter()
                    .all(|sample| sample.selected_pages.len() == 10)
        }));
    }

    #[test]
    fn v26_fast_pq16_packed_index_deduplicates_postings_and_matches_reference_top512() {
        // Break caught: production duplicates codes per page, allocates a corpus-sized query
        // marker, scores dual-page rows twice, or changes deterministic PQ ranking.
        let rows = (0_u64..2_113)
            .map(|source_ordinal| {
                let angle = source_ordinal as f32 / 2_048.0;
                let mut vector = [0.0_f32; 96];
                vector[0] = angle.cos();
                vector[1] = angle.sin();
                V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..2_113)
            .map(|source_ordinal| V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal % 8).unwrap(),
                replica_page: 8 + u32::try_from(source_ordinal % 8).unwrap(),
            })
            .collect::<Vec<_>>();
        let index = build_v26_pq16_packed_index(&rows, &assignments).unwrap();
        assert_eq!(index.codes.len(), rows.len() * 16);
        assert_eq!(index.posting_rows.len(), rows.len() * 2);
        assert_eq!(index.page_offsets.len(), 17);
        assert_eq!(index.projected_resident_bytes_100m, 2_937_537_416);

        let candidate_pages = (0_u32..16).collect::<Vec<_>>();
        let packed =
            rank_v26_pq16_packed_candidates(&index, &candidate_pages, &rows[42].vector, 512)
                .unwrap();
        let linear = rank_v26_pq16_linear_occurrence_candidates(
            &index,
            &candidate_pages,
            &rows[42].vector,
            512,
        )
        .unwrap();
        let parallel = rank_v26_pq16_parallel_occurrence_candidates(
            &index,
            &candidate_pages,
            &rows[42].vector,
            512,
        )
        .unwrap();
        assert_eq!(linear, packed);
        assert_eq!(parallel, linear);
        assert_eq!(packed.len(), 512);
        assert_eq!(
            packed
                .iter()
                .map(|row| row.source_ordinal)
                .collect::<BTreeSet<_>>()
                .len(),
            512
        );
        assert!(packed.windows(2).all(|pair| pair[0] <= pair[1]));
        let codes = index
            .codes
            .as_chunks::<16>()
            .0
            .iter()
            .map(|code| code.to_vec())
            .collect::<Vec<_>>();
        let tables = prepare_v26_pq_tables(&index.codebook, &rows[42].vector).unwrap();
        let reference =
            rank_v26_pq16_candidate_rows(&codes, &assignments, &candidate_pages, &tables).unwrap();
        assert_eq!(
            packed
                .iter()
                .map(|row| (row.source_ordinal, row.distance.to_bits()))
                .collect::<Vec<_>>(),
            reference
                .iter()
                .take(512)
                .map(|row| (row.source_ordinal, row.distance.to_bits()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn v26_fast_global_pq16_scans_each_row_once_and_ignores_tree_postings() {
        // Break caught: the router-free quality gate still depends on a tree frontier, scans
        // mirrored posting occurrences, or changes deterministic (distance, ordinal) ranking.
        let codebook = super::V26PqCodebook {
            width: 16,
            subspace_width: 6,
            centroids: (0..16)
                .map(|subspace| {
                    (0..256)
                        .flat_map(|centroid| {
                            (0..6).map(move |dimension| {
                                (centroid * 17 + subspace * 11 + dimension) as f32 / 4_096.0
                            })
                        })
                        .collect()
                })
                .collect(),
        };
        let codes = (0..4_096)
            .flat_map(|row| (0..16).map(move |subspace| ((row * 37 + subspace * 19) % 256) as u8))
            .collect::<Vec<_>>();
        let mut index = super::V26PackedPq16Index {
            codebook,
            codes,
            page_offsets: vec![u64::MAX; 17],
            posting_rows: vec![u32::MAX; 8_192],
            projected_resident_bytes_100m: 2_937_537_416,
        };
        let mut query = [0.0_f32; 96];
        query[0] = 1.0;
        let tables = prepare_v26_pq_tables(&index.codebook, &query).unwrap();
        let mut reference = index
            .codes
            .as_chunks::<16>()
            .0
            .iter()
            .enumerate()
            .map(|(source_ordinal, code)| super::V26PqRankedRow {
                source_ordinal: u64::try_from(source_ordinal).unwrap(),
                distance: code
                    .iter()
                    .enumerate()
                    .map(|(subspace, code)| tables[subspace][usize::from(*code)])
                    .sum(),
            })
            .collect::<Vec<_>>();
        reference.sort();

        index.page_offsets.reverse();
        index.posting_rows.reverse();
        let actual = rank_v26_pq16_global_candidates(&index, &query, 2_048).unwrap();

        assert_eq!(actual.len(), 2_048);
        assert_eq!(
            actual
                .iter()
                .map(|row| (row.source_ordinal, row.distance.to_bits()))
                .collect::<Vec<_>>(),
            reference
                .iter()
                .take(2_048)
                .map(|row| (row.source_ordinal, row.distance.to_bits()))
                .collect::<Vec<_>>()
        );

        let cold_rows = (0_u64..4_096)
            .map(|source_ordinal| V26ConstructionRow {
                source_ordinal,
                vector: query,
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..4_096)
            .map(|source_ordinal| V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal % 10).unwrap(),
                replica_page: 10 + u32::try_from(source_ordinal % 10).unwrap(),
            })
            .collect::<Vec<_>>();
        let selection =
            select_v26_pq16_global_packed_pages(&index, &query, &cold_rows, &assignments, 2_048)
                .unwrap();
        assert_eq!(
            selection.selected_pages.len(),
            super::V26_SERVING_PAGE_BUDGET
        );
        assert_eq!(selection.exact_rows_read, 2_048);
        assert_eq!(selection.page_body_reads, 0);
    }

    #[test]
    fn v26_fast_simhash_pq16_multi_index_matches_global_ranking_without_page_postings() {
        // Break caught: the fail-fast router loses row identity, depends on the page tree, or
        // cannot reproduce the global PQ16 order when every registered bucket is searched.
        let rows = (0_u64..4_096)
            .map(|source_ordinal| {
                let mut row = row(source_ordinal);
                let norm = row
                    .vector
                    .iter()
                    .map(|coordinate| coordinate * coordinate)
                    .sum::<f32>()
                    .sqrt();
                row.vector
                    .iter_mut()
                    .for_each(|coordinate| *coordinate /= norm);
                row
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..4_096)
            .map(|source_ordinal| V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal % 16).unwrap(),
                replica_page: 16 + u32::try_from(source_ordinal % 16).unwrap(),
            })
            .collect::<Vec<_>>();
        let mut packed = build_v26_pq16_packed_index(&rows, &assignments).unwrap();
        packed.page_offsets.fill(u64::MAX);
        packed.posting_rows.fill(u32::MAX);
        let query = rows[42].vector;
        let expected = rank_v26_pq16_global_candidates(&packed, &query, 512).unwrap();

        let multi = super::build_v26_simhash_pq16_multi_index(&packed, &rows).unwrap();
        let actual = super::rank_v26_simhash_pq16_candidates(&multi, &query, 65_536, 512).unwrap();

        assert_eq!(actual, expected);
        assert_eq!(multi.bucket_offsets.len(), 65_537);
        assert_eq!(multi.source_ordinals.len(), rows.len());
        assert_eq!(multi.codes.len(), rows.len() * 16);
        assert_eq!(multi.projected_resident_bytes_100m, 2_537_493_520);
        assert!(multi.projected_resident_bytes_100m < 3 * 1_024_u64.pow(3));
    }

    #[test]
    fn v26_fast_dual_pq_key_index_matches_global_ranking_and_memory_contract() {
        // Break caught: the distance-aligned router drops rows, depends on tree postings,
        // changes deterministic full-PQ ranking, or exceeds the 3 GiB serving ceiling.
        let codebook = super::V26PqCodebook {
            width: 16,
            subspace_width: 6,
            centroids: (0..16)
                .map(|subspace| {
                    (0..256)
                        .flat_map(|centroid| {
                            (0..6).map(move |dimension| {
                                (centroid * 17 + subspace * 11 + dimension) as f32 / 4_096.0
                            })
                        })
                        .collect()
                })
                .collect(),
        };
        let codes = (0..4_096)
            .flat_map(|row| (0..16).map(move |subspace| ((row * 37 + subspace * 19) % 256) as u8))
            .collect::<Vec<_>>();
        let packed = super::V26PackedPq16Index {
            codebook,
            codes,
            page_offsets: vec![u64::MAX; 17],
            posting_rows: vec![u32::MAX; 8_192],
            projected_resident_bytes_100m: 2_937_537_416,
        };
        let mut query = [0.0_f32; 96];
        query[0] = (15.0_f32 / 16.0).sqrt();
        query[48] = -0.25;
        let expected = rank_v26_pq16_global_candidates(&packed, &query, 512).unwrap();

        let dual = super::build_v26_dual_pq_key_index(&packed).unwrap();
        let actual = super::rank_v26_dual_pq_key_candidates(&dual, &query, 65_536, 512).unwrap();

        assert_eq!(actual, expected);
        assert_eq!(dual.bucket_offsets[0].len(), 65_537);
        assert_eq!(dual.bucket_offsets[1].len(), 65_537);
        assert_eq!(dual.source_ordinals[0].len(), 4_096);
        assert_eq!(dual.source_ordinals[1].len(), 4_096);
        for plane in 0..2 {
            for bounds in dual.bucket_offsets[plane].windows(2) {
                let start = usize::try_from(bounds[0]).unwrap();
                let end = usize::try_from(bounds[1]).unwrap();
                assert!(
                    dual.source_ordinals[plane][start..end]
                        .windows(2)
                        .all(|pair| pair[0] < pair[1])
                );
            }
        }
        assert_eq!(dual.projected_resident_bytes_100m, 2_938_017_816);
        assert!(dual.projected_resident_bytes_100m < 3 * 1_024_u64.pow(3));
    }

    #[test]
    fn v26_pq16_serving_kernel_reads_exactly_512_cold_vectors_and_selects_ten_pages() {
        // Break caught: serving exact-reranks an unbounded set, consults truth, reads page bodies,
        // or changes the fixed depth-512/ten-page contract.
        let rows = (0_u64..2_113)
            .map(|source_ordinal| {
                let angle = source_ordinal as f32 / 2_048.0;
                let mut vector = [0.0_f32; 96];
                vector[0] = angle.cos();
                vector[1] = angle.sin();
                V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..2_113)
            .map(|source_ordinal| V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal % 8).unwrap(),
                replica_page: 8 + u32::try_from(source_ordinal % 8).unwrap(),
            })
            .collect::<Vec<_>>();
        let index = build_v26_pq16_packed_index(&rows, &assignments).unwrap();
        let result = select_v26_pq16_packed_pages(
            &index,
            &(0_u32..16).collect::<Vec<_>>(),
            &rows[42].vector,
            &rows,
            &assignments,
        )
        .unwrap();
        assert_eq!(result.exact_rows_read, 512);
        assert_eq!(result.selected_pages.len(), super::V26_SERVING_PAGE_BUDGET);
        assert!(
            result
                .selected_pages
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        );
        assert_eq!(result.page_body_reads, 0);
    }

    #[test]
    fn v26_pq8_page_occurrences_bind_both_assignments_without_row_id_storage() {
        // Break caught: materialization stores a separate row ID, omits one assignment, changes
        // page order, or encodes the same row differently in its mirrored occurrence.
        let rows = (0_u64..512)
            .map(|source_ordinal| {
                let angle = source_ordinal as f32 / 1_024.0;
                let mut vector = [0.0_f32; 96];
                vector[0] = angle.cos();
                vector[1] = angle.sin();
                V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..512)
            .map(|source_ordinal| V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal % 8).unwrap(),
                replica_page: 8 + u32::try_from(source_ordinal % 8).unwrap(),
            })
            .collect::<Vec<_>>();
        let vectors = rows.iter().map(|row| row.vector).collect::<Vec<_>>();
        let codebook = fit_v26_pq8_codebook(&vectors).unwrap();

        let pages = build_v26_pq8_page_occurrences(&rows, &assignments, &codebook).unwrap();

        assert_eq!(pages.len(), 16);
        assert_eq!(pages.values().map(Vec::len).sum::<usize>(), 1_024);
        for row in 0..512_usize {
            let primary = assignments[row].primary_page;
            let replica = assignments[row].replica_page;
            let primary_occurrence = &pages[&primary][row / 8];
            let replica_occurrence = &pages[&replica][row / 8];
            assert_eq!(primary_occurrence.partner_page, replica);
            assert_eq!(replica_occurrence.partner_page, primary);
            assert_eq!(primary_occurrence.code, replica_occurrence.code);
        }
    }

    #[test]
    fn v26_pq8_candidate_cover_selects_exactly_eight_pages_before_truth_join() {
        // Break caught: truth affects selection, PQ occurrences are reduced to page scores, or
        // the serving request persists fewer than the exact eight-page budget.
        let primary = v26_router_test_tree(PRIMARY_SEED, 0, [100.0, 10.0, 1.0, 2.0, 5.0, 1.0, 2.0]);
        let replica = v26_router_test_tree(REPLICA_SEED, 8, [200.0, 20.0, 3.0, 4.0, 5.0, 1.0, 2.0]);
        let rows = (0_u64..512)
            .map(|source_ordinal| {
                let angle = source_ordinal as f32 / 1_024.0;
                let mut vector = [0.0_f32; 96];
                vector[0] = angle.cos();
                vector[1] = angle.sin();
                V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..512)
            .map(|source_ordinal| V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal % 8).unwrap(),
                replica_page: 8 + u32::try_from(source_ordinal % 8).unwrap(),
            })
            .collect::<Vec<_>>();
        let queries = (0_u32..512)
            .map(|query_ordinal| V26ExternalQuery {
                query_ordinal,
                vector: rows[0].vector,
            })
            .collect::<Vec<_>>();
        let truth = |offset: u64| {
            (0_u32..512)
                .map(|query_ordinal| V26QueryTruth {
                    query_ordinal,
                    neighbor_source_ordinals: (offset..offset + 10).collect(),
                    ground_truth_page_assignments: (offset..offset + 10)
                        .map(|source_ordinal| {
                            vec![
                                u32::try_from(source_ordinal % 8).unwrap(),
                                8 + u32::try_from(source_ordinal % 8).unwrap(),
                            ]
                        })
                        .collect(),
                })
                .collect::<Vec<_>>()
        };

        let (first, result) = evaluate_v26_pq8_candidate_cover(
            &primary,
            &replica,
            &rows,
            &assignments,
            &queries,
            &truth(0),
            16,
        )
        .unwrap();
        let (second, _) = evaluate_v26_pq8_candidate_cover(
            &primary,
            &replica,
            &rows,
            &assignments,
            &queries,
            &truth(10),
            16,
        )
        .unwrap();

        assert_eq!(result.schema, "borsuk-v26-pq8-candidate-cover-result-v1");
        assert_eq!(result.page_body_reads, 0);
        assert_eq!(
            first
                .iter()
                .map(|sample| &sample.selected_pages)
                .collect::<Vec<_>>(),
            second
                .iter()
                .map(|sample| &sample.selected_pages)
                .collect::<Vec<_>>()
        );
        assert!(first.iter().all(|sample| sample.selected_pages.len() == 8));
    }

    #[test]
    fn v26_exact_global_cumulative_rank_evidence_replaces_rank_sharp_minimum() {
        // Break caught: a page is represented only by its nearest row, so repeated later
        // evidence cannot promote a coherent page above isolated early rows.
        let ranked = (0_u64..12)
            .map(|source_ordinal| V26RankedRow {
                source_ordinal,
                distance: source_ordinal as f32,
            })
            .collect::<Vec<_>>();
        let assignments = ranked
            .iter()
            .map(|row| V26RowPages {
                source_ordinal: row.source_ordinal,
                primary_page: match row.source_ordinal {
                    0 => 99,
                    1 => 8,
                    _ => 7,
                },
                replica_page: 100 + u32::try_from(row.source_ordinal).unwrap(),
            })
            .map(|row| (row.source_ordinal, row))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            select_v26_ranked_pages(&ranked, &assignments, 8).unwrap(),
            vec![7, 99, 100, 8, 101, 102, 103, 104]
        );
    }

    #[test]
    fn v26_external_query_exact_global_has_no_own_row_or_page_exclusion() {
        // Break caught: a production query is assigned a construction source/page identity and
        // the exact-global ceiling silently discards valid nearest neighbors on those pages.
        let rows = (0_u64..12)
            .map(|source_ordinal| {
                let mut vector = [0.0_f32; 96];
                vector[0] = 1.0;
                V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..12)
            .map(|source_ordinal| V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal).unwrap(),
                replica_page: u32::try_from(source_ordinal).unwrap() + 32,
            })
            .collect::<Vec<_>>();
        let mut vector = [0.0_f32; 96];
        vector[0] = 1.0;
        let queries = [V26ExternalQuery {
            query_ordinal: 0,
            vector,
        }];
        let truths = [V26QueryTruth {
            query_ordinal: 0,
            neighbor_source_ordinals: (0_u64..10).collect(),
            ground_truth_page_assignments: (0_u32..10).map(|page| vec![page, page + 32]).collect(),
        }];
        let limits = [10, 32, 128, 512, 2_048, 4_096];

        let samples = evaluate_v26_exact_global_external_rows(
            &rows,
            &assignments,
            &queries,
            &truths,
            &limits,
            8,
        )
        .unwrap();

        assert_eq!(samples.len(), 6);
        for sample in samples {
            assert_eq!(sample.candidate_rows, 12);
            assert_eq!(sample.selected_pages, vec![0, 1, 2, 3, 32, 33, 34, 35]);
            assert_eq!(sample.hits, 4);
            assert_eq!(sample.oracle_hits, 8);
            assert_eq!(sample.first_ten_ranked_rows[0].source_ordinal, 0);
        }
    }

    #[test]
    fn v26_external_query_truth_exactly_ranks_construction_without_layout_capability() {
        // Break caught: truth depends on a page layout, keeps nondeterministic ties, or omits
        // the exact f32 evidence needed to authenticate later recall evaluation.
        let rows = (0_u64..12)
            .map(|source_ordinal| {
                let mut vector = [0.0_f32; 96];
                vector[0] = 1.0;
                V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let mut vector = [0.0_f32; 96];
        vector[0] = 1.0;
        let queries = [V26ExternalQuery {
            query_ordinal: 0,
            vector,
        }];

        let truth = build_v26_external_truth_rows(&rows, &queries).unwrap();

        assert_eq!(
            truth,
            vec![V26ExternalTruth {
                query_ordinal: 0,
                neighbor_source_ordinals: (0_u64..10).collect(),
                neighbor_distance_bits: vec![0_f32.to_bits(); 10],
            }]
        );
        let mut reversed = rows;
        reversed.reverse();
        assert_eq!(
            build_v26_external_truth_rows(&reversed, &queries).unwrap(),
            truth
        );
    }

    fn authority(expected_rows: u64) -> V26LayoutAuthority {
        V26LayoutAuthority {
            schema: "borsuk-v26-dual-tree-layout-v2".to_owned(),
            generation: "v26-test-generation".to_owned(),
            source_commit: "1".repeat(40),
            source_archive_sha256: "2".repeat(64),
            binary: identity("v26-layout-binary", '9', 4096),
            construction_rows: identity("construction-parquet", '3', 1024),
            primary_seed: PRIMARY_SEED,
            replica_seed: REPLICA_SEED,
            page_capacity: 704,
            expected_rows,
        }
    }

    fn identity(role: &str, marker: char, encoded_bytes: u64) -> V26ObjectIdentity {
        V26ObjectIdentity {
            role: role.to_owned(),
            uri: format!("s3://v26-test/{role}"),
            digest_algorithm: "sha256".to_owned(),
            digest: marker.to_string().repeat(64),
            encoded_bytes,
            generation: "v26-test-generation".to_owned(),
        }
    }

    #[test]
    fn v26_layout_authority_uses_one_construction_parquet_without_redundant_source_map() {
        // Break caught: full-scale construction requires a derived ordinal-map artifact even
        // though construction.parquet already owns the complete ordered source inventory.
        let mut value = serde_json::to_value(authority(1_409)).unwrap();
        let object = value.as_object_mut().unwrap();
        object.insert(
            "schema".to_owned(),
            serde_json::Value::String("borsuk-v26-dual-tree-layout-v2".to_owned()),
        );
        object.remove("source_map");

        let decoded: V26LayoutAuthority = serde_json::from_value(value).unwrap();
        super::validate_layout_authority(&decoded).unwrap();
    }

    #[test]
    fn v26_tree_balances_aligned_leaves_and_is_byte_deterministic() {
        // Break caught: unstable worker scheduling, an unaligned split, or page-range overlap.
        let rows = (0..1_409).map(row).collect::<Vec<_>>();
        let authority = authority(rows.len() as u64);

        let one = build_v26_dual_tree_layout(&authority, &rows).unwrap();
        let repeated = build_v26_dual_tree_layout(&authority, &rows).unwrap();
        assert_eq!(
            serde_json::to_vec(&one).unwrap(),
            serde_json::to_vec(&repeated).unwrap()
        );

        let (primary, replica, assignments) = one;
        assert_eq!(assignments.len(), 1_409);
        assert_eq!(
            assignments
                .iter()
                .map(|assignment| assignment.source_ordinal)
                .collect::<BTreeSet<_>>(),
            (0..1_409).collect()
        );

        let primary_counts = assignments.iter().fold(BTreeMap::new(), |mut counts, row| {
            *counts.entry(row.primary_page).or_insert(0_usize) += 1;
            counts
        });
        let replica_counts = assignments.iter().fold(BTreeMap::new(), |mut counts, row| {
            *counts.entry(row.replica_page).or_insert(0_usize) += 1;
            counts
        });
        assert_eq!(
            primary_counts.keys().copied().collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            replica_counts.keys().copied().collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
        assert!(primary_counts.values().all(|count| *count <= 704));
        assert!(replica_counts.values().all(|count| *count <= 704));
        assert!(
            assignments
                .iter()
                .all(|assignment| assignment.primary_page != assignment.replica_page)
        );
        assert_eq!(primary.seed, PRIMARY_SEED);
        assert_eq!(replica.seed, REPLICA_SEED);
        assert!(primary.nodes.iter().all(|node| node.threshold.is_finite()));
        assert!(replica.nodes.iter().all(|node| node.threshold.is_finite()));
        validate_v26_dual_tree_layout(&authority, &primary, &replica, &assignments).unwrap();

        let mut invalid = assignments.clone();
        invalid[0].replica_page = invalid[0].primary_page;
        assert!(validate_v26_dual_tree_layout(&authority, &primary, &replica, &invalid).is_err());
    }

    #[test]
    fn v26_tree_records_zero_gap_plateaus_without_losing_assignment_authority() {
        // Break caught: an unrecorded score plateau makes later best-first routing ambiguous.
        let rows = (0..705)
            .map(|source_ordinal| V26ConstructionRow {
                source_ordinal,
                vector: [0.125; 96],
            })
            .collect::<Vec<_>>();
        let authority = authority(rows.len() as u64);
        let (primary, replica, assignments) =
            build_v26_dual_tree_layout(&authority, &rows).unwrap();
        assert_eq!(primary.nodes[0].split_gap, 0.0);
        assert_eq!(replica.nodes[0].split_gap, 0.0);
        assert_eq!(assignments.len(), 705);
        validate_v26_dual_tree_layout(&authority, &primary, &replica, &assignments).unwrap();
    }

    #[test]
    fn v26_tree_accepts_only_the_preregistered_page_capacity_ladder() {
        // Break caught: the open-screen capacity ladder is hard-coded away or widened ad hoc.
        let rows = (0..705).map(row).collect::<Vec<_>>();
        for page_capacity in [704, 768, 896, 1_024, 1_408, 2_048, 2_816, 4_096, 8_192] {
            let mut candidate = authority(rows.len() as u64);
            candidate.page_capacity = page_capacity;
            let (primary, replica, assignments) =
                build_v26_dual_tree_layout(&candidate, &rows).unwrap();
            validate_v26_dual_tree_layout(&candidate, &primary, &replica, &assignments).unwrap();
        }
        for page_capacity in [0, 512, 2_049, 2_817, 16_384] {
            let mut candidate = authority(rows.len() as u64);
            candidate.page_capacity = page_capacity;
            assert!(build_v26_dual_tree_layout(&candidate, &rows).is_err());
        }
        assert_eq!(super::projected_steps(1_409, 2, 768).unwrap(), 2_164_224);
    }

    fn receipt() -> V26LayoutReceipt {
        let authority = authority(1_409);
        V26LayoutReceipt {
            inputs: vec![
                authority.construction_rows.clone(),
                identity("layout-manifest", '5', 900),
            ],
            authority,
            outputs: vec![
                identity("page-assignments-parquet", '6', 30_000),
                identity("primary-tree-parquet", '7', 4_000),
                identity("replica-tree-parquet", '8', 4_000),
            ],
            row_count: 1_409,
            leaves_per_tree: 3,
            page_count: 6,
            projection_steps: 6_494_208,
            worker_count: 4,
            elapsed_ns: 2_000_000,
            cpu_ns: 6_000_000,
            peak_rss_bytes: 32 * 1024 * 1024,
            peak_psi_full_avg10_milli_percent: 0,
            swap_start_bytes: 0,
            swap_end_bytes: 0,
            query_role_opens: 0,
            page_body_reads: 0,
            claim_eligible: false,
        }
    }

    #[test]
    fn v26_tree_layout_receipt_recomputes_counts_work_and_identities() {
        // Break caught: accepting incomplete authority, hidden evaluation I/O, or false counts.
        let valid = receipt();
        let bytes = canonical_v26_layout_receipt_bytes(&valid).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert!(!bytes[..bytes.len() - 1].contains(&b'\n'));

        type ReceiptMutation = Box<dyn Fn(&mut V26LayoutReceipt)>;
        let mut mutations: Vec<ReceiptMutation> = vec![
            Box::new(|value| value.authority.binary.digest_algorithm = "blake3".to_owned()),
            Box::new(|value| value.authority.schema.push_str("-drift")),
            Box::new(|value| value.authority.source_commit = "a".repeat(39)),
            Box::new(|value| value.authority.source_archive_sha256 = "g".repeat(64)),
            Box::new(|value| value.authority.primary_seed ^= 1),
            Box::new(|value| value.authority.replica_seed ^= 1),
            Box::new(|value| value.authority.page_capacity = 703),
            Box::new(|value| value.row_count -= 1),
            Box::new(|value| value.leaves_per_tree -= 1),
            Box::new(|value| value.page_count -= 1),
            Box::new(|value| value.projection_steps = 0),
            Box::new(|value| value.worker_count = 0),
            Box::new(|value| value.elapsed_ns = 0),
            Box::new(|value| value.cpu_ns = 0),
            Box::new(|value| value.peak_rss_bytes = 0),
            Box::new(|value| value.peak_psi_full_avg10_milli_percent = 501),
            Box::new(|value| value.swap_end_bytes = 1),
            Box::new(|value| value.query_role_opens = 1),
            Box::new(|value| value.page_body_reads = 1),
            Box::new(|value| value.claim_eligible = true),
            Box::new(|value| value.inputs[0].role = "external-queries-parquet".to_owned()),
            Box::new(|value| value.inputs[0].digest_algorithm = "blake3".to_owned()),
            Box::new(|value| value.inputs[0].digest = "A".repeat(64)),
            Box::new(|value| value.inputs[0].encoded_bytes = 0),
            Box::new(|value| value.outputs.swap(0, 1)),
            Box::new(|value| value.outputs[0].uri = value.inputs[0].uri.clone()),
        ];
        for mutate in mutations.drain(..) {
            let mut candidate = valid.clone();
            mutate(&mut candidate);
            assert!(canonical_v26_layout_receipt_bytes(&candidate).is_err());
        }
    }

    #[test]
    fn v26_layout_oracle_uses_both_pages_and_prefers_shorter_lexicographic_cover() {
        // Break caught: redundant pages displace the shortest complete two-copy cover.
        let assignments = (1_u32..=10).map(|page| vec![0, page]).collect::<Vec<_>>();
        assert_eq!(
            exact_v26_layout_oracle_pages(&assignments, 8).unwrap(),
            vec![0]
        );

        let assignments = vec![
            vec![0, 8],
            vec![1, 8],
            vec![2, 9],
            vec![3, 9],
            vec![4, 10],
            vec![5, 10],
            vec![6, 11],
            vec![7, 11],
            vec![12, 13],
            vec![14, 15],
        ];
        assert_eq!(
            exact_v26_layout_oracle_pages(&assignments, 8).unwrap(),
            vec![8, 9, 10, 11, 12, 14]
        );
    }

    #[test]
    fn v26_fast_layout_oracle_supports_the_frozen_ten_page_serving_budget() {
        // Break caught: the full-scale perfect-recall budget is rejected or truncated at eight.
        let assignments = (0_u32..10)
            .map(|page| vec![page, page + 10])
            .collect::<Vec<_>>();
        assert_eq!(
            exact_v26_layout_oracle_pages(&assignments, 10).unwrap(),
            (0_u32..10).collect::<Vec<_>>()
        );
    }

    #[test]
    fn v26_layout_oracle_result_recomputes_samples_gates_and_disposition() {
        // Break caught: a claimed layout result drifts from its per-query truth authority.
        let truths = (0_u32..512)
            .map(|query_ordinal| {
                let ground_truth_page_assignments = if query_ordinal < 13 {
                    (0_u32..10).map(|page| vec![page]).collect::<Vec<_>>()
                } else {
                    (0_u32..10)
                        .map(|page| vec![0, page + 1])
                        .collect::<Vec<_>>()
                };
                V26QueryTruth {
                    query_ordinal,
                    neighbor_source_ordinals: (0_u64..10)
                        .map(|neighbor| u64::from(query_ordinal) * 10 + neighbor)
                        .collect(),
                    ground_truth_page_assignments,
                }
            })
            .collect::<Vec<_>>();
        let samples = truths
            .iter()
            .map(|truth| {
                let selected_pages =
                    exact_v26_layout_oracle_pages(&truth.ground_truth_page_assignments, 8).unwrap();
                let hits = if truth.query_ordinal < 13 { 8 } else { 10 };
                V26LayoutSample {
                    query_ordinal: truth.query_ordinal,
                    selected_pages,
                    hits,
                    recall_ppm: u64::from(hits) * 100_000,
                }
            })
            .collect::<Vec<_>>();
        let valid = V26LayoutResult {
            schema: "borsuk-v26-layout-result-v1".to_owned(),
            query_count: 512,
            aggregate_recall_ppm: 994_921,
            minimum_query_recall_ppm: 800_000,
            disposition: V26Disposition::LayoutRejected,
            page_body_reads: 0,
            claim_eligible: false,
        };
        let bytes = canonical_v26_layout_result_bytes(&valid, &truths, &samples).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);

        type ResultMutation = Box<dyn Fn(&mut V26LayoutResult, &mut Vec<V26LayoutSample>)>;
        let mut mutations: Vec<ResultMutation> = vec![
            Box::new(|result, _| result.query_count = 511),
            Box::new(|result, _| result.aggregate_recall_ppm += 1),
            Box::new(|result, _| result.minimum_query_recall_ppm += 1),
            Box::new(|result, _| result.disposition = V26Disposition::BoundedLayoutCandidate),
            Box::new(|result, _| result.page_body_reads = 1),
            Box::new(|result, _| result.claim_eligible = true),
            Box::new(|_, rows| rows[0].query_ordinal = 1),
            Box::new(|_, rows| rows[0].selected_pages.swap(0, 1)),
            Box::new(|_, rows| rows[0].hits += 1),
            Box::new(|_, rows| rows[0].recall_ppm += 1),
        ];
        for mutation in mutations.drain(..) {
            let mut result = valid.clone();
            let mut rows = samples.clone();
            mutation(&mut result, &mut rows);
            assert!(canonical_v26_layout_result_bytes(&result, &truths, &rows).is_err());
        }
    }

    #[test]
    fn v26_exact_global_result_recomputes_samples_rank_gates_and_truth_injection() {
        // Break caught: a forged sample, rank aggregate, or claimed disposition is serialized
        // without independent recomputation from query truth and page assignments.
        let rows = (0_u64..32)
            .map(|source_ordinal| {
                let mut vector = [0.0_f32; 96];
                vector[0] = 1.0;
                V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..32)
            .map(|source_ordinal| V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal).unwrap(),
                replica_page: u32::try_from(source_ordinal).unwrap() + 32,
            })
            .collect::<Vec<_>>();
        let mut vector = [0.0_f32; 96];
        vector[0] = 1.0;
        let queries = (0_u32..512)
            .map(|query_ordinal| V26ExternalQuery {
                query_ordinal,
                vector,
            })
            .collect::<Vec<_>>();
        let truths = (0_u32..512)
            .map(|query_ordinal| V26QueryTruth {
                query_ordinal,
                neighbor_source_ordinals: (0_u64..10).collect(),
                ground_truth_page_assignments: (0_u32..10)
                    .map(|page| vec![page, page + 32])
                    .collect(),
            })
            .collect::<Vec<_>>();
        let limits = [10, 32, 128, 512, 2_048, 4_096];
        let samples = evaluate_v26_exact_global_external_rows(
            &rows,
            &assignments,
            &queries,
            &truths,
            &limits,
            8,
        )
        .unwrap();
        let rank_results = limits
            .into_iter()
            .map(|ranked_row_limit| V26ExactGlobalRankResult {
                ranked_row_limit,
                aggregate_recall_ppm: 400_000,
                minimum_query_recall_ppm: 400_000,
                oracle_attainment_ppm: 500_000,
                passed: false,
            })
            .collect();
        let valid = V26ExactGlobalResult {
            schema: "borsuk-v26-cumulative-exact-global-result-v1".to_owned(),
            query_count: 512,
            rank_results,
            disposition: V26Disposition::RankReducerRejected,
            page_body_reads: 0,
            claim_eligible: false,
        };

        let bytes = canonical_v26_exact_global_result_bytes(
            &valid,
            &rows,
            &assignments,
            &queries,
            &truths,
            &samples,
        )
        .unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);

        let mut forged_result = valid.clone();
        forged_result.rank_results[0].aggregate_recall_ppm += 1;
        assert!(
            canonical_v26_exact_global_result_bytes(
                &forged_result,
                &rows,
                &assignments,
                &queries,
                &truths,
                &samples,
            )
            .is_err()
        );
        let mut forged_samples = samples.clone();
        forged_samples[0].first_ten_ranked_rows.swap(0, 1);
        assert!(
            canonical_v26_exact_global_result_bytes(
                &valid,
                &rows,
                &assignments,
                &queries,
                &truths,
                &forged_samples,
            )
            .is_err()
        );
        forged_samples = samples.clone();
        forged_samples[0].hits += 1;
        assert!(
            canonical_v26_exact_global_result_bytes(
                &valid,
                &rows,
                &assignments,
                &queries,
                &truths,
                &forged_samples,
            )
            .is_err()
        );
        forged_samples = samples.clone();
        forged_samples[1].selected_pages = vec![0, 1, 2, 3, 32, 33, 34, 63];
        assert!(
            canonical_v26_exact_global_result_bytes(
                &valid,
                &rows,
                &assignments,
                &queries,
                &truths,
                &forged_samples,
            )
            .is_err()
        );
    }
}

#[cfg(test)]
mod local_schema_tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::{
        v26_construction_schema, v26_page_assignments_schema, v26_query_schema, v26_tree_schema,
        v26_truth_schema,
    };

    #[test]
    fn v26_layout_local_schema_contracts_are_exact_and_nonnullable() {
        // Break caught: cross-language field/type/order/nullability drift.
        let vector = DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
        );
        assert_eq!(
            v26_construction_schema(),
            Schema::new(vec![
                Field::new("source_ordinal", DataType::UInt64, false),
                Field::new("vector", vector, false),
            ])
        );
        assert_eq!(
            v26_tree_schema(),
            Schema::new(vec![
                Field::new("node_ordinal", DataType::UInt32, false),
                Field::new("left", DataType::UInt32, true),
                Field::new("right", DataType::UInt32, true),
                Field::new("direction_ordinal", DataType::UInt8, false),
                Field::new("threshold", DataType::Float32, false),
                Field::new("split_gap", DataType::Float32, false),
                Field::new("leaf_page", DataType::UInt32, true),
            ])
        );
        assert_eq!(
            v26_page_assignments_schema(),
            Schema::new(vec![
                Field::new("source_ordinal", DataType::UInt64, false),
                Field::new("primary_page", DataType::UInt32, false),
                Field::new("replica_page", DataType::UInt32, false),
            ])
        );
        assert_eq!(
            v26_query_schema(),
            Schema::new(vec![Field::new(
                "emb",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float32, false)),
                    96,
                ),
                false,
            )])
        );
        assert_eq!(
            v26_truth_schema(),
            Schema::new(vec![
                Field::new("query_ordinal", DataType::UInt32, false),
                Field::new(
                    "neighbor_source_ordinals",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("element", DataType::UInt64, false)),
                        10,
                    ),
                    false,
                ),
                Field::new(
                    "neighbor_distance_bits",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("element", DataType::UInt32, false)),
                        10,
                    ),
                    false,
                ),
            ])
        );
    }
}
