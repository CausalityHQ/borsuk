use std::{
    cmp::Ordering,
    collections::{BTreeSet, BinaryHeap},
    time::Instant,
};

use serde::{Deserialize, Serialize};

use crate::{
    BorsukError, Result,
    v23_balanced_pages::{V23BalancedArm, V23BalancedPageBudget, V23BalancedSelectedPair},
    v23_balanced_pages_arrow::{V23PageRow, V23SupercellRow, validate_v23_balanced_page_geometry},
    v23_diagnostic::V23PageCoverage,
    v23_incidence_tree::normalize_v23_incidence_vector,
};

const RESULT_SCHEMA: &str = "borsuk-v23-balanced-page-result-v2";
const MAX_SCORED_DIMENSIONS: u64 = 4_000_000;
const MAX_SERVING_BYTES: u64 = 3 * 1024 * 1024 * 1024;
const MAX_SCALAR_SIMD_DISTANCE_DELTA_PPM: u64 = 10;
const MAX_PROJECTED_PAGE_BYTES: u64 = 1_966_080;
const MAX_ENCODED_PAGE_BYTES: u64 = 122_880;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23BalancedPseudoqueryPair {
    pub(crate) selected_pair: V23BalancedSelectedPair,
    pub(crate) aggregate_recall_ppm: u64,
    pub(crate) minimum_recall_ppm: u64,
    pub(crate) oracle_attainment_ppm: u64,
    pub(crate) every_query_has_budget: bool,
    pub(crate) projected_page_bytes: u64,
    pub(crate) maximum_scored_dimensions: u64,
    pub(crate) amplification_and_page_caps_valid: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V23BalancedSelectedPairEvidence {
    pub(crate) selected_pair: V23BalancedSelectedPair,
    pub(crate) official_query_reads: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23BalancedPageSelection {
    pub(crate) pages: Vec<u32>,
    pub(crate) containment_page_universe: Vec<u32>,
    pub(crate) scored_dimensions: u64,
    pub(crate) scalar_simd_pages_equal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23BalancedTimingEvidence {
    pub(crate) warmups: u32,
    pub(crate) samples_ns: Vec<u64>,
    pub(crate) p99_ns: u64,
    pub(crate) pages_by_query: Vec<Vec<u32>>,
    pub(crate) scored_dimensions: u64,
    pub(crate) scalar_simd_pages_equal: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct V23BalancedServingSupercell {
    supercell_ordinal: u32,
    centroid: [f32; 96],
    cosine_radius: f32,
    first_page: u32,
    page_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
struct V23BalancedServingPage {
    centroid: [f32; 96],
    cosine_radius: f32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23BalancedServingGeometry {
    selected_arm: V23BalancedArm,
    maximum_replica_rows: u16,
    total_primary_rows: u64,
    total_replica_rows: u64,
    supercells: Vec<V23BalancedServingSupercell>,
    pages: Vec<V23BalancedServingPage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23BalancedPseudoqueryEvidence {
    pub(crate) query_source_ordinal: u64,
    pub(crate) neighbor_source_ordinals: [u64; 10],
    pub(crate) scored_dimensions: u64,
    pub(crate) scalar_control_dimensions: u64,
    pub(crate) scalar_simd_max_distance_delta_ppm: u64,
    pub(crate) scalar_simd_equal: bool,
}

#[derive(Debug, Clone, Copy)]
struct V23BalancedRankedRow {
    distance: f32,
    source_ordinal: u64,
    vector: [f32; 96],
}

impl PartialEq for V23BalancedRankedRow {
    fn eq(&self, other: &Self) -> bool {
        self.distance.total_cmp(&other.distance) == Ordering::Equal
            && self.source_ordinal == other.source_ordinal
    }
}

impl Eq for V23BalancedRankedRow {}

impl PartialOrd for V23BalancedRankedRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for V23BalancedRankedRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.source_ordinal.cmp(&other.source_ordinal))
    }
}

pub(crate) struct V23BalancedPseudoqueryAccumulator {
    queries: Vec<(u64, [f32; 96])>,
    fused: Vec<BinaryHeap<V23BalancedRankedRow>>,
    scored_dimensions: Vec<u64>,
    first_source_ordinal: Option<u64>,
    last_source_ordinal: Option<u64>,
    retained_candidates: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23BalancedSample {
    pub(crate) query_index: u32,
    pub(crate) ground_truth_page_assignments: Vec<Vec<u32>>,
    pub(crate) layout_oracle_pages: Vec<u32>,
    pub(crate) containment_page_universe: Vec<u32>,
    pub(crate) containment_oracle_pages: Vec<u32>,
    pub(crate) selected_pages: Vec<u32>,
    pub(crate) layout_oracle_hits: u8,
    pub(crate) containment_hits: u8,
    pub(crate) selector_hits: u8,
    pub(crate) scored_dimensions: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum V23BalancedCausalClass {
    #[serde(rename = "authority-stop")]
    AuthorityStop,
    #[serde(rename = "balanced-layout-rejected")]
    BalancedLayoutRejected,
    #[serde(rename = "supercell-containment-rejected")]
    SupercellContainmentRejected,
    #[serde(rename = "page-selector-rejected")]
    PageSelectorRejected,
    #[serde(rename = "balanced-page-candidate")]
    BalancedPageCandidate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23BalancedResult {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) selected_arm: V23BalancedArm,
    pub(crate) selected_page_budget: V23BalancedPageBudget,
    pub(crate) samples: Vec<V23BalancedSample>,
    pub(crate) aggregate_layout_oracle_hits: u64,
    pub(crate) minimum_layout_oracle_hits: u8,
    pub(crate) aggregate_containment_hits: u64,
    pub(crate) aggregate_selector_hits: u64,
    pub(crate) minimum_selector_hits: u8,
    pub(crate) oracle_attainment_ppm: u64,
    pub(crate) timing_warmups: u32,
    pub(crate) resident_cpu_samples_ns: Vec<u64>,
    pub(crate) resident_cpu_p99_ns: u64,
    pub(crate) projected_serving_bytes: u64,
    pub(crate) projected_page_bytes: u64,
    pub(crate) scalar_simd_pages_equal: bool,
    pub(crate) causal_class: V23BalancedCausalClass,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(format!("V23 balanced page evaluation {message}"))
}

fn best_v23_balanced_page_coverage(
    truth_assignments: &[Vec<u32>],
    maximum_pages: usize,
) -> Result<V23PageCoverage> {
    if truth_assignments.is_empty()
        || truth_assignments.len() > 10
        || maximum_pages == 0
        || maximum_pages > 16
        || truth_assignments.iter().any(|pages| {
            pages.is_empty() || pages.len() > 2 || pages.windows(2).any(|pair| pair[0] >= pair[1])
        })
    {
        return Err(invalid("coverage oracle authority differs"));
    }
    let candidates = truth_assignments
        .iter()
        .flatten()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let limit = maximum_pages.min(candidates.len());
    let masks = candidates
        .iter()
        .map(|page| {
            truth_assignments
                .iter()
                .enumerate()
                .fold(0_usize, |mask, (index, assignments)| {
                    mask | (usize::from(assignments.binary_search(page).is_ok()) << index)
                })
        })
        .collect::<Vec<_>>();
    let mask_count = 1_usize << truth_assignments.len();
    let mut choices = vec![vec![None::<Vec<u32>>; mask_count]; limit + 1];
    choices[0][0] = Some(Vec::new());
    for (page, page_mask) in candidates.into_iter().zip(masks) {
        for count in (0..limit).rev() {
            for mask in 0..mask_count {
                let Some(existing) = choices[count][mask].clone() else {
                    continue;
                };
                let mut candidate = existing;
                candidate.push(page);
                let target = &mut choices[count + 1][mask | page_mask];
                if target.as_ref().is_none_or(|current| candidate < *current) {
                    *target = Some(candidate);
                }
            }
        }
    }
    let mut best = V23PageCoverage {
        page_ordinals: Vec::new(),
        hits: 0,
    };
    for by_mask in choices.iter().skip(1) {
        for (mask, pages) in by_mask.iter().enumerate() {
            let Some(pages) = pages else { continue };
            let hits = mask.count_ones() as usize;
            if hits > best.hits
                || (hits == best.hits
                    && (best.page_ordinals.is_empty() || *pages < best.page_ordinals))
            {
                best.hits = hits;
                best.page_ordinals.clone_from(pages);
            }
        }
    }
    Ok(best)
}

fn retain_top_ten(
    heap: &mut BinaryHeap<V23BalancedRankedRow>,
    candidate: V23BalancedRankedRow,
) -> bool {
    if heap.len() == 10 && heap.peek().is_some_and(|worst| candidate >= *worst) {
        return false;
    }
    let grew = heap.len() < 10;
    heap.push(candidate);
    if heap.len() > 10 {
        heap.pop();
    }
    grew
}

fn pseudoquery_distance(query: &[f32; 96], row: &[f32; 96], fused: bool) -> Result<f32> {
    let dot = if fused {
        borsuk_fma::fused_dot_8x12(query, row)
            .map_err(|_| invalid("fused SIMD backend unavailable"))?
            .0
    } else {
        query
            .iter()
            .zip(row)
            .map(|(left, right)| f64::from(*left) * f64::from(*right))
            .sum::<f64>() as f32
    };
    let distance = 1.0 - dot;
    if !distance.is_finite() || distance < -(16.0 * f32::EPSILON) {
        return Err(invalid("pseudoquery distance differs"));
    }
    Ok(distance.max(0.0))
}

impl V23BalancedPseudoqueryAccumulator {
    pub(crate) fn new(queries: Vec<(u64, [f32; 96])>) -> Result<Self> {
        if queries.is_empty() || queries.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
            return Err(invalid("pseudoquery source authority differs"));
        }
        let queries = queries
            .into_iter()
            .map(|(source_ordinal, vector)| {
                Ok((source_ordinal, normalize_v23_incidence_vector(&vector)?))
            })
            .collect::<Result<Vec<_>>>()?;
        let count = queries.len();
        Ok(Self {
            queries,
            fused: (0..count).map(|_| BinaryHeap::with_capacity(11)).collect(),
            scored_dimensions: vec![0; count],
            first_source_ordinal: None,
            last_source_ordinal: None,
            retained_candidates: 0,
        })
    }

    pub(crate) fn consider(&mut self, source_ordinal: u64, vector: &[f32; 96]) -> Result<()> {
        if self
            .last_source_ordinal
            .is_some_and(|last| source_ordinal <= last)
        {
            return Err(invalid("pseudoquery corpus order differs"));
        }
        self.first_source_ordinal.get_or_insert(source_ordinal);
        self.last_source_ordinal = Some(source_ordinal);
        let row = normalize_v23_incidence_vector(vector)?;
        for (index, (query_source_ordinal, query)) in self.queries.iter().enumerate() {
            let distance = pseudoquery_distance(query, &row, true)?;
            self.scored_dimensions[index] = self.scored_dimensions[index]
                .checked_add(96)
                .ok_or_else(|| invalid("pseudoquery work count overflows"))?;
            if source_ordinal == *query_source_ordinal {
                continue;
            }
            if retain_top_ten(
                &mut self.fused[index],
                V23BalancedRankedRow {
                    distance,
                    source_ordinal,
                    vector: row,
                },
            ) {
                self.retained_candidates += 1;
            }
        }
        Ok(())
    }

    pub(crate) fn merge(&mut self, other: Self) -> Result<()> {
        if self.queries != other.queries
            || self.last_source_ordinal.is_none()
            || other.first_source_ordinal.is_none()
            || other.first_source_ordinal <= self.last_source_ordinal
        {
            return Err(invalid("pseudoquery shard authority differs"));
        }
        for (index, heap) in other.fused.into_iter().enumerate() {
            for candidate in heap {
                retain_top_ten(&mut self.fused[index], candidate);
            }
            self.scored_dimensions[index] = self.scored_dimensions[index]
                .checked_add(other.scored_dimensions[index])
                .ok_or_else(|| invalid("pseudoquery merged work count overflows"))?;
        }
        self.last_source_ordinal = other.last_source_ordinal;
        self.retained_candidates = self.fused.iter().map(BinaryHeap::len).sum();
        Ok(())
    }

    pub(crate) fn finish(&self) -> Result<Vec<V23BalancedPseudoqueryEvidence>> {
        self.queries
            .iter()
            .enumerate()
            .map(|(index, (query_source_ordinal, _))| {
                if self.fused[index].len() != 10 {
                    return Err(invalid("pseudoquery top ten is incomplete"));
                }
                let mut fused = self.fused[index].clone().into_vec();
                fused.sort_unstable();
                let fused_sources = fused
                    .iter()
                    .map(|candidate| candidate.source_ordinal)
                    .collect::<Vec<_>>();
                let mut maximum_delta_ppm = 0_u64;
                for candidate in &fused {
                    let scalar_distance =
                        pseudoquery_distance(&self.queries[index].1, &candidate.vector, false)?;
                    let scale = candidate.distance.abs().max(scalar_distance.abs()).max(1.0);
                    let delta_ppm = (f64::from((candidate.distance - scalar_distance).abs())
                        * 1_000_000.0
                        / f64::from(scale))
                    .ceil() as u64;
                    maximum_delta_ppm = maximum_delta_ppm.max(delta_ppm);
                }
                if maximum_delta_ppm > MAX_SCALAR_SIMD_DISTANCE_DELTA_PPM {
                    return Err(invalid("pseudoquery scalar/SIMD evidence differs"));
                }
                Ok(V23BalancedPseudoqueryEvidence {
                    query_source_ordinal: *query_source_ordinal,
                    neighbor_source_ordinals: fused_sources
                        .try_into()
                        .map_err(|_| invalid("pseudoquery top ten is incomplete"))?,
                    scored_dimensions: self.scored_dimensions[index],
                    scalar_control_dimensions: 10 * 96,
                    scalar_simd_max_distance_delta_ppm: maximum_delta_ppm,
                    scalar_simd_equal: true,
                })
            })
            .collect()
    }

    pub(crate) fn maximum_retained_candidates(&self) -> usize {
        self.retained_candidates
    }
}

pub(crate) fn select_v23_balanced_pair(
    pairs: &[V23BalancedPseudoqueryPair],
) -> Result<V23BalancedSelectedPairEvidence> {
    let expected = [8_u8, 12, 16]
        .into_iter()
        .flat_map(|budget| {
            [
                V23BalancedArm::Amp1125,
                V23BalancedArm::Amp1250,
                V23BalancedArm::Amp1500,
            ]
            .into_iter()
            .map(move |arm| (budget, arm))
        })
        .collect::<Vec<_>>();
    if pairs.len() != expected.len()
        || pairs.iter().zip(expected).any(|(pair, expected)| {
            (pair.selected_pair.page_budget.get(), pair.selected_pair.arm) != expected
                || pair.projected_page_bytes
                    != u64::from(pair.selected_pair.page_budget.get()) * MAX_ENCODED_PAGE_BYTES
        })
    {
        return Err(invalid("pseudoquery pair authority differs"));
    }
    let selected = pairs
        .iter()
        .find(|pair| {
            pair.aggregate_recall_ppm >= 993_750
                && pair.minimum_recall_ppm >= 900_000
                && pair.oracle_attainment_ppm >= 995_000
                && pair.every_query_has_budget
                && pair.projected_page_bytes <= MAX_PROJECTED_PAGE_BYTES
                && pair.maximum_scored_dimensions <= MAX_SCORED_DIMENSIONS
                && pair.amplification_and_page_caps_valid
        })
        .ok_or_else(|| invalid("no pseudoquery pair passes"))?;
    Ok(V23BalancedSelectedPairEvidence {
        selected_pair: selected.selected_pair,
        official_query_reads: 0,
    })
}

fn adjusted_score(
    query: &[f32; 96],
    centroid: &[f32; 96],
    radius: f32,
    fused: bool,
) -> Result<f32> {
    if !radius.is_finite() || radius < 0.0 {
        return Err(invalid("selector radius differs"));
    }
    let dot = if fused {
        borsuk_fma::fused_dot_8x12(query, centroid)
            .map_err(|_| invalid("fused SIMD backend unavailable"))?
            .0
    } else {
        query
            .iter()
            .zip(centroid)
            .map(|(left, right)| f64::from(*left) * f64::from(*right))
            .sum::<f64>() as f32
    };
    let distance = 1.0 - dot;
    if !distance.is_finite() || distance < -(16.0 * f32::EPSILON) {
        return Err(invalid("selector distance differs"));
    }
    Ok((distance.max(0.0) - radius).max(0.0))
}

fn ranked_pages(
    query: &[f32; 96],
    geometry: &V23BalancedServingGeometry,
    page_budget: V23BalancedPageBudget,
    fused: bool,
) -> Result<(Vec<u32>, Vec<u32>, u64)> {
    let mut cells = geometry
        .supercells
        .iter()
        .map(|cell| {
            Ok((
                adjusted_score(query, &cell.centroid, cell.cosine_radius, fused)?,
                cell.supercell_ordinal,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    cells.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    cells.truncate(96);
    let mut candidates = Vec::new();
    for (_, supercell_ordinal) in &cells {
        let cell = &geometry.supercells[usize::try_from(*supercell_ordinal).unwrap()];
        let end = cell
            .first_page
            .checked_add(cell.page_count)
            .ok_or_else(|| invalid("selector page range overflows"))?;
        for page_ordinal in cell.first_page..end {
            let page = &geometry.pages[usize::try_from(page_ordinal).unwrap()];
            candidates.push((
                adjusted_score(query, &page.centroid, page.cosine_radius, fused)?,
                page_ordinal,
            ));
        }
    }
    let page_budget = usize::from(page_budget.get());
    if candidates.len() < page_budget {
        return Err(invalid("selector has fewer pages than its frozen budget"));
    }
    let mut containment_page_universe = candidates
        .iter()
        .map(|candidate| candidate.1)
        .collect::<Vec<_>>();
    containment_page_universe.sort_unstable();
    containment_page_universe.dedup();
    if containment_page_universe.len() != candidates.len() {
        return Err(invalid("selector containment page duplicates"));
    }
    candidates.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    let candidate_count = candidates.len();
    let selected = candidates
        .into_iter()
        .take(page_budget)
        .map(|entry| entry.1)
        .collect::<Vec<_>>();
    let dimensions = u64::try_from(geometry.supercells.len())
        .ok()
        .and_then(|cells| cells.checked_add(u64::try_from(candidate_count).ok()?))
        .and_then(|vectors| vectors.checked_mul(96))
        .ok_or_else(|| invalid("selector work count overflows"))?;
    Ok((selected, containment_page_universe, dimensions))
}

pub(crate) fn prepare_v23_balanced_serving_geometry(
    supercells: &[V23SupercellRow],
    pages: &[V23PageRow],
    selected_arm: V23BalancedArm,
) -> Result<V23BalancedServingGeometry> {
    let maximum_replica_rows = match selected_arm {
        V23BalancedArm::Amp1125 => 48,
        V23BalancedArm::Amp1250 => 96,
        V23BalancedArm::Amp1500 => 192,
    };
    validate_v23_balanced_page_geometry(supercells, pages, maximum_replica_rows)?;
    let mut total_primary_rows = 0_u64;
    let mut total_replica_rows = 0_u64;
    for page in pages {
        total_primary_rows = total_primary_rows
            .checked_add(u64::from(page.primary_rows))
            .ok_or_else(|| invalid("primary population overflows"))?;
        total_replica_rows = total_replica_rows
            .checked_add(u64::from(page.replica_rows))
            .ok_or_else(|| invalid("replica population overflows"))?;
    }
    let amplification_valid = match selected_arm {
        V23BalancedArm::Amp1125 => total_replica_rows.checked_mul(8),
        V23BalancedArm::Amp1250 => total_replica_rows.checked_mul(4),
        V23BalancedArm::Amp1500 => total_replica_rows.checked_mul(2),
    }
    .is_some_and(|scaled_replicas| scaled_replicas <= total_primary_rows);
    if !amplification_valid {
        return Err(invalid("selected arm amplification differs"));
    }
    let supercells = supercells
        .iter()
        .map(|cell| {
            Ok(V23BalancedServingSupercell {
                supercell_ordinal: cell.supercell_ordinal,
                centroid: normalize_v23_incidence_vector(&cell.centroid.map(half::f16::to_f32))?,
                cosine_radius: cell.cosine_radius,
                first_page: cell.first_page,
                page_count: cell.page_count,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let pages = pages
        .iter()
        .map(|page| {
            Ok(V23BalancedServingPage {
                centroid: normalize_v23_incidence_vector(&page.centroid.map(half::f16::to_f32))?,
                cosine_radius: page.cosine_radius,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(V23BalancedServingGeometry {
        selected_arm,
        maximum_replica_rows,
        total_primary_rows,
        total_replica_rows,
        supercells,
        pages,
    })
}

pub(crate) fn select_v23_balanced_pages(
    query: &[f32; 96],
    geometry: &V23BalancedServingGeometry,
    page_budget: V23BalancedPageBudget,
) -> Result<V23BalancedPageSelection> {
    let query = normalize_v23_incidence_vector(query)?;
    let (pages_fused, fused_universe, scored_dimensions) =
        ranked_pages(&query, geometry, page_budget, true)?;
    let (pages_scalar, scalar_universe, scalar_dimensions) =
        ranked_pages(&query, geometry, page_budget, false)?;
    if scored_dimensions != scalar_dimensions
        || scored_dimensions > MAX_SCORED_DIMENSIONS
        || pages_fused != pages_scalar
        || fused_universe != scalar_universe
    {
        return Err(invalid("selector scalar/SIMD evidence differs"));
    }
    Ok(V23BalancedPageSelection {
        pages: pages_fused,
        containment_page_universe: fused_universe,
        scored_dimensions,
        scalar_simd_pages_equal: true,
    })
}

pub(crate) fn measure_v23_balanced_selector(
    queries: &[[f32; 96]],
    geometry: &V23BalancedServingGeometry,
    page_budget: V23BalancedPageBudget,
) -> Result<V23BalancedTimingEvidence> {
    const WARMUPS: u32 = 1_024;
    const SAMPLES: usize = 10_000;

    if queries.is_empty() {
        return Err(invalid("timing query cohort is empty"));
    }
    let fused_once = |query: &[f32; 96]| {
        let query = normalize_v23_incidence_vector(std::hint::black_box(query))?;
        ranked_pages(&query, geometry, page_budget, true).map(|result| (result.0, result.2))
    };
    for iteration in 0..WARMUPS {
        let query = &queries[usize::try_from(iteration).unwrap() % queries.len()];
        std::hint::black_box(fused_once(query)?);
    }
    let mut samples_ns = Vec::with_capacity(SAMPLES);
    for iteration in 0..SAMPLES {
        let query = &queries[iteration % queries.len()];
        let started = Instant::now();
        std::hint::black_box(fused_once(query)?);
        let elapsed = u64::try_from(started.elapsed().as_nanos())
            .map_err(|_| invalid("selector timing overflows"))?;
        if elapsed == 0 {
            return Err(invalid("selector timing resolution differs"));
        }
        samples_ns.push(elapsed);
    }
    let mut pages_by_query = Vec::with_capacity(queries.len());
    let mut scored_dimensions = None;
    for query in queries {
        let query = normalize_v23_incidence_vector(query)?;
        let (pages_fused, fused_universe, fused_dimensions) =
            ranked_pages(&query, geometry, page_budget, true)?;
        let (pages_scalar, scalar_universe, scalar_dimensions) =
            ranked_pages(&query, geometry, page_budget, false)?;
        if fused_dimensions != scalar_dimensions
            || fused_dimensions > MAX_SCORED_DIMENSIONS
            || pages_fused != pages_scalar
            || fused_universe != scalar_universe
            || scored_dimensions.is_some_and(|expected| expected != fused_dimensions)
        {
            return Err(invalid("selector scalar/SIMD evidence differs"));
        }
        scored_dimensions = Some(fused_dimensions);
        pages_by_query.push(pages_fused);
    }
    let mut sorted = samples_ns.clone();
    sorted.sort_unstable();
    Ok(V23BalancedTimingEvidence {
        warmups: WARMUPS,
        samples_ns,
        p99_ns: sorted[9_899],
        pages_by_query,
        scored_dimensions: scored_dimensions.unwrap(),
        scalar_simd_pages_equal: true,
    })
}

pub(crate) fn build_v23_balanced_sample(
    query_index: u32,
    query: &[f32; 96],
    ground_truth_page_assignments: Vec<Vec<u32>>,
    geometry: &V23BalancedServingGeometry,
    page_budget: V23BalancedPageBudget,
) -> Result<V23BalancedSample> {
    let selection = select_v23_balanced_pages(query, geometry, page_budget)?;
    evaluate_v23_balanced_sample(
        query_index,
        ground_truth_page_assignments,
        selection.containment_page_universe,
        selection.pages,
        selection.scored_dimensions,
        page_budget,
    )
}

pub(crate) fn evaluate_v23_balanced_sample(
    query_index: u32,
    ground_truth_page_assignments: Vec<Vec<u32>>,
    containment_page_universe: Vec<u32>,
    selected_pages: Vec<u32>,
    scored_dimensions: u64,
    page_budget: V23BalancedPageBudget,
) -> Result<V23BalancedSample> {
    let page_budget = usize::from(page_budget.get());
    if ground_truth_page_assignments.len() != 10
        || ground_truth_page_assignments.iter().any(|pages| {
            pages.is_empty() || pages.len() > 2 || pages.windows(2).any(|pair| pair[0] >= pair[1])
        })
        || containment_page_universe.is_empty()
        || containment_page_universe
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || selected_pages.len() != page_budget
        || selected_pages
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != page_budget
        || scored_dimensions > MAX_SCORED_DIMENSIONS
    {
        return Err(invalid("sample selection authority differs"));
    }
    let containment = containment_page_universe
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if selected_pages
        .iter()
        .any(|page| !containment.contains(page))
    {
        return Err(invalid("selector escapes containment universe"));
    }
    let layout_oracle =
        best_v23_balanced_page_coverage(&ground_truth_page_assignments, page_budget)?;
    let contained_truth = ground_truth_page_assignments
        .iter()
        .map(|assignments| {
            assignments
                .iter()
                .copied()
                .filter(|page| containment.contains(page))
                .collect::<Vec<_>>()
        })
        .filter(|assignments| !assignments.is_empty())
        .collect::<Vec<_>>();
    let containment_oracle = if contained_truth.is_empty() {
        None
    } else {
        Some(best_v23_balanced_page_coverage(
            &contained_truth,
            page_budget,
        )?)
    };
    let layout_oracle_hits =
        u8::try_from(layout_oracle.hits).map_err(|_| invalid("layout hits overflow"))?;
    let selector_hits = ground_truth_page_assignments
        .iter()
        .filter(|assignments| assignments.iter().any(|page| selected_pages.contains(page)))
        .count();
    Ok(V23BalancedSample {
        query_index,
        ground_truth_page_assignments,
        layout_oracle_pages: layout_oracle.page_ordinals,
        containment_page_universe,
        containment_oracle_pages: containment_oracle
            .as_ref()
            .map_or_else(Vec::new, |coverage| coverage.page_ordinals.clone()),
        selected_pages,
        layout_oracle_hits,
        containment_hits: u8::try_from(
            containment_oracle
                .as_ref()
                .map_or(0, |coverage| coverage.hits),
        )
        .map_err(|_| invalid("containment hits overflow"))?,
        selector_hits: u8::try_from(selector_hits)
            .map_err(|_| invalid("selector hits overflow"))?,
        scored_dimensions,
    })
}

fn validate_v23_balanced_sample_page_universe(
    sample: &V23BalancedSample,
    geometry: &V23BalancedServingGeometry,
) -> Result<()> {
    let page_count =
        u32::try_from(geometry.pages.len()).map_err(|_| invalid("serving page count overflows"))?;
    if sample
        .ground_truth_page_assignments
        .iter()
        .flatten()
        .chain(&sample.containment_page_universe)
        .chain(&sample.selected_pages)
        .any(|page| *page >= page_count)
    {
        return Err(invalid("sample page authority differs"));
    }
    Ok(())
}

pub(crate) fn evaluate_v23_balanced_pseudoquery_pair(
    selected_pair: V23BalancedSelectedPair,
    samples: &[V23BalancedSample],
    geometry: &V23BalancedServingGeometry,
) -> Result<V23BalancedPseudoqueryPair> {
    evaluate_v23_balanced_pseudoquery_pair_for_expected_count(
        selected_pair,
        samples,
        geometry,
        1_024,
    )
}

fn evaluate_v23_balanced_pseudoquery_pair_for_expected_count(
    selected_pair: V23BalancedSelectedPair,
    samples: &[V23BalancedSample],
    geometry: &V23BalancedServingGeometry,
    expected_count: usize,
) -> Result<V23BalancedPseudoqueryPair> {
    let page_budget = V23BalancedPageBudget::new(selected_pair.page_budget.get())?;
    if expected_count == 0
        || samples.len() != expected_count
        || geometry.selected_arm != selected_pair.arm
    {
        return Err(invalid("pseudoquery pair cohort cardinality differs"));
    }
    let mut selector_hits = 0_u64;
    let mut oracle_hits = 0_u64;
    let mut minimum_hits = 10_u8;
    let mut maximum_scored_dimensions = 0_u64;
    for (query_index, sample) in samples.iter().enumerate() {
        validate_v23_balanced_sample_page_universe(sample, geometry)?;
        let expected = evaluate_v23_balanced_sample(
            u32::try_from(query_index).unwrap(),
            sample.ground_truth_page_assignments.clone(),
            sample.containment_page_universe.clone(),
            sample.selected_pages.clone(),
            sample.scored_dimensions,
            page_budget,
        )?;
        if *sample != expected {
            return Err(invalid("pseudoquery pair sample evidence differs"));
        }
        selector_hits = selector_hits
            .checked_add(u64::from(sample.selector_hits))
            .ok_or_else(|| invalid("pseudoquery pair selector hits overflow"))?;
        oracle_hits = oracle_hits
            .checked_add(u64::from(sample.layout_oracle_hits))
            .ok_or_else(|| invalid("pseudoquery pair oracle hits overflow"))?;
        minimum_hits = minimum_hits.min(sample.selector_hits);
        maximum_scored_dimensions = maximum_scored_dimensions.max(sample.scored_dimensions);
    }
    let possible_hits = u64::try_from(expected_count)
        .ok()
        .and_then(|count| count.checked_mul(10))
        .ok_or_else(|| invalid("pseudoquery pair cohort cardinality overflows"))?;
    let aggregate_recall_ppm = selector_hits
        .checked_mul(1_000_000)
        .ok_or_else(|| invalid("pseudoquery pair aggregate recall overflows"))?
        / possible_hits;
    let oracle_attainment_ppm = selector_hits
        .checked_mul(1_000_000)
        .ok_or_else(|| invalid("pseudoquery pair oracle attainment overflows"))?
        / oracle_hits.max(1);
    Ok(V23BalancedPseudoqueryPair {
        selected_pair,
        aggregate_recall_ppm,
        minimum_recall_ppm: u64::from(minimum_hits) * 100_000,
        oracle_attainment_ppm,
        every_query_has_budget: samples
            .iter()
            .all(|sample| sample.selected_pages.len() == usize::from(page_budget.get())),
        projected_page_bytes: u64::from(page_budget.get()) * MAX_ENCODED_PAGE_BYTES,
        maximum_scored_dimensions,
        amplification_and_page_caps_valid: true,
    })
}

fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        serde_json::Value::Object(values) => {
            let mut entries = values.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json_value(value)))
                    .collect(),
            )
        }
        scalar => scalar,
    }
}

pub(crate) fn evaluate_v23_balanced_development(
    selected_pair: V23BalancedSelectedPair,
    samples: Vec<V23BalancedSample>,
    geometry: &V23BalancedServingGeometry,
    timing: &V23BalancedTimingEvidence,
    projected_serving_bytes: u64,
) -> Result<V23BalancedResult> {
    let selected_page_budget = V23BalancedPageBudget::new(selected_pair.page_budget.get())?;
    let selected_arm = selected_pair.arm;
    if samples.len() != 32
        || geometry.selected_arm != selected_arm
        || samples
            .iter()
            .any(|sample| sample.selected_pages.len() != usize::from(selected_page_budget.get()))
        || timing.warmups != 1_024
        || timing.samples_ns.len() != 10_000
        || timing.samples_ns.contains(&0)
        || timing.pages_by_query.len() != samples.len()
        || samples
            .iter()
            .zip(&timing.pages_by_query)
            .any(|(sample, pages)| {
                sample.selected_pages != *pages
                    || sample.scored_dimensions != timing.scored_dimensions
            })
    {
        return Err(invalid("development evidence shape differs"));
    }
    for sample in &samples {
        validate_v23_balanced_sample_page_universe(sample, geometry)?;
    }
    let aggregate_layout_oracle_hits = samples
        .iter()
        .map(|sample| u64::from(sample.layout_oracle_hits))
        .sum::<u64>();
    let minimum_layout_oracle_hits = samples
        .iter()
        .map(|sample| sample.layout_oracle_hits)
        .min()
        .unwrap();
    let aggregate_containment_hits = samples
        .iter()
        .map(|sample| u64::from(sample.containment_hits))
        .sum::<u64>();
    let aggregate_selector_hits = samples
        .iter()
        .map(|sample| u64::from(sample.selector_hits))
        .sum::<u64>();
    let minimum_selector_hits = samples
        .iter()
        .map(|sample| sample.selector_hits)
        .min()
        .unwrap();
    let oracle_attainment_ppm = aggregate_selector_hits
        .checked_mul(1_000_000)
        .ok_or_else(|| invalid("development attainment overflows"))?
        / aggregate_layout_oracle_hits.max(1);
    let mut timings = timing.samples_ns.clone();
    timings.sort_unstable();
    let resident_cpu_p99_ns = timings[9_899];
    if resident_cpu_p99_ns != timing.p99_ns {
        return Err(invalid("development timing percentile differs"));
    }
    let scalar_simd_pages_equal = timing.scalar_simd_pages_equal;
    let projected_page_bytes = u64::from(selected_page_budget.get()) * MAX_ENCODED_PAGE_BYTES;
    let causal_class = if projected_serving_bytes >= MAX_SERVING_BYTES
        || projected_page_bytes > MAX_PROJECTED_PAGE_BYTES
        || !scalar_simd_pages_equal
    {
        V23BalancedCausalClass::AuthorityStop
    } else if aggregate_layout_oracle_hits < 318 || minimum_layout_oracle_hits < 9 {
        V23BalancedCausalClass::BalancedLayoutRejected
    } else if aggregate_containment_hits != aggregate_layout_oracle_hits {
        V23BalancedCausalClass::SupercellContainmentRejected
    } else if aggregate_selector_hits < 318
        || minimum_selector_hits < 9
        || oracle_attainment_ppm < 995_000
        || resident_cpu_p99_ns >= 15_000_000
    {
        V23BalancedCausalClass::PageSelectorRejected
    } else {
        V23BalancedCausalClass::BalancedPageCandidate
    };
    let result = V23BalancedResult {
        schema: RESULT_SCHEMA.to_owned(),
        claim_eligible: false,
        selected_arm,
        selected_page_budget,
        samples,
        aggregate_layout_oracle_hits,
        minimum_layout_oracle_hits,
        aggregate_containment_hits,
        aggregate_selector_hits,
        minimum_selector_hits,
        oracle_attainment_ppm,
        timing_warmups: timing.warmups,
        resident_cpu_samples_ns: timing.samples_ns.clone(),
        resident_cpu_p99_ns,
        projected_serving_bytes,
        projected_page_bytes,
        scalar_simd_pages_equal,
        causal_class,
    };
    canonical_v23_balanced_result_bytes(&result)?;
    Ok(result)
}

pub(crate) fn canonical_v23_balanced_result_bytes(result: &V23BalancedResult) -> Result<Vec<u8>> {
    if result.schema != RESULT_SCHEMA
        || result.claim_eligible
        || result.samples.len() != 32
        || V23BalancedPageBudget::new(result.selected_page_budget.get()).is_err()
        || result.samples.iter().any(|sample| {
            sample.selected_pages.len() != usize::from(result.selected_page_budget.get())
        })
        || result.projected_page_bytes
            != u64::from(result.selected_page_budget.get()) * MAX_ENCODED_PAGE_BYTES
        || result.projected_page_bytes > MAX_PROJECTED_PAGE_BYTES
        || result.timing_warmups != 1_024
        || result.resident_cpu_samples_ns.len() != 10_000
        || result.resident_cpu_samples_ns.contains(&0)
    {
        return Err(invalid("result authority differs"));
    }
    let mut timings = result.resident_cpu_samples_ns.clone();
    timings.sort_unstable();
    let resident_cpu_p99_ns = timings[9_899];
    if result.resident_cpu_p99_ns != resident_cpu_p99_ns {
        return Err(invalid("timing percentile differs"));
    }
    let mut aggregate_oracle = 0_u64;
    let mut aggregate_containment = 0_u64;
    let mut aggregate_selector = 0_u64;
    let mut minimum_oracle = 10_u8;
    let mut minimum_selector = 10_u8;
    for (query_index, sample) in result.samples.iter().enumerate() {
        let expected = evaluate_v23_balanced_sample(
            u32::try_from(query_index).unwrap(),
            sample.ground_truth_page_assignments.clone(),
            sample.containment_page_universe.clone(),
            sample.selected_pages.clone(),
            sample.scored_dimensions,
            result.selected_page_budget,
        )?;
        if *sample != expected {
            return Err(invalid("sample evidence differs"));
        }
        aggregate_oracle += u64::from(sample.layout_oracle_hits);
        aggregate_containment += u64::from(sample.containment_hits);
        aggregate_selector += u64::from(sample.selector_hits);
        minimum_oracle = minimum_oracle.min(sample.layout_oracle_hits);
        minimum_selector = minimum_selector.min(sample.selector_hits);
    }
    let attainment = aggregate_selector
        .checked_mul(1_000_000)
        .ok_or_else(|| invalid("oracle attainment overflows"))?
        / aggregate_oracle.max(1);
    let class =
        if result.projected_serving_bytes >= MAX_SERVING_BYTES || !result.scalar_simd_pages_equal {
            V23BalancedCausalClass::AuthorityStop
        } else if aggregate_oracle < 318 || minimum_oracle < 9 {
            V23BalancedCausalClass::BalancedLayoutRejected
        } else if aggregate_containment != aggregate_oracle {
            V23BalancedCausalClass::SupercellContainmentRejected
        } else if aggregate_selector < 318
            || minimum_selector < 9
            || attainment < 995_000
            || resident_cpu_p99_ns >= 15_000_000
        {
            V23BalancedCausalClass::PageSelectorRejected
        } else {
            V23BalancedCausalClass::BalancedPageCandidate
        };
    if result.aggregate_layout_oracle_hits != aggregate_oracle
        || result.minimum_layout_oracle_hits != minimum_oracle
        || result.aggregate_containment_hits != aggregate_containment
        || result.aggregate_selector_hits != aggregate_selector
        || result.minimum_selector_hits != minimum_selector
        || result.oracle_attainment_ppm != attainment
        || result.causal_class != class
    {
        return Err(invalid("result recomputation differs"));
    }
    let value = serde_json::to_value(result).map_err(|_| invalid("result encoding differs"))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|_| invalid("result encoding differs"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use half::f16;

    use crate::v23_balanced_pages::{V23BalancedPageBudget, V23BalancedSelectedPair};
    use crate::v23_balanced_pages_arrow::{V23PageRow, V23SupercellRow};

    use super::{
        MAX_SERVING_BYTES, V23BalancedArm, V23BalancedCausalClass,
        V23BalancedPseudoqueryAccumulator, V23BalancedPseudoqueryPair, V23BalancedResult,
        V23BalancedSample, V23BalancedTimingEvidence, build_v23_balanced_sample,
        canonical_v23_balanced_result_bytes, evaluate_v23_balanced_development,
        evaluate_v23_balanced_pseudoquery_pair,
        evaluate_v23_balanced_pseudoquery_pair_for_expected_count, evaluate_v23_balanced_sample,
        measure_v23_balanced_selector, prepare_v23_balanced_serving_geometry,
        select_v23_balanced_pages, select_v23_balanced_pair,
    };

    fn valid_geometry() -> super::V23BalancedServingGeometry {
        let mut centroid = [f16::from_f32(0.0); 96];
        centroid[0] = f16::from_f32(1.0);
        let supercells = vec![V23SupercellRow {
            supercell_ordinal: 0,
            centroid,
            cosine_radius: 0.0,
            primary_rows: 9,
            first_page: 0,
            page_count: 9,
        }];
        let pages = (0_u32..9)
            .map(|page_ordinal| V23PageRow {
                page_ordinal,
                supercell_ordinal: 0,
                primary_rows: 1,
                replica_rows: 0,
                centroid,
                cosine_radius: 0.0,
            })
            .collect::<Vec<_>>();
        prepare_v23_balanced_serving_geometry(&supercells, &pages, V23BalancedArm::Amp1250).unwrap()
    }

    fn timing_evidence(
        result: &V23BalancedResult,
        sample_ns: u64,
        scalar_simd_pages_equal: bool,
    ) -> V23BalancedTimingEvidence {
        V23BalancedTimingEvidence {
            warmups: 1_024,
            samples_ns: vec![sample_ns; 10_000],
            p99_ns: sample_ns,
            pages_by_query: result
                .samples
                .iter()
                .map(|sample| sample.selected_pages.clone())
                .collect(),
            scored_dimensions: 1_376_256,
            scalar_simd_pages_equal,
        }
    }

    fn selected_pair(result: &V23BalancedResult) -> V23BalancedSelectedPair {
        V23BalancedSelectedPair {
            page_budget: result.selected_page_budget,
            arm: result.selected_arm,
        }
    }

    fn pseudo_pair(
        budget: u8,
        arm: V23BalancedArm,
        aggregate_recall_ppm: u64,
        minimum_recall_ppm: u64,
    ) -> V23BalancedPseudoqueryPair {
        let page_budget = V23BalancedPageBudget::new(budget).unwrap();
        V23BalancedPseudoqueryPair {
            selected_pair: V23BalancedSelectedPair { page_budget, arm },
            aggregate_recall_ppm,
            minimum_recall_ppm,
            oracle_attainment_ppm: 995_000,
            every_query_has_budget: true,
            projected_page_bytes: u64::from(budget) * 122_880,
            maximum_scored_dimensions: 1_376_256,
            amplification_and_page_caps_valid: true,
        }
    }

    #[test]
    fn v23_balanced_eval_selection_freezes_budget_major_pair_without_official_inputs() {
        let mut pairs = Vec::new();
        for budget in [8, 12, 16] {
            for arm in [
                V23BalancedArm::Amp1125,
                V23BalancedArm::Amp1250,
                V23BalancedArm::Amp1500,
            ] {
                pairs.push(pseudo_pair(budget, arm, 993_749, 900_000));
            }
        }
        pairs[4] = pseudo_pair(12, V23BalancedArm::Amp1250, 993_750, 900_000);
        pairs[6] = pseudo_pair(16, V23BalancedArm::Amp1125, 1_000_000, 1_000_000);

        let selected = select_v23_balanced_pair(&pairs).unwrap();
        assert_eq!(
            selected.selected_pair,
            V23BalancedSelectedPair {
                page_budget: V23BalancedPageBudget::new(12).unwrap(),
                arm: V23BalancedArm::Amp1250,
            }
        );
        assert_eq!(selected.official_query_reads, 0);

        pairs[4] = pseudo_pair(12, V23BalancedArm::Amp1250, 993_749, 900_000);
        let selected = select_v23_balanced_pair(&pairs).unwrap();
        assert_eq!(selected.selected_pair.page_budget.get(), 16);

        pairs.swap(0, 1);
        assert!(select_v23_balanced_pair(&pairs).is_err());
    }

    fn valid_result() -> V23BalancedResult {
        let samples = (0_u32..32)
            .map(|query_index| V23BalancedSample {
                query_index,
                ground_truth_page_assignments: if query_index == 0 {
                    (0_u32..10)
                        .map(|rank| vec![if rank == 8 { 0 } else { rank.min(8) }])
                        .collect()
                } else {
                    (0_u32..10).map(|rank| vec![rank % 8]).collect()
                },
                layout_oracle_pages: (0_u32..8).collect(),
                containment_page_universe: (0_u32..8).collect(),
                containment_oracle_pages: (0_u32..8).collect(),
                selected_pages: (0_u32..8).collect(),
                layout_oracle_hits: if query_index == 0 { 9 } else { 10 },
                containment_hits: if query_index == 0 { 9 } else { 10 },
                selector_hits: if query_index == 0 { 9 } else { 10 },
                scored_dimensions: 1_376_256,
            })
            .collect();
        V23BalancedResult {
            schema: "borsuk-v23-balanced-page-result-v2".to_owned(),
            claim_eligible: false,
            selected_arm: V23BalancedArm::Amp1250,
            selected_page_budget: V23BalancedPageBudget::new(8).unwrap(),
            samples,
            aggregate_layout_oracle_hits: 319,
            minimum_layout_oracle_hits: 9,
            aggregate_containment_hits: 319,
            aggregate_selector_hits: 319,
            minimum_selector_hits: 9,
            oracle_attainment_ppm: 1_000_000,
            timing_warmups: 1_024,
            resident_cpu_samples_ns: vec![14_000_000; 10_000],
            resident_cpu_p99_ns: 14_000_000,
            projected_serving_bytes: 1_014_902_784,
            projected_page_bytes: 8 * 122_880,
            scalar_simd_pages_equal: true,
            causal_class: V23BalancedCausalClass::BalancedPageCandidate,
        }
    }

    #[test]
    fn v23_balanced_eval_result_recomputes_samples_gates_and_class() {
        let result = valid_result();
        let bytes = canonical_v23_balanced_result_bytes(&result).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert!(
            std::str::from_utf8(&bytes)
                .unwrap()
                .contains("\"causal_class\":\"balanced-page-candidate\"")
        );

        let mut aggregate_drift = result.clone();
        aggregate_drift.aggregate_selector_hits = 318;
        assert!(canonical_v23_balanced_result_bytes(&aggregate_drift).is_err());

        let mut class_drift = result;
        class_drift.causal_class = V23BalancedCausalClass::PageSelectorRejected;
        assert!(canonical_v23_balanced_result_bytes(&class_drift).is_err());

        let mut budget_drift = valid_result();
        budget_drift.selected_page_budget = V23BalancedPageBudget::new(12).unwrap();
        assert!(canonical_v23_balanced_result_bytes(&budget_drift).is_err());

        let mut bytes_drift = valid_result();
        bytes_drift.projected_page_bytes += 1;
        assert!(canonical_v23_balanced_result_bytes(&bytes_drift).is_err());

        let mut timing_drift = valid_result();
        timing_drift.resident_cpu_samples_ns[9_899..].fill(16_000_000);
        assert!(canonical_v23_balanced_result_bytes(&timing_drift).is_err());

        let mut ranked_not_ordinal = valid_result();
        ranked_not_ordinal.samples[0].selected_pages = vec![7, 0, 1, 2, 3, 4, 5, 6];
        assert!(canonical_v23_balanced_result_bytes(&ranked_not_ordinal).is_ok());

        let mut truth_drift = valid_result();
        truth_drift.samples[0].ground_truth_page_assignments[0] = vec![99];
        assert!(canonical_v23_balanced_result_bytes(&truth_drift).is_err());
    }

    #[test]
    fn v23_balanced_eval_sample_recomputes_layout_containment_and_selector_hits() {
        let truth = (0_u32..10)
            .map(|rank| vec![if rank == 8 { 0 } else { rank.min(8) }])
            .collect::<Vec<_>>();
        let sample = evaluate_v23_balanced_sample(
            0,
            truth,
            (0_u32..8).collect(),
            vec![7, 0, 1, 2, 3, 4, 5, 6],
            1_376_256,
            V23BalancedPageBudget::new(8).unwrap(),
        )
        .unwrap();
        assert_eq!(sample.layout_oracle_pages, (0_u32..8).collect::<Vec<_>>());
        assert_eq!(
            sample.containment_oracle_pages,
            (0_u32..8).collect::<Vec<_>>()
        );
        assert_eq!(sample.layout_oracle_hits, 9);
        assert_eq!(sample.containment_hits, 9);
        assert_eq!(sample.selector_hits, 9);

        assert!(
            evaluate_v23_balanced_sample(
                0,
                sample.ground_truth_page_assignments,
                (0_u32..8).collect(),
                vec![99, 0, 1, 2, 3, 4, 5, 6],
                1_376_256,
                V23BalancedPageBudget::new(8).unwrap(),
            )
            .is_err()
        );
    }

    #[test]
    fn v23_balanced_eval_builds_sample_from_query_and_serving_geometry() {
        let geometry = valid_geometry();
        let mut query = [0.0_f32; 96];
        query[0] = 1.0;
        let sample = build_v23_balanced_sample(
            0,
            &query,
            (0_u32..10).map(|rank| vec![rank % 9]).collect(),
            &geometry,
            V23BalancedPageBudget::new(8).unwrap(),
        )
        .unwrap();
        assert_eq!(
            sample.containment_page_universe,
            (0_u32..9).collect::<Vec<_>>()
        );
        assert_eq!(sample.selected_pages, (0_u32..8).collect::<Vec<_>>());
        assert_eq!(sample.layout_oracle_hits, 9);
        assert_eq!(sample.containment_hits, 9);
        assert_eq!(sample.selector_hits, 9);
    }

    #[test]
    fn v23_balanced_eval_sample_uses_the_frozen_page_budget_for_every_control() {
        let sample = evaluate_v23_balanced_sample(
            0,
            (0_u32..10).map(|page| vec![page]).collect(),
            (0_u32..16).collect(),
            (0_u32..12).collect(),
            1_376_256,
            V23BalancedPageBudget::new(12).unwrap(),
        )
        .unwrap();
        assert_eq!(sample.selected_pages.len(), 12);
        assert_eq!(sample.layout_oracle_hits, 10);
        assert_eq!(sample.containment_hits, 10);
        assert_eq!(sample.selector_hits, 10);

        assert!(
            evaluate_v23_balanced_sample(
                0,
                sample.ground_truth_page_assignments,
                sample.containment_page_universe,
                (0_u32..8).collect(),
                1_376_256,
                V23BalancedPageBudget::new(12).unwrap(),
            )
            .is_err()
        );
    }

    #[test]
    fn v23_balanced_eval_pair_recomputes_reduced_cohort_for_ladder_integration() {
        let geometry = valid_geometry();
        let mut query = [0.0_f32; 96];
        query[0] = 1.0;
        let budget = V23BalancedPageBudget::new(8).unwrap();
        let samples = (0_u32..8)
            .map(|query_index| {
                build_v23_balanced_sample(
                    query_index,
                    &query,
                    (0_u32..10).map(|rank| vec![rank % 9]).collect(),
                    &geometry,
                    budget,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let pair = evaluate_v23_balanced_pseudoquery_pair_for_expected_count(
            V23BalancedSelectedPair {
                page_budget: budget,
                arm: V23BalancedArm::Amp1250,
            },
            &samples,
            &geometry,
            8,
        )
        .unwrap();
        assert_eq!(pair.aggregate_recall_ppm, 900_000);
        assert_eq!(pair.minimum_recall_ppm, 900_000);
        assert_eq!(pair.oracle_attainment_ppm, 1_000_000);
        assert_eq!(pair.projected_page_bytes, 8 * 122_880);
    }

    #[test]
    fn v23_balanced_eval_development_derives_causal_precedence_and_timing() {
        let fixture = valid_result();
        let geometry = valid_geometry();
        let timing = timing_evidence(&fixture, 14_000_000, true);
        let candidate = evaluate_v23_balanced_development(
            selected_pair(&fixture),
            fixture.samples.clone(),
            &geometry,
            &timing,
            fixture.projected_serving_bytes,
        )
        .unwrap();
        assert_eq!(
            candidate.causal_class,
            V23BalancedCausalClass::BalancedPageCandidate
        );
        assert_eq!(candidate.aggregate_selector_hits, 319);
        assert_eq!(candidate.resident_cpu_p99_ns, 14_000_000);

        let slow_timing = timing_evidence(&fixture, 15_000_000, true);
        let slow = evaluate_v23_balanced_development(
            selected_pair(&fixture),
            fixture.samples.clone(),
            &geometry,
            &slow_timing,
            fixture.projected_serving_bytes,
        )
        .unwrap();
        assert_eq!(
            slow.causal_class,
            V23BalancedCausalClass::PageSelectorRejected
        );

        let fixture = valid_result();
        let mut layout_rejected = fixture.samples.clone();
        for query_index in 1..3 {
            layout_rejected[query_index] = layout_rejected[0].clone();
            layout_rejected[query_index].query_index = u32::try_from(query_index).unwrap();
        }
        let invalid_timing = V23BalancedTimingEvidence {
            pages_by_query: layout_rejected
                .iter()
                .map(|sample| sample.selected_pages.clone())
                .collect(),
            ..timing_evidence(&fixture, 14_000_000, false)
        };
        let invalid_simd = evaluate_v23_balanced_development(
            selected_pair(&fixture),
            layout_rejected,
            &geometry,
            &invalid_timing,
            fixture.projected_serving_bytes,
        )
        .unwrap();
        assert_eq!(
            invalid_simd.causal_class,
            V23BalancedCausalClass::AuthorityStop
        );

        let invalid_memory = evaluate_v23_balanced_development(
            selected_pair(&fixture),
            valid_result().samples,
            &geometry,
            &timing,
            MAX_SERVING_BYTES,
        )
        .unwrap();
        assert_eq!(
            invalid_memory.causal_class,
            V23BalancedCausalClass::AuthorityStop
        );

        let mut outside = valid_result();
        let mut outside_truth = outside.samples[0].ground_truth_page_assignments.clone();
        outside_truth[0] = vec![9];
        outside.samples[0] = evaluate_v23_balanced_sample(
            0,
            outside_truth,
            (0_u32..8).collect(),
            (0_u32..8).collect(),
            1_376_256,
            V23BalancedPageBudget::new(8).unwrap(),
        )
        .unwrap();
        let outside_timing = timing_evidence(&outside, 14_000_000, true);
        assert!(
            evaluate_v23_balanced_development(
                selected_pair(&outside),
                outside.samples,
                &geometry,
                &outside_timing,
                outside.projected_serving_bytes,
            )
            .is_err()
        );
    }

    #[test]
    fn v23_balanced_eval_selector_scores_top_cells_and_respects_frozen_page_budget() {
        let supercells = (0_u32..100)
            .map(|ordinal| {
                let mut centroid = [f16::from_f32(0.0); 96];
                centroid[usize::try_from(ordinal % 96).unwrap()] = f16::from_f32(1.0);
                V23SupercellRow {
                    supercell_ordinal: ordinal,
                    centroid,
                    cosine_radius: 0.0,
                    primary_rows: 1,
                    first_page: ordinal,
                    page_count: 1,
                }
            })
            .collect::<Vec<_>>();
        let pages = supercells
            .iter()
            .map(|cell| V23PageRow {
                page_ordinal: cell.supercell_ordinal,
                supercell_ordinal: cell.supercell_ordinal,
                primary_rows: 1,
                replica_rows: 0,
                centroid: cell.centroid,
                cosine_radius: 0.0,
            })
            .collect::<Vec<_>>();
        let mut query = [0.0_f32; 96];
        query[0] = 1.0;
        let geometry =
            prepare_v23_balanced_serving_geometry(&supercells, &pages, V23BalancedArm::Amp1500)
                .unwrap();
        let selected =
            select_v23_balanced_pages(&query, &geometry, V23BalancedPageBudget::new(12).unwrap())
                .unwrap();
        assert_eq!(selected.pages, [0, 96, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(selected.pages.len(), 12);
        assert_eq!(selected.scored_dimensions, (100 + 96) * 96);
        assert!(selected.scalar_simd_pages_equal);
    }

    #[test]
    fn v23_balanced_eval_timing_records_warmups_raw_samples_and_fixed_p99() {
        let supercells = (0_u32..8)
            .map(|ordinal| {
                let mut centroid = [f16::from_f32(0.0); 96];
                centroid[usize::try_from(ordinal).unwrap()] = f16::from_f32(1.0);
                V23SupercellRow {
                    supercell_ordinal: ordinal,
                    centroid,
                    cosine_radius: 0.0,
                    primary_rows: 1,
                    first_page: ordinal,
                    page_count: 1,
                }
            })
            .collect::<Vec<_>>();
        let pages = supercells
            .iter()
            .map(|cell| V23PageRow {
                page_ordinal: cell.supercell_ordinal,
                supercell_ordinal: cell.supercell_ordinal,
                primary_rows: 1,
                replica_rows: 0,
                centroid: cell.centroid,
                cosine_radius: 0.0,
            })
            .collect::<Vec<_>>();
        let mut query = [0.0_f32; 96];
        query[0] = 1.0;
        let mut second_query = [0.0_f32; 96];
        second_query[1] = 1.0;

        let geometry =
            prepare_v23_balanced_serving_geometry(&supercells, &pages, V23BalancedArm::Amp1500)
                .unwrap();
        let evidence = measure_v23_balanced_selector(
            &[query, second_query],
            &geometry,
            V23BalancedPageBudget::new(8).unwrap(),
        )
        .unwrap();
        assert_eq!(evidence.warmups, 1_024);
        assert_eq!(evidence.samples_ns.len(), 10_000);
        assert!(!evidence.samples_ns.contains(&0));
        let mut sorted = evidence.samples_ns.clone();
        sorted.sort_unstable();
        assert_eq!(evidence.p99_ns, sorted[9_899]);
        assert_eq!(
            evidence.pages_by_query,
            vec![(0_u32..8).collect::<Vec<_>>(), vec![1, 0, 2, 3, 4, 5, 6, 7],]
        );
        assert_eq!(evidence.scored_dimensions, 16 * 96);
        assert!(evidence.scalar_simd_pages_equal);
    }

    #[test]
    fn v23_balanced_eval_prepares_f32_centroids_once_and_enforces_arm_cap() {
        let mut centroid = [f16::from_f32(0.0); 96];
        centroid[0] = f16::from_f32(0.5);
        centroid[1] = f16::from_f32(0.5);
        let supercells = vec![V23SupercellRow {
            supercell_ordinal: 0,
            centroid,
            cosine_radius: 0.0,
            primary_rows: 256,
            first_page: 0,
            page_count: 8,
        }];
        let pages = (0_u32..8)
            .map(|page_ordinal| V23PageRow {
                page_ordinal,
                supercell_ordinal: 0,
                primary_rows: 32,
                replica_rows: if page_ordinal == 0 { 49 } else { 0 },
                centroid,
                cosine_radius: 0.0,
            })
            .collect::<Vec<_>>();
        assert!(
            prepare_v23_balanced_serving_geometry(&supercells, &pages, V23BalancedArm::Amp1125,)
                .is_err()
        );
        let geometry =
            prepare_v23_balanced_serving_geometry(&supercells, &pages, V23BalancedArm::Amp1250)
                .unwrap();
        assert_eq!(geometry.maximum_replica_rows, 96);
        assert!(
            (geometry.supercells[0].centroid[0] - std::f32::consts::FRAC_1_SQRT_2).abs() < 1e-6
        );
        assert!((geometry.pages[0].centroid[1] - std::f32::consts::FRAC_1_SQRT_2).abs() < 1e-6);
    }

    #[test]
    fn v23_balanced_eval_pseudoqueries_stream_bounded_leave_self_out_top_ten() {
        let mut query_zero = [0.0_f32; 96];
        query_zero[0] = 1.0;
        let mut query_one = [0.0_f32; 96];
        query_one[1] = 1.0;
        let mut accumulator =
            V23BalancedPseudoqueryAccumulator::new(vec![(0, query_zero), (1, query_one)]).unwrap();
        for source_ordinal in 0_u64..12 {
            let row = if source_ordinal % 2 == 0 {
                query_zero
            } else {
                query_one
            };
            accumulator.consider(source_ordinal, &row).unwrap();
        }
        let evidence = accumulator.finish().unwrap();
        assert_eq!(evidence.len(), 2);
        assert_eq!(
            evidence[0].neighbor_source_ordinals,
            [2, 4, 6, 8, 10, 1, 3, 5, 7, 9]
        );
        assert_eq!(
            evidence[1].neighbor_source_ordinals,
            [3, 5, 7, 9, 11, 0, 2, 4, 6, 8]
        );
        assert_eq!(evidence[0].scored_dimensions, 12 * 96);
        assert_eq!(evidence[1].scored_dimensions, 12 * 96);
        assert_eq!(evidence[0].scalar_control_dimensions, 10 * 96);
        assert_eq!(evidence[1].scalar_control_dimensions, 10 * 96);
        assert!(evidence.iter().all(|sample| sample.scalar_simd_equal));
        assert!(
            evidence
                .iter()
                .all(|sample| sample.scalar_simd_max_distance_delta_ppm <= 10)
        );
        assert_eq!(accumulator.maximum_retained_candidates(), 20);

        let mut first =
            V23BalancedPseudoqueryAccumulator::new(vec![(0, query_zero), (1, query_one)]).unwrap();
        let mut second =
            V23BalancedPseudoqueryAccumulator::new(vec![(0, query_zero), (1, query_one)]).unwrap();
        for source_ordinal in 0_u64..6 {
            let row = if source_ordinal % 2 == 0 {
                query_zero
            } else {
                query_one
            };
            first.consider(source_ordinal, &row).unwrap();
        }
        for source_ordinal in 6_u64..12 {
            let row = if source_ordinal % 2 == 0 {
                query_zero
            } else {
                query_one
            };
            second.consider(source_ordinal, &row).unwrap();
        }
        first.merge(second).unwrap();
        assert_eq!(first.finish().unwrap(), evidence);
        assert_eq!(first.maximum_retained_candidates(), 20);
    }

    #[test]
    fn v23_balanced_eval_pseudoquery_pair_recomputes_frozen_budget_gates() {
        let mut centroid = [f16::from_f32(0.0); 96];
        centroid[0] = f16::from_f32(1.0);
        let supercells = vec![V23SupercellRow {
            supercell_ordinal: 0,
            centroid,
            cosine_radius: 0.0,
            primary_rows: 13,
            first_page: 0,
            page_count: 13,
        }];
        let pages = (0_u32..13)
            .map(|page_ordinal| V23PageRow {
                page_ordinal,
                supercell_ordinal: 0,
                primary_rows: 1,
                replica_rows: 0,
                centroid,
                cosine_radius: 0.0,
            })
            .collect::<Vec<_>>();
        let geometry =
            prepare_v23_balanced_serving_geometry(&supercells, &pages, V23BalancedArm::Amp1250)
                .unwrap();
        let samples = (0_u32..1_024)
            .map(|query_index| {
                let misses_one = query_index < 11;
                evaluate_v23_balanced_sample(
                    query_index,
                    (0_u32..10)
                        .map(|rank| vec![if misses_one && rank == 9 { 12 } else { rank }])
                        .collect(),
                    (0_u32..13).collect(),
                    (0_u32..12).collect(),
                    1_376_256,
                    V23BalancedPageBudget::new(12).unwrap(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let selected_pair = V23BalancedSelectedPair {
            page_budget: V23BalancedPageBudget::new(12).unwrap(),
            arm: V23BalancedArm::Amp1250,
        };
        let pair =
            evaluate_v23_balanced_pseudoquery_pair(selected_pair, &samples, &geometry).unwrap();
        assert_eq!(pair.aggregate_recall_ppm, 998_925);
        assert_eq!(pair.minimum_recall_ppm, 900_000);
        assert_eq!(pair.oracle_attainment_ppm, 998_925);
        assert!(pair.every_query_has_budget);
        assert_eq!(pair.projected_page_bytes, 12 * 122_880);
        assert_eq!(pair.maximum_scored_dimensions, 1_376_256);
        assert!(pair.amplification_and_page_caps_valid);

        let mut drift = samples;
        drift[0].selector_hits = 10;
        assert!(evaluate_v23_balanced_pseudoquery_pair(selected_pair, &drift, &geometry,).is_err());
    }
}
