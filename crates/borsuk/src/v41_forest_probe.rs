//! Throwaway algorithm-first hyperplane-forest coverage probe.

use crate::error::{BorsukError, Result};
use crate::v37_relation_router::{V37BalancedTree, V37TrainingShape};
use crate::v38_boundary_spill::v38_v40_owner_rows_from_artifacts;
use rayon::{ThreadPoolBuilder, prelude::*};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::{self, File},
    path::PathBuf,
};

const SOURCE_ROWS: usize = 1_000_000;
const QUERY_ROWS: usize = 1_000;
const PAGE_COUNT: u32 = 123;
const MAXIMUM_ROWS_PER_PAGE: u32 = 10_240;
const TREE_COUNT: usize = 8;
const LEAF_COUNT: u64 = 1_954;
const VISITED_LEAVES: usize = 32;
const PAIRS_PER_LEAF: usize = 64;
const SELECTED_PAGES: usize = 21;
const CANDIDATE_WIDTHS: [usize; 5] = [64, 80, 96, 112, 123];
const LEAF_LIMITS: [usize; 6] = [1, 2, 4, 8, 16, 32];
const CALIBRATION_MIX_PPM: [u32; 4] = [1_000_000, 750_000, 500_000, 250_000];
const LOCAL_MODES: usize = 4;
const MODE_LLOYD_ITERATIONS: usize = 20;
const MODE_RANKED_PAIRS: usize = 256;
const PROJECTED_DIMENSIONS: usize = 192;
const MASS_FEATURES: usize = 8;
const MODE_FEATURES: usize = 10;
const MODE_HIDDEN_ONE: usize = 32;
const MODE_HIDDEN_TWO: usize = 16;
const PAGE_FEATURES: usize = 4;
const PAGE_HIDDEN: usize = 8;
const NONLINEAR_PARAMETERS: usize = MODE_FEATURES * MODE_HIDDEN_ONE
    + MODE_HIDDEN_ONE
    + MODE_HIDDEN_ONE * MODE_HIDDEN_TWO
    + MODE_HIDDEN_TWO
    + MODE_HIDDEN_TWO
    + 1
    + PAGE_FEATURES * PAGE_HIDDEN
    + PAGE_HIDDEN
    + PAGE_HIDDEN
    + 1;
const NONLINEAR_EPOCHS: usize = 50;
const NONLINEAR_BATCH: usize = 32;
const OPTIMIZATION_QUERY_STRIDE: usize = 4;
const SOFT_SELECTION_TEMPERATURE: f64 = 1.0;
const DIRECTIONAL_RANK: usize = 8;
const DIRECTIONAL_U: usize = NONLINEAR_PARAMETERS;
const DIRECTIONAL_V: usize = DIRECTIONAL_U + PROJECTED_DIMENSIONS * DIRECTIONAL_RANK;
const DIRECTIONAL_PARAMETERS: usize =
    NONLINEAR_PARAMETERS + 2 * PROJECTED_DIMENSIONS * DIRECTIONAL_RANK;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

/// Local inputs for the one-million-row forest coverage spike.
#[doc(hidden)]
pub struct V41ForestProbeRequest {
    pub source: PathBuf,
    pub training_query: PathBuf,
    pub training_ground_truth: PathBuf,
    pub query: PathBuf,
    pub ground_truth: PathBuf,
    pub spill_relation: PathBuf,
    pub spill_postings: PathBuf,
    pub initial_model: PathBuf,
    pub workers: usize,
}

#[derive(Serialize)]
struct Screen {
    aggregate_recall_ppm: u32,
    minimum_recall_ppm: u32,
    passed: bool,
}

#[derive(Serialize)]
struct CandidateWidthScreen {
    candidate_pages: usize,
    upper: Screen,
}

#[derive(Serialize)]
struct LeafLimitScreen {
    visited_leaves_per_tree: usize,
    candidate_pages: usize,
    weighted_overlap_greedy: Screen,
}

#[derive(Serialize)]
struct CalibrationScreen {
    calibrated_weight_ppm: u32,
    candidate_pages: usize,
    weighted_overlap_greedy: Screen,
}

#[derive(Serialize)]
struct LocalModeScreen {
    modes_per_group: usize,
    ranked_pairs: usize,
    candidate_pages: usize,
    selected: Screen,
    tree_rrf_singleton: Screen,
    restricted_gt_singleton_mass: Screen,
    restricted_gt_greedy: Screen,
}

#[derive(Serialize)]
struct ProbeResult {
    schema: &'static str,
    claim_eligible: bool,
    source_rows: usize,
    query_rows: usize,
    trees: usize,
    leaves_per_tree: u64,
    visited_leaves_per_tree: usize,
    retained_pairs_per_leaf: usize,
    selected_pages: usize,
    layout_gt_greedy: Screen,
    retained_union_upper: Screen,
    candidate_width_screens: Vec<CandidateWidthScreen>,
    candidate96_gt_greedy: Screen,
    leaf_limit_screens: Vec<LeafLimitScreen>,
    calibration_screens: Vec<CalibrationScreen>,
    local_mode_groups: usize,
    local_mode_centers: usize,
    local_mode_bytes: usize,
    activated_mode_groups_p50: usize,
    activated_mode_groups_p95: usize,
    activated_mode_groups_max: usize,
    local_mode_screen: LocalModeScreen,
    learned_mass_development: Screen,
    learned_mass_validation: Option<Screen>,
    learned_mass_weights: [f64; MASS_FEATURES],
}

#[derive(Serialize)]
struct NonlinearProbeResult {
    schema: &'static str,
    claim_eligible: bool,
    source_rows: usize,
    development_query_rows: usize,
    optimization_query_rows: usize,
    validation_query_rows: usize,
    trees: usize,
    leaves_per_tree: u64,
    visited_leaves_per_tree: usize,
    retained_pairs_per_leaf: usize,
    modes_per_group: usize,
    ranked_pairs: usize,
    candidate_pages: usize,
    selected_pages: usize,
    control_parameter_count: usize,
    directional_parameter_count: usize,
    epochs: usize,
    objective: &'static str,
    soft_selection_temperature: f64,
    control_best_union_loss: f64,
    control_development: Screen,
    directional_best_union_loss: Option<f64>,
    directional_development: Option<Screen>,
    development_singleton_oracle: Screen,
    validation: Option<Screen>,
    stopped_before_validation: bool,
    local_mode_groups: usize,
    local_mode_centers: usize,
    local_mode_bytes: usize,
    winning_arm: Option<&'static str>,
}

type OwnerPair = (u32, u32);
type LeafSummary = Vec<(OwnerPair, u32)>;
type CalibratedLeafSummary = Vec<(OwnerPair, f64)>;

struct LocalModeGroup {
    pair: OwnerPair,
    centers: Vec<f32>,
    populations: Vec<u32>,
    radius_means: Vec<f32>,
    radius_variances: Vec<f32>,
    center_norms: Vec<f32>,
}

type LocalModeLeaf = Vec<LocalModeGroup>;

struct ModeObservation {
    values: [f64; MODE_FEATURES],
    population: f64,
    tree: usize,
    pages: [u32; 2],
    leaf: usize,
    group: usize,
    mode: usize,
}

struct NonlinearExample {
    observations: Vec<ModeObservation>,
    candidates: Vec<u32>,
    truth: Vec<u64>,
    query: [f32; PROJECTED_DIMENSIONS],
}

fn screen(hits: &[u32]) -> Screen {
    let total = hits.iter().map(|value| u64::from(*value)).sum::<u64>();
    let aggregate = u32::try_from(total * 1_000_000 / (hits.len() as u64 * 100)).unwrap();
    let minimum = hits.iter().copied().min().unwrap() * 10_000;
    Screen {
        aggregate_recall_ppm: aggregate,
        minimum_recall_ppm: minimum,
        passed: aggregate >= 998_000 && minimum >= 800_000,
    }
}

fn mode_screen(hits: &[u32]) -> Screen {
    let mut result = screen(hits);
    result.passed = result.aggregate_recall_ppm >= 995_000 && result.minimum_recall_ppm >= 800_000;
    result
}

fn squared_distance(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let delta = left - right;
            delta * delta
        })
        .sum()
}

fn normalized_projected_query(query: &[f32]) -> [f32; PROJECTED_DIMENSIONS] {
    let norm = query
        .iter()
        .map(|value| value * value)
        .sum::<f32>()
        .sqrt()
        .max(1.0e-12);
    std::array::from_fn(|dimension| query[dimension] / norm)
}

fn local_group_modes(pair: OwnerPair, rows: &[u32], coordinates: &[f32]) -> LocalModeGroup {
    let row = |ordinal: u32| {
        let start = ordinal as usize * PROJECTED_DIMENSIONS;
        &coordinates[start..start + PROJECTED_DIMENSIONS]
    };
    let mode_count = LOCAL_MODES.min(rows.len());
    let mut mean = vec![0.0_f64; PROJECTED_DIMENSIONS];
    for &ordinal in rows {
        for (sum, value) in mean.iter_mut().zip(row(ordinal)) {
            *sum += f64::from(*value);
        }
    }
    let inverse = 1.0 / rows.len() as f64;
    let mean = mean
        .into_iter()
        .map(|value| (value * inverse) as f32)
        .collect::<Vec<_>>();
    let mut chosen = vec![
        *rows
            .iter()
            .min_by(|left, right| {
                squared_distance(row(**left), &mean)
                    .total_cmp(&squared_distance(row(**right), &mean))
                    .then_with(|| left.cmp(right))
            })
            .unwrap(),
    ];
    while chosen.len() < mode_count {
        let next = rows
            .iter()
            .copied()
            .filter(|ordinal| !chosen.contains(ordinal))
            .max_by(|left, right| {
                let nearest = |ordinal| {
                    chosen
                        .iter()
                        .map(|center| squared_distance(row(ordinal), row(*center)))
                        .min_by(f32::total_cmp)
                        .unwrap()
                };
                nearest(*left)
                    .total_cmp(&nearest(*right))
                    .then_with(|| right.cmp(left))
            })
            .unwrap();
        chosen.push(next);
    }
    let mut centers = chosen
        .iter()
        .flat_map(|ordinal| row(*ordinal).iter().copied())
        .collect::<Vec<_>>();
    if rows.len() > mode_count {
        let mut assignments = vec![usize::MAX; rows.len()];
        for _ in 0..MODE_LLOYD_ITERATIONS {
            let mut changed = false;
            let mut counts = vec![0_usize; mode_count];
            let mut sums = vec![0.0_f64; mode_count * PROJECTED_DIMENSIONS];
            for (row_index, &ordinal) in rows.iter().enumerate() {
                let vector = row(ordinal);
                let mode = (0..mode_count)
                    .min_by(|left, right| {
                        let left_start = left * PROJECTED_DIMENSIONS;
                        let right_start = right * PROJECTED_DIMENSIONS;
                        squared_distance(
                            vector,
                            &centers[left_start..left_start + PROJECTED_DIMENSIONS],
                        )
                        .total_cmp(&squared_distance(
                            vector,
                            &centers[right_start..right_start + PROJECTED_DIMENSIONS],
                        ))
                        .then_with(|| left.cmp(right))
                    })
                    .unwrap();
                changed |= assignments[row_index] != mode;
                assignments[row_index] = mode;
                counts[mode] += 1;
                let start = mode * PROJECTED_DIMENSIONS;
                for (sum, value) in sums[start..start + PROJECTED_DIMENSIONS]
                    .iter_mut()
                    .zip(vector)
                {
                    *sum += f64::from(*value);
                }
            }
            for mode in 0..mode_count {
                if counts[mode] == 0 {
                    continue;
                }
                let start = mode * PROJECTED_DIMENSIONS;
                let inverse = 1.0 / counts[mode] as f64;
                for (center, sum) in centers[start..start + PROJECTED_DIMENSIONS]
                    .iter_mut()
                    .zip(&sums[start..start + PROJECTED_DIMENSIONS])
                {
                    *center = (*sum * inverse) as f32;
                }
            }
            if !changed {
                break;
            }
        }
    }
    let mut populations = vec![0_u32; mode_count];
    let mut radius_sums = vec![0.0_f64; mode_count];
    let mut radius_square_sums = vec![0.0_f64; mode_count];
    for &ordinal in rows {
        let vector = row(ordinal);
        let (mode, distance) = (0..mode_count)
            .map(|mode| {
                let start = mode * PROJECTED_DIMENSIONS;
                (
                    mode,
                    squared_distance(vector, &centers[start..start + PROJECTED_DIMENSIONS]),
                )
            })
            .min_by(|left, right| {
                left.1
                    .total_cmp(&right.1)
                    .then_with(|| left.0.cmp(&right.0))
            })
            .unwrap();
        populations[mode] += 1;
        radius_sums[mode] += f64::from(distance);
        radius_square_sums[mode] += f64::from(distance) * f64::from(distance);
    }
    let radius_means = populations
        .iter()
        .zip(&radius_sums)
        .map(|(count, sum)| {
            if *count == 0 {
                0.0
            } else {
                (sum / f64::from(*count)) as f32
            }
        })
        .collect::<Vec<_>>();
    let radius_variances = populations
        .iter()
        .zip(&radius_sums)
        .zip(&radius_square_sums)
        .map(|((count, sum), square_sum)| {
            if *count == 0 {
                return 0.0;
            }
            let mean = sum / f64::from(*count);
            (square_sum / f64::from(*count) - mean * mean).max(0.0) as f32
        })
        .collect::<Vec<_>>();
    let center_norms = centers
        .chunks_exact(PROJECTED_DIMENSIONS)
        .map(|center| center.iter().map(|value| value * value).sum::<f32>().sqrt())
        .collect();
    LocalModeGroup {
        pair,
        centers,
        populations,
        radius_means,
        radius_variances,
        center_norms,
    }
}

fn gt_hits(
    truth: &[u64],
    owners: &BTreeMap<u64, OwnerPair>,
    selected: &BTreeSet<u32>,
) -> Result<u32> {
    let mut hits = 0_u32;
    for feature in truth {
        let &(primary, alternate) = owners
            .get(feature)
            .ok_or_else(|| invalid("V41 forest truth owner differs"))?;
        if selected.contains(&primary) || (alternate != u32::MAX && selected.contains(&alternate)) {
            hits += 1;
        }
    }
    Ok(hits)
}

fn gt_greedy(
    truth: &[u64],
    owners: &BTreeMap<u64, OwnerPair>,
    allowed: &BTreeSet<u32>,
) -> Result<BTreeSet<u32>> {
    let mut selected = BTreeSet::new();
    while selected.len() < SELECTED_PAGES {
        let mut best = None;
        for page in allowed
            .iter()
            .copied()
            .filter(|page| !selected.contains(page))
        {
            let mut gain = 0_u32;
            for feature in truth {
                let &(primary, alternate) = owners
                    .get(feature)
                    .ok_or_else(|| invalid("V41 forest truth owner differs"))?;
                if selected.contains(&primary)
                    || (alternate != u32::MAX && selected.contains(&alternate))
                {
                    continue;
                }
                if page == primary || page == alternate {
                    gain += 1;
                }
            }
            if best.is_none_or(|(best_gain, best_page)| {
                (gain, std::cmp::Reverse(page)) > (best_gain, std::cmp::Reverse(best_page))
            }) {
                best = Some((gain, page));
            }
        }
        let Some((_, page)) = best else { break };
        selected.insert(page);
    }
    Ok(selected)
}

fn gt_singleton_mass(
    truth: &[u64],
    owners: &BTreeMap<u64, OwnerPair>,
    allowed: &BTreeSet<u32>,
) -> Result<BTreeSet<u32>> {
    let mut masses = BTreeMap::<u32, u32>::new();
    for feature in truth {
        let &(primary, alternate) = owners
            .get(feature)
            .ok_or_else(|| invalid("V41 forest truth owner differs"))?;
        for page in [primary, alternate] {
            if page != u32::MAX && allowed.contains(&page) {
                *masses.entry(page).or_default() += 1;
            }
        }
    }
    let mut ranked = masses.into_iter().collect::<Vec<_>>();
    ranked.sort_unstable_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    ranked.truncate(SELECTED_PAGES);
    Ok(ranked.into_iter().map(|(page, _)| page).collect())
}

fn retained_leaf_summaries(
    tree: &V37BalancedTree,
    feature_ids: &[u64],
    owners: &BTreeMap<u64, OwnerPair>,
) -> Result<Vec<LeafSummary>> {
    let mut counts = (0..tree.leaf_populations.len())
        .map(|_| HashMap::<OwnerPair, u32>::new())
        .collect::<Vec<_>>();
    for assignment in &tree.assignments {
        let feature = *feature_ids
            .get(
                usize::try_from(assignment.source_ordinal)
                    .map_err(|_| invalid("V41 forest source ordinal differs"))?,
            )
            .ok_or_else(|| invalid("V41 forest source ordinal differs"))?;
        let pair = *owners
            .get(&feature)
            .ok_or_else(|| invalid("V41 forest source owner differs"))?;
        let leaf = counts
            .get_mut(assignment.posting_ordinal as usize)
            .ok_or_else(|| invalid("V41 forest leaf differs"))?;
        *leaf.entry(pair).or_default() += 1;
    }
    Ok(counts
        .into_iter()
        .map(|leaf| {
            let mut entries = leaf.into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| {
                right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0))
            });
            entries.truncate(PAIRS_PER_LEAF);
            entries
        })
        .collect())
}

fn local_mode_summaries(
    tree: &V37BalancedTree,
    summaries: &[LeafSummary],
    feature_ids: &[u64],
    owners: &BTreeMap<u64, OwnerPair>,
    coordinates: &[f32],
    workers: usize,
) -> Result<Vec<LocalModeLeaf>> {
    let mut members = summaries
        .iter()
        .map(|leaf| leaf.iter().map(|_| Vec::<u32>::new()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let indexes = summaries
        .iter()
        .map(|leaf| {
            leaf.iter()
                .enumerate()
                .map(|(index, (pair, _))| (*pair, index))
                .collect::<HashMap<_, _>>()
        })
        .collect::<Vec<_>>();
    for assignment in &tree.assignments {
        let row = usize::try_from(assignment.source_ordinal)
            .map_err(|_| invalid("V41 mode source ordinal differs"))?;
        let feature = *feature_ids
            .get(row)
            .ok_or_else(|| invalid("V41 mode source ordinal differs"))?;
        let pair = owners
            .get(&feature)
            .ok_or_else(|| invalid("V41 mode source owner differs"))?;
        let leaf = assignment.posting_ordinal as usize;
        if let Some(index) = indexes[leaf].get(pair) {
            members[leaf][*index].push(
                u32::try_from(assignment.source_ordinal)
                    .map_err(|_| invalid("V41 mode source ordinal differs"))?,
            );
        }
    }
    let pool = ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .map_err(|_| invalid("V41 mode worker count differs"))?;
    Ok(pool.install(|| {
        summaries
            .par_iter()
            .zip(members.par_iter())
            .map(|(leaf, members)| {
                leaf.iter()
                    .zip(members)
                    .map(|((pair, _), rows)| local_group_modes(*pair, rows, coordinates))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    }))
}

fn local_mode_pair_ranking(
    trees: &[V37BalancedTree],
    summaries: &[Vec<LocalModeLeaf>],
    query: &[f32],
) -> Result<(Vec<OwnerPair>, usize)> {
    let mut distances = BTreeMap::<OwnerPair, f32>::new();
    let mut activated = 0_usize;
    for (tree, leaves) in trees.iter().zip(summaries) {
        let selection = crate::v37_relation_router::select_v37_tree_postings_with_limit(
            tree,
            &tree.fma_backend,
            tree.nodes.len().min(1_024),
            VISITED_LEAVES,
            query,
        )?;
        for leaf in &selection.selected_postings {
            for group in &leaves[*leaf as usize] {
                activated += 1;
                let distance = group
                    .centers
                    .chunks_exact(PROJECTED_DIMENSIONS)
                    .map(|center| squared_distance(query, center))
                    .min_by(f32::total_cmp)
                    .unwrap();
                distances
                    .entry(group.pair)
                    .and_modify(|current| *current = current.min(distance))
                    .or_insert(distance);
            }
        }
    }
    let mut ranked = distances.into_iter().collect::<Vec<_>>();
    ranked.sort_unstable_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    ranked.truncate(MODE_RANKED_PAIRS);
    Ok((
        ranked.into_iter().map(|(pair, _)| pair).collect(),
        activated,
    ))
}

fn local_mode_tree_rrf(
    trees: &[V37BalancedTree],
    summaries: &[Vec<LocalModeLeaf>],
    query: &[f32],
) -> Result<BTreeMap<OwnerPair, f64>> {
    let mut fused = BTreeMap::<OwnerPair, f64>::new();
    for (tree, leaves) in trees.iter().zip(summaries) {
        let selection = crate::v37_relation_router::select_v37_tree_postings_with_limit(
            tree,
            &tree.fma_backend,
            tree.nodes.len().min(1_024),
            VISITED_LEAVES,
            query,
        )?;
        let mut distances = BTreeMap::<OwnerPair, f32>::new();
        for leaf in &selection.selected_postings {
            for group in &leaves[*leaf as usize] {
                let distance = group
                    .centers
                    .chunks_exact(PROJECTED_DIMENSIONS)
                    .map(|center| squared_distance(query, center))
                    .min_by(f32::total_cmp)
                    .unwrap();
                distances
                    .entry(group.pair)
                    .and_modify(|current| *current = current.min(distance))
                    .or_insert(distance);
            }
        }
        let mut ranked = distances.into_iter().collect::<Vec<_>>();
        ranked.sort_unstable_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        ranked.truncate(MODE_RANKED_PAIRS);
        for (rank, (pair, _)) in ranked.into_iter().enumerate() {
            *fused.entry(pair).or_default() += 1.0 / (60 + rank + 1) as f64;
        }
    }
    let mut ranked = fused.into_iter().collect::<Vec<_>>();
    ranked.sort_unstable_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    ranked.truncate(MODE_RANKED_PAIRS);
    Ok(ranked.into_iter().collect())
}

fn local_mode_page_features(
    trees: &[V37BalancedTree],
    summaries: &[Vec<LocalModeLeaf>],
    query: &[f32],
    candidates: &BTreeSet<u32>,
) -> Result<BTreeMap<u32, [f64; MASS_FEATURES]>> {
    let mut aggregate = candidates
        .iter()
        .map(|page| (*page, [0.0; MASS_FEATURES]))
        .collect::<BTreeMap<_, _>>();
    for (tree, leaves) in trees.iter().zip(summaries) {
        let selection = crate::v37_relation_router::select_v37_tree_postings_with_limit(
            tree,
            &tree.fma_backend,
            tree.nodes.len().min(1_024),
            VISITED_LEAVES,
            query,
        )?;
        let mut per_tree = BTreeMap::<u32, [f64; 5]>::new();
        for leaf in &selection.selected_postings {
            for group in &leaves[*leaf as usize] {
                let mut evidence = 0.0_f64;
                let mut minimum_z = f64::INFINITY;
                let mut population = 0_u32;
                let mut uncertainty = 0.0_f64;
                for (mode, center) in group.centers.chunks_exact(PROJECTED_DIMENSIONS).enumerate() {
                    let count = group.populations[mode];
                    if count == 0 {
                        continue;
                    }
                    let radius = f64::from(group.radius_means[mode]);
                    let dispersion = f64::from(group.radius_variances[mode]).sqrt();
                    let scale = (radius + dispersion).max(1.0e-6);
                    let z = f64::from(squared_distance(query, center)) / scale;
                    evidence += f64::from(count) / (1.0 + z);
                    minimum_z = minimum_z.min(z);
                    population += count;
                    uncertainty += f64::from(count) * dispersion / scale;
                }
                for page in [group.pair.0, group.pair.1] {
                    if page == u32::MAX || !candidates.contains(&page) {
                        continue;
                    }
                    let entry = per_tree
                        .entry(page)
                        .or_insert([0.0, 0.0, 0.0, f64::INFINITY, 0.0]);
                    entry[0] += evidence;
                    entry[1] += f64::from(population);
                    entry[2] += 1.0;
                    entry[3] = entry[3].min(minimum_z);
                    entry[4] += uncertainty;
                }
            }
        }
        for (page, tree_values) in per_tree {
            let entry = aggregate.get_mut(&page).unwrap();
            entry[1] += tree_values[0];
            entry[2] = entry[2].max(tree_values[0]);
            entry[3] += 1.0;
            entry[4] = if entry[4] == 0.0 {
                tree_values[3]
            } else {
                entry[4].min(tree_values[3])
            };
            entry[5] += tree_values[1];
            entry[6] += tree_values[2];
            entry[7] += tree_values[4];
        }
    }
    for features in aggregate.values_mut() {
        features[0] = 1.0;
        features[1] = features[1].ln_1p();
        features[2] = features[2].ln_1p();
        features[3] /= TREE_COUNT as f64;
        features[4] = -features[4].ln_1p();
        features[5] = features[5].ln_1p();
        features[6] = features[6].ln_1p();
        features[7] = features[7].ln_1p();
    }
    Ok(aggregate)
}

fn page_truth_masses(
    truth: &[u64],
    owners: &BTreeMap<u64, OwnerPair>,
) -> Result<BTreeMap<u32, u32>> {
    let mut masses = BTreeMap::new();
    for feature in truth {
        let pair = owners
            .get(feature)
            .ok_or_else(|| invalid("V41 mass truth owner differs"))?;
        for page in [pair.0, pair.1] {
            if page != u32::MAX {
                *masses.entry(page).or_default() += 1;
            }
        }
    }
    Ok(masses)
}

fn fit_mass_model(examples: &[([f64; MASS_FEATURES], f64)]) -> [f64; MASS_FEATURES] {
    let mut matrix = [[0.0_f64; MASS_FEATURES]; MASS_FEATURES];
    let mut target = [0.0_f64; MASS_FEATURES];
    for (features, mass) in examples {
        let response = mass.ln_1p();
        for row in 0..MASS_FEATURES {
            target[row] += features[row] * response;
            for column in 0..MASS_FEATURES {
                matrix[row][column] += features[row] * features[column];
            }
        }
    }
    for index in 1..MASS_FEATURES {
        matrix[index][index] += 1.0;
    }
    for pivot in 0..MASS_FEATURES {
        let best = (pivot..MASS_FEATURES)
            .max_by(|left, right| {
                matrix[*left][pivot]
                    .abs()
                    .total_cmp(&matrix[*right][pivot].abs())
            })
            .unwrap();
        matrix.swap(pivot, best);
        target.swap(pivot, best);
        let divisor = matrix[pivot][pivot];
        for column in pivot..MASS_FEATURES {
            matrix[pivot][column] /= divisor;
        }
        target[pivot] /= divisor;
        for row in 0..MASS_FEATURES {
            if row == pivot {
                continue;
            }
            let factor = matrix[row][pivot];
            for column in pivot..MASS_FEATURES {
                matrix[row][column] -= factor * matrix[pivot][column];
            }
            target[row] -= factor * target[pivot];
        }
    }
    target
}

fn select_mass_pages(
    features: &BTreeMap<u32, [f64; MASS_FEATURES]>,
    weights: &[f64; MASS_FEATURES],
) -> BTreeSet<u32> {
    let scores = features
        .iter()
        .map(|(page, values)| (*page, values.iter().zip(weights).map(|(x, w)| x * w).sum()))
        .collect::<BTreeMap<_, _>>();
    top_pages(&scores, SELECTED_PAGES)
}

const MODE_W1: usize = 0;
const MODE_B1: usize = MODE_W1 + MODE_FEATURES * MODE_HIDDEN_ONE;
const MODE_W2: usize = MODE_B1 + MODE_HIDDEN_ONE;
const MODE_B2: usize = MODE_W2 + MODE_HIDDEN_ONE * MODE_HIDDEN_TWO;
const MODE_W3: usize = MODE_B2 + MODE_HIDDEN_TWO;
const MODE_B3: usize = MODE_W3 + MODE_HIDDEN_TWO;
const PAGE_W1: usize = MODE_B3 + 1;
const PAGE_B1: usize = PAGE_W1 + PAGE_FEATURES * PAGE_HIDDEN;
const PAGE_W2: usize = PAGE_B1 + PAGE_HIDDEN;
const PAGE_B2: usize = PAGE_W2 + PAGE_HIDDEN;

fn sigmoid(value: f64) -> f64 {
    if value >= 0.0 {
        1.0 / (1.0 + (-value).exp())
    } else {
        let exponential = value.exp();
        exponential / (1.0 + exponential)
    }
}

fn silu(value: f64) -> f64 {
    value * sigmoid(value)
}

fn silu_derivative(value: f64) -> f64 {
    let probability = sigmoid(value);
    probability * (1.0 + value * (1.0 - probability))
}

fn softplus(value: f64) -> f64 {
    if value > 30.0 {
        value
    } else if value < -30.0 {
        value.exp()
    } else {
        value.exp().ln_1p()
    }
}

fn mode_forward(
    parameters: &[f64],
    values: &[f64; MODE_FEATURES],
) -> (f64, [f64; MODE_HIDDEN_ONE], [f64; MODE_HIDDEN_TWO]) {
    let mut hidden_one = [0.0; MODE_HIDDEN_ONE];
    for hidden in 0..MODE_HIDDEN_ONE {
        let mut value = parameters[MODE_B1 + hidden];
        for input in 0..MODE_FEATURES {
            value += parameters[MODE_W1 + hidden * MODE_FEATURES + input] * values[input];
        }
        hidden_one[hidden] = silu(value);
    }
    let mut hidden_two = [0.0; MODE_HIDDEN_TWO];
    for hidden in 0..MODE_HIDDEN_TWO {
        let mut value = parameters[MODE_B2 + hidden];
        for input in 0..MODE_HIDDEN_ONE {
            value += parameters[MODE_W2 + hidden * MODE_HIDDEN_ONE + input] * hidden_one[input];
        }
        hidden_two[hidden] = silu(value);
    }
    let mut output = parameters[MODE_B3];
    for hidden in 0..MODE_HIDDEN_TWO {
        output += parameters[MODE_W3 + hidden] * hidden_two[hidden];
    }
    (output, hidden_one, hidden_two)
}

fn page_forward(
    parameters: &[f64],
    values: &[f64; PAGE_FEATURES],
) -> (f64, f64, [f64; PAGE_HIDDEN], [f64; PAGE_HIDDEN]) {
    let mut pre = [0.0; PAGE_HIDDEN];
    let mut hidden = [0.0; PAGE_HIDDEN];
    for unit in 0..PAGE_HIDDEN {
        let mut value = parameters[PAGE_B1 + unit];
        for input in 0..PAGE_FEATURES {
            value += parameters[PAGE_W1 + unit * PAGE_FEATURES + input] * values[input];
        }
        pre[unit] = value;
        hidden[unit] = silu(value);
    }
    let mut output = parameters[PAGE_B2];
    for unit in 0..PAGE_HIDDEN {
        output += parameters[PAGE_W2 + unit] * hidden[unit];
    }
    (softplus(output) + 1.0e-6, output, pre, hidden)
}

fn nonlinear_mode_observations(
    trees: &[V37BalancedTree],
    summaries: &[Vec<LocalModeLeaf>],
    query: &[f32],
    candidates: &BTreeSet<u32>,
) -> Result<Vec<ModeObservation>> {
    struct RawMode {
        pair: OwnerPair,
        tree: usize,
        leaf_rank: usize,
        population: u32,
        distance: f64,
        radius: f64,
        variance: f64,
        leaf: usize,
        group: usize,
        mode: usize,
    }
    let mut raw = Vec::new();
    for (tree_index, (tree, leaves)) in trees.iter().zip(summaries).enumerate() {
        let selection = crate::v37_relation_router::select_v37_tree_postings_with_limit(
            tree,
            &tree.fma_backend,
            tree.nodes.len().min(1_024),
            VISITED_LEAVES,
            query,
        )?;
        for (leaf_rank, leaf) in selection.selected_postings.iter().enumerate() {
            for (group_index, group) in leaves[*leaf as usize].iter().enumerate() {
                for (mode, center) in group.centers.chunks_exact(PROJECTED_DIMENSIONS).enumerate() {
                    if group.populations[mode] == 0 {
                        continue;
                    }
                    raw.push(RawMode {
                        pair: group.pair,
                        tree: tree_index,
                        leaf_rank,
                        population: group.populations[mode],
                        distance: f64::from(squared_distance(query, center)),
                        radius: f64::from(group.radius_means[mode]),
                        variance: f64::from(group.radius_variances[mode]),
                        leaf: *leaf as usize,
                        group: group_index,
                        mode,
                    });
                }
            }
        }
    }
    raw.sort_unstable_by(|left, right| {
        left.distance
            .total_cmp(&right.distance)
            .then_with(|| left.pair.cmp(&right.pair))
            .then_with(|| left.tree.cmp(&right.tree))
    });
    let minimum = raw.first().map_or(0.0, |mode| mode.distance);
    let scale = raw
        .get(raw.len() / 2)
        .map_or(1.0, |mode| mode.distance)
        .max(1.0e-12);
    let mut pair_minimum = BTreeMap::<OwnerPair, f64>::new();
    for mode in &raw {
        pair_minimum
            .entry(mode.pair)
            .and_modify(|value| *value = value.min(mode.distance))
            .or_insert(mode.distance);
    }
    let mut ranked_pairs = pair_minimum.into_iter().collect::<Vec<_>>();
    ranked_pairs.sort_unstable_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    ranked_pairs.truncate(MODE_RANKED_PAIRS);
    let retained = ranked_pairs
        .into_iter()
        .map(|(pair, _)| pair)
        .collect::<BTreeSet<_>>();
    Ok(raw
        .into_iter()
        .enumerate()
        .filter(|(_, mode)| retained.contains(&mode.pair))
        .filter(|(_, mode)| {
            [mode.pair.0, mode.pair.1]
                .into_iter()
                .any(|page| page != u32::MAX && candidates.contains(&page))
        })
        .map(|(rank, mode)| {
            let radius = mode.radius.max(1.0e-12);
            let uncertainty = mode.variance.max(0.0).sqrt() / radius;
            let z = mode.distance / radius;
            ModeObservation {
                values: [
                    f64::from(mode.population).ln_1p(),
                    mode.distance,
                    radius.ln(),
                    uncertainty,
                    z,
                    z.ln_1p(),
                    (rank as f64 + 1.0).ln(),
                    (mode.leaf_rank as f64 + 1.0).ln(),
                    (mode.distance - minimum) / scale,
                    scale.ln(),
                ],
                population: f64::from(mode.population),
                tree: mode.tree,
                pages: [mode.pair.0, mode.pair.1],
                leaf: mode.leaf,
                group: mode.group,
                mode: mode.mode,
            }
        })
        .collect())
}

fn apply_observation_standardization(
    examples: &mut [NonlinearExample],
    means: &[f64; MODE_FEATURES],
    scales: &[f64; MODE_FEATURES],
) {
    for observation in examples
        .iter_mut()
        .flat_map(|example| &mut example.observations)
    {
        for index in 0..MODE_FEATURES {
            observation.values[index] = (observation.values[index] - means[index]) / scales[index];
        }
    }
}

fn standardize_observations(
    examples: &mut [NonlinearExample],
) -> ([f64; MODE_FEATURES], [f64; MODE_FEATURES]) {
    let mut means = [0.0; MODE_FEATURES];
    let mut count = 0.0;
    for observation in examples.iter().flat_map(|example| &example.observations) {
        count += 1.0;
        for (mean, value) in means.iter_mut().zip(observation.values) {
            *mean += value;
        }
    }
    for mean in &mut means {
        *mean /= count;
    }
    let mut scales = [0.0; MODE_FEATURES];
    for observation in examples.iter().flat_map(|example| &example.observations) {
        for index in 0..MODE_FEATURES {
            scales[index] += (observation.values[index] - means[index]).powi(2);
        }
    }
    for scale in &mut scales {
        *scale = (*scale / count).sqrt().max(1.0e-9);
    }
    apply_observation_standardization(examples, &means, &scales);
    (means, scales)
}

fn directional_mode_logit(
    parameters: &[f64],
    observation: &ModeObservation,
    example: &NonlinearExample,
    summaries: &[Vec<LocalModeLeaf>],
) -> f64 {
    let scalar = mode_forward(parameters, &observation.values).0;
    if parameters.len() == NONLINEAR_PARAMETERS {
        return scalar;
    }
    let group = &summaries[observation.tree][observation.leaf][observation.group];
    let center_start = observation.mode * PROJECTED_DIMENSIONS;
    let center = &group.centers[center_start..center_start + PROJECTED_DIMENSIONS];
    let inverse_norm = 1.0 / f64::from(group.center_norms[observation.mode].max(1.0e-12));
    let mut query_projection = [0.0; DIRECTIONAL_RANK];
    let mut center_projection = [0.0; DIRECTIONAL_RANK];
    for rank in 0..DIRECTIONAL_RANK {
        for dimension in 0..PROJECTED_DIMENSIONS {
            query_projection[rank] += parameters
                [DIRECTIONAL_U + dimension * DIRECTIONAL_RANK + rank]
                * f64::from(example.query[dimension]);
            center_projection[rank] += parameters
                [DIRECTIONAL_V + dimension * DIRECTIONAL_RANK + rank]
                * f64::from(center[dimension])
                * inverse_norm;
        }
    }
    scalar
        + query_projection
            .iter()
            .zip(center_projection)
            .map(|(query, center)| query * center)
            .sum::<f64>()
            / (DIRECTIONAL_RANK as f64).sqrt()
}

fn nonlinear_page_state(
    example: &NonlinearExample,
    parameters: &[f64],
    summaries: &[Vec<LocalModeLeaf>],
) -> (Vec<[f64; TREE_COUNT]>, Vec<[f64; PAGE_FEATURES]>) {
    let page_indexes = example
        .candidates
        .iter()
        .enumerate()
        .map(|(index, page)| (*page, index))
        .collect::<BTreeMap<_, _>>();
    let mut evidence = vec![[0.0; TREE_COUNT]; example.candidates.len()];
    for observation in &example.observations {
        let logit = directional_mode_logit(parameters, observation, example, summaries);
        let weight = observation.population * sigmoid(logit);
        for page in observation.pages {
            if let Some(index) = page_indexes.get(&page) {
                evidence[*index][observation.tree] += weight;
            }
        }
    }
    let features = evidence
        .iter()
        .map(|trees| {
            let mean = trees.iter().sum::<f64>() / TREE_COUNT as f64;
            let maximum = trees.iter().copied().fold(0.0_f64, f64::max);
            let variance = trees
                .iter()
                .map(|value| (value - mean).powi(2))
                .sum::<f64>()
                / TREE_COUNT as f64;
            [
                mean.ln_1p(),
                maximum.ln_1p(),
                variance.sqrt().ln_1p(),
                trees.iter().filter(|value| **value > 0.0).count() as f64 / TREE_COUNT as f64,
            ]
        })
        .collect();
    (evidence, features)
}

fn nonlinear_selected_pages(
    example: &NonlinearExample,
    parameters: &[f64],
    summaries: &[Vec<LocalModeLeaf>],
) -> BTreeSet<u32> {
    let (_, features) = nonlinear_page_state(example, parameters, summaries);
    let scores = example
        .candidates
        .iter()
        .zip(features)
        .map(|(page, values)| (*page, page_forward(parameters, &values).0))
        .collect::<BTreeMap<_, _>>();
    top_pages(&scores, SELECTED_PAGES)
}

fn nonlinear_loss_and_gradient(
    example: &NonlinearExample,
    parameters: &[f64],
    gradient: &mut [f64],
    summaries: &[Vec<LocalModeLeaf>],
    owners: &BTreeMap<u64, OwnerPair>,
) -> f64 {
    let (evidence, page_features) = nonlinear_page_state(example, parameters, summaries);
    let page_indexes = example
        .candidates
        .iter()
        .enumerate()
        .map(|(index, page)| (*page, index))
        .collect::<BTreeMap<_, _>>();
    let forwards = page_features
        .iter()
        .map(|values| page_forward(parameters, values))
        .collect::<Vec<_>>();
    let scores = forwards.iter().map(|forward| forward.1).collect::<Vec<_>>();
    let mut lower = scores.iter().copied().fold(f64::INFINITY, f64::min) - 40.0;
    let mut upper = scores.iter().copied().fold(f64::NEG_INFINITY, f64::max) + 40.0;
    for _ in 0..64 {
        let threshold = 0.5 * (lower + upper);
        let selected_mass = scores
            .iter()
            .map(|score| sigmoid((score - threshold) / SOFT_SELECTION_TEMPERATURE))
            .sum::<f64>();
        if selected_mass > SELECTED_PAGES as f64 {
            lower = threshold;
        } else {
            upper = threshold;
        }
    }
    let threshold = 0.5 * (lower + upper);
    let probabilities = scores
        .iter()
        .map(|score| sigmoid((score - threshold) / SOFT_SELECTION_TEMPERATURE))
        .collect::<Vec<_>>();
    let mut probability_gradients = vec![0.0; probabilities.len()];
    let inverse_truth = 1.0 / example.truth.len() as f64;
    let mut loss = 0.0;
    for feature in &example.truth {
        let &(primary, alternate) = owners.get(feature).expect("validated owner");
        let primary_index = page_indexes.get(&primary).copied();
        let alternate_index = (alternate != u32::MAX && alternate != primary)
            .then(|| page_indexes.get(&alternate).copied())
            .flatten();
        let primary_probability = primary_index.map_or(0.0, |index| probabilities[index]);
        let alternate_probability = alternate_index.map_or(0.0, |index| probabilities[index]);
        let coverage = 1.0 - (1.0 - primary_probability) * (1.0 - alternate_probability);
        loss += (1.0 - coverage) * inverse_truth;
        if let Some(index) = primary_index {
            probability_gradients[index] -= (1.0 - alternate_probability) * inverse_truth;
        }
        if let Some(index) = alternate_index {
            probability_gradients[index] -= (1.0 - primary_probability) * inverse_truth;
        }
    }
    let sigmoid_weights = probabilities
        .iter()
        .map(|probability| probability * (1.0 - probability))
        .collect::<Vec<_>>();
    let total_weight = sigmoid_weights.iter().sum::<f64>().max(1.0e-12);
    let coupled_gradient = probability_gradients
        .iter()
        .zip(&sigmoid_weights)
        .map(|(gradient, weight)| gradient * weight)
        .sum::<f64>()
        / total_weight;
    let output_gradients = probability_gradients
        .iter()
        .zip(&sigmoid_weights)
        .map(|(gradient, weight)| {
            weight * (gradient - coupled_gradient) / SOFT_SELECTION_TEMPERATURE
        })
        .collect::<Vec<_>>();
    let mut evidence_gradient = vec![[0.0; TREE_COUNT]; example.candidates.len()];
    for page in 0..example.candidates.len() {
        let values = page_features[page];
        let (_, _, hidden_pre, hidden) = forwards[page];
        let output_gradient = output_gradients[page];
        gradient[PAGE_B2] += output_gradient;
        let mut value_gradient = [0.0; PAGE_FEATURES];
        for unit in 0..PAGE_HIDDEN {
            gradient[PAGE_W2 + unit] += output_gradient * hidden[unit];
            let hidden_gradient =
                output_gradient * parameters[PAGE_W2 + unit] * silu_derivative(hidden_pre[unit]);
            gradient[PAGE_B1 + unit] += hidden_gradient;
            for input in 0..PAGE_FEATURES {
                gradient[PAGE_W1 + unit * PAGE_FEATURES + input] += hidden_gradient * values[input];
                value_gradient[input] +=
                    hidden_gradient * parameters[PAGE_W1 + unit * PAGE_FEATURES + input];
            }
        }
        let trees = evidence[page];
        let mean = trees.iter().sum::<f64>() / TREE_COUNT as f64;
        let (maximum_tree, maximum) = trees
            .iter()
            .copied()
            .enumerate()
            .max_by(|left, right| {
                left.1
                    .total_cmp(&right.1)
                    .then_with(|| right.0.cmp(&left.0))
            })
            .unwrap();
        let standard_deviation = (trees
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / TREE_COUNT as f64)
            .sqrt();
        for tree in 0..TREE_COUNT {
            evidence_gradient[page][tree] += value_gradient[0] / (1.0 + mean) / TREE_COUNT as f64;
            if tree == maximum_tree {
                evidence_gradient[page][tree] += value_gradient[1] / (1.0 + maximum);
            }
            if standard_deviation > 1.0e-12 {
                evidence_gradient[page][tree] += value_gradient[2] / (1.0 + standard_deviation)
                    * (trees[tree] - mean)
                    / (TREE_COUNT as f64 * standard_deviation);
            }
        }
    }
    for observation in &example.observations {
        let mut pre_one = [0.0; MODE_HIDDEN_ONE];
        let mut hidden_one = [0.0; MODE_HIDDEN_ONE];
        for hidden in 0..MODE_HIDDEN_ONE {
            let mut value = parameters[MODE_B1 + hidden];
            for input in 0..MODE_FEATURES {
                value += parameters[MODE_W1 + hidden * MODE_FEATURES + input]
                    * observation.values[input];
            }
            pre_one[hidden] = value;
            hidden_one[hidden] = silu(value);
        }
        let mut pre_two = [0.0; MODE_HIDDEN_TWO];
        let mut hidden_two = [0.0; MODE_HIDDEN_TWO];
        for hidden in 0..MODE_HIDDEN_TWO {
            let mut value = parameters[MODE_B2 + hidden];
            for input in 0..MODE_HIDDEN_ONE {
                value += parameters[MODE_W2 + hidden * MODE_HIDDEN_ONE + input] * hidden_one[input];
            }
            pre_two[hidden] = value;
            hidden_two[hidden] = silu(value);
        }
        let mut logit = parameters[MODE_B3];
        for hidden in 0..MODE_HIDDEN_TWO {
            logit += parameters[MODE_W3 + hidden] * hidden_two[hidden];
        }
        let mut query_projection = [0.0; DIRECTIONAL_RANK];
        let mut center_projection = [0.0; DIRECTIONAL_RANK];
        let mut normalized_center = [0.0; PROJECTED_DIMENSIONS];
        if parameters.len() == DIRECTIONAL_PARAMETERS {
            let group = &summaries[observation.tree][observation.leaf][observation.group];
            let center_start = observation.mode * PROJECTED_DIMENSIONS;
            let inverse_norm = 1.0 / f64::from(group.center_norms[observation.mode].max(1.0e-12));
            for dimension in 0..PROJECTED_DIMENSIONS {
                normalized_center[dimension] =
                    f64::from(group.centers[center_start + dimension]) * inverse_norm;
                for rank in 0..DIRECTIONAL_RANK {
                    query_projection[rank] += parameters
                        [DIRECTIONAL_U + dimension * DIRECTIONAL_RANK + rank]
                        * f64::from(example.query[dimension]);
                    center_projection[rank] += parameters
                        [DIRECTIONAL_V + dimension * DIRECTIONAL_RANK + rank]
                        * normalized_center[dimension];
                }
            }
            logit += query_projection
                .iter()
                .zip(center_projection)
                .map(|(query, center)| query * center)
                .sum::<f64>()
                / (DIRECTIONAL_RANK as f64).sqrt();
        }
        let probability = sigmoid(logit);
        let mut weight_gradient = 0.0;
        for page in observation.pages {
            if let Some(index) = page_indexes.get(&page) {
                weight_gradient += evidence_gradient[*index][observation.tree];
            }
        }
        let logit_gradient =
            weight_gradient * observation.population * probability * (1.0 - probability);
        if parameters.len() == DIRECTIONAL_PARAMETERS {
            let inverse_rank = 1.0 / (DIRECTIONAL_RANK as f64).sqrt();
            for dimension in 0..PROJECTED_DIMENSIONS {
                for rank in 0..DIRECTIONAL_RANK {
                    gradient[DIRECTIONAL_U + dimension * DIRECTIONAL_RANK + rank] += logit_gradient
                        * f64::from(example.query[dimension])
                        * center_projection[rank]
                        * inverse_rank;
                    gradient[DIRECTIONAL_V + dimension * DIRECTIONAL_RANK + rank] += logit_gradient
                        * normalized_center[dimension]
                        * query_projection[rank]
                        * inverse_rank;
                }
            }
        }
        gradient[MODE_B3] += logit_gradient;
        let mut hidden_two_gradient = [0.0; MODE_HIDDEN_TWO];
        for hidden in 0..MODE_HIDDEN_TWO {
            gradient[MODE_W3 + hidden] += logit_gradient * hidden_two[hidden];
            hidden_two_gradient[hidden] =
                logit_gradient * parameters[MODE_W3 + hidden] * silu_derivative(pre_two[hidden]);
            gradient[MODE_B2 + hidden] += hidden_two_gradient[hidden];
        }
        let mut hidden_one_gradient = [0.0; MODE_HIDDEN_ONE];
        for hidden in 0..MODE_HIDDEN_TWO {
            for input in 0..MODE_HIDDEN_ONE {
                gradient[MODE_W2 + hidden * MODE_HIDDEN_ONE + input] +=
                    hidden_two_gradient[hidden] * hidden_one[input];
                hidden_one_gradient[input] += hidden_two_gradient[hidden]
                    * parameters[MODE_W2 + hidden * MODE_HIDDEN_ONE + input];
            }
        }
        for hidden in 0..MODE_HIDDEN_ONE {
            let hidden_gradient = hidden_one_gradient[hidden] * silu_derivative(pre_one[hidden]);
            gradient[MODE_B1 + hidden] += hidden_gradient;
            for input in 0..MODE_FEATURES {
                gradient[MODE_W1 + hidden * MODE_FEATURES + input] +=
                    hidden_gradient * observation.values[input];
            }
        }
    }
    loss
}

fn initial_nonlinear_parameters() -> Vec<f64> {
    let mut state = 0x41_946_5eed_u64;
    let mut random = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state as f64 / u64::MAX as f64 - 0.5) * 0.1
    };
    let mut parameters = vec![0.0; NONLINEAR_PARAMETERS];
    for index in MODE_W1..MODE_B1 {
        parameters[index] = random();
    }
    for index in MODE_W2..MODE_B2 {
        parameters[index] = random();
    }
    for index in MODE_W3..MODE_B3 {
        parameters[index] = random();
    }
    for index in PAGE_W1..PAGE_B1 {
        parameters[index] = random();
    }
    for index in PAGE_W2..PAGE_B2 {
        parameters[index] = random();
    }
    parameters
}

fn load_initial_nonlinear_parameters(path: &PathBuf) -> Result<Vec<f64>> {
    let bytes = fs::read(path).map_err(|source| BorsukError::Io {
        path: path.clone(),
        source,
    })?;
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| invalid("V41 initial model differs"))?;
    let values = value
        .get("weights")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| invalid("V41 initial model differs"))?;
    if values.len() != NONLINEAR_PARAMETERS {
        return Err(invalid("V41 initial model differs"));
    }
    values
        .iter()
        .map(|value| {
            value
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or_else(|| invalid("V41 initial model differs"))
        })
        .collect()
}

fn initial_directional_parameters(scalar: &[f64]) -> Vec<f64> {
    let mut parameters = vec![0.0; DIRECTIONAL_PARAMETERS];
    parameters[..NONLINEAR_PARAMETERS].copy_from_slice(scalar);
    let mut state = 0x41_d1_8ec7_u64;
    for value in &mut parameters[DIRECTIONAL_U..DIRECTIONAL_V] {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *value = (state as f64 / u64::MAX as f64 - 0.5) * 0.02;
    }
    parameters
}

fn fit_nonlinear_mass_model(
    examples: &[&NonlinearExample],
    summaries: &[Vec<LocalModeLeaf>],
    owners: &BTreeMap<u64, OwnerPair>,
    mut parameters: Vec<f64>,
) -> (Vec<f64>, f64) {
    let mut first_moment = vec![0.0; parameters.len()];
    let mut second_moment = vec![0.0; parameters.len()];
    let mut best_parameters = parameters.clone();
    let mut best_loss = f64::INFINITY;
    let mut step = 0_i32;
    let mut order = (0..examples.len()).collect::<Vec<_>>();
    let mut shuffle_state = 0x41_300_ada0_u64;
    for epoch in 0..NONLINEAR_EPOCHS {
        for index in (1..order.len()).rev() {
            shuffle_state ^= shuffle_state << 13;
            shuffle_state ^= shuffle_state >> 7;
            shuffle_state ^= shuffle_state << 17;
            order.swap(index, shuffle_state as usize % (index + 1));
        }
        let mut epoch_loss = 0.0;
        for batch in order.chunks(NONLINEAR_BATCH) {
            let partials = batch
                .par_iter()
                .map(|example| {
                    let mut gradient = vec![0.0; parameters.len()];
                    let loss = nonlinear_loss_and_gradient(
                        examples[*example],
                        &parameters,
                        &mut gradient,
                        summaries,
                        owners,
                    );
                    (loss, gradient)
                })
                .collect::<Vec<_>>();
            let mut gradient = vec![0.0; parameters.len()];
            for (loss, partial) in partials {
                epoch_loss += loss;
                for (total, value) in gradient.iter_mut().zip(partial) {
                    *total += value;
                }
            }
            let inverse_batch = 1.0 / batch.len() as f64;
            step += 1;
            for index in 0..parameters.len() {
                let value = (gradient[index] * inverse_batch).clamp(-100.0, 100.0);
                first_moment[index] = 0.9 * first_moment[index] + 0.1 * value;
                second_moment[index] = 0.999 * second_moment[index] + 0.001 * value * value;
                let corrected_first = first_moment[index] / (1.0 - 0.9_f64.powi(step));
                let corrected_second = second_moment[index] / (1.0 - 0.999_f64.powi(step));
                parameters[index] -= 1.0e-3 * corrected_first / (corrected_second.sqrt() + 1.0e-8);
            }
        }
        epoch_loss /= examples.len() as f64;
        if epoch_loss.is_finite() && epoch_loss < best_loss {
            best_loss = epoch_loss;
            best_parameters.clone_from(&parameters);
        }
        if epoch + 1 == NONLINEAR_EPOCHS {
            let hits = examples
                .iter()
                .map(|example| {
                    let selected = nonlinear_selected_pages(example, &parameters, summaries);
                    gt_hits(&example.truth, owners, &selected)
                })
                .collect::<Result<Vec<_>>>();
            if hits.is_ok_and(|hits| mode_screen(&hits).passed) {
                return (parameters, epoch_loss);
            }
        }
    }
    (best_parameters, best_loss)
}

fn weighted_pages(
    trees: &[V37BalancedTree],
    summaries: &[Vec<LeafSummary>],
    query: &[f32],
    leaf_limit: usize,
) -> Result<(BTreeSet<u32>, BTreeMap<OwnerPair, f64>, BTreeMap<u32, f64>)> {
    let mut union = BTreeSet::new();
    let mut pair_weights = BTreeMap::<OwnerPair, f64>::new();
    let mut page_weights = BTreeMap::<u32, f64>::new();
    for (tree, leaves) in trees.iter().zip(summaries) {
        let selection = crate::v37_relation_router::select_v37_tree_postings_with_limit(
            tree,
            &tree.fma_backend,
            tree.nodes.len().min(1_024),
            leaf_limit,
            query,
        )?;
        for (rank, leaf) in selection.selected_postings.iter().enumerate() {
            let population = tree.leaf_populations[*leaf as usize] as f64;
            for &(pair, count) in &leaves[*leaf as usize] {
                let weight = f64::from(count) / population / (rank + 1) as f64;
                *pair_weights.entry(pair).or_default() += weight;
                for page in [pair.0, pair.1] {
                    if page != u32::MAX {
                        union.insert(page);
                        *page_weights.entry(page).or_default() += weight;
                    }
                }
            }
        }
    }
    Ok((union, pair_weights, page_weights))
}

fn calibrated_leaf_summaries(
    trees: &[V37BalancedTree],
    queries: &[crate::v36_prefix_dataset::V36PrefixQueryRow],
    truth: &[crate::v37_relation_router::V37FeatureGroundTruth],
    owners: &BTreeMap<u64, OwnerPair>,
) -> Result<Vec<Vec<CalibratedLeafSummary>>> {
    if queries.len() != truth.len() {
        return Err(invalid("V41 forest calibration rows differ"));
    }
    let projection = crate::v36_funnel_geometry::build_v36_srht192_control()?;
    let mut scores = trees
        .iter()
        .map(|tree| {
            tree.leaf_populations
                .iter()
                .map(|_| HashMap::<OwnerPair, f64>::new())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut visits = trees
        .iter()
        .map(|tree| vec![0.0_f64; tree.leaf_populations.len()])
        .collect::<Vec<_>>();
    for (query, truth) in queries.iter().zip(truth) {
        if query.query_ordinal != truth.query_ordinal {
            return Err(invalid("V41 forest calibration ordinal differs"));
        }
        let projected =
            crate::v35_projection::project_v35_query_simd(&projection, &query.embedding)?;
        let vector = projected
            .coordinates()
            .iter()
            .map(|value| if *value == 0.0 { 0.0 } else { *value as f32 })
            .collect::<Vec<_>>();
        let mut relevant = BTreeMap::<OwnerPair, u32>::new();
        for feature in &truth.feature_row_ids {
            let pair = *owners
                .get(feature)
                .ok_or_else(|| invalid("V41 forest calibration owner differs"))?;
            *relevant.entry(pair).or_default() += 1;
        }
        for (tree_index, tree) in trees.iter().enumerate() {
            let selection = crate::v37_relation_router::select_v37_tree_postings_with_limit(
                tree,
                &tree.fma_backend,
                tree.nodes.len().min(1_024),
                VISITED_LEAVES,
                &vector,
            )?;
            for (rank, leaf) in selection.selected_postings.iter().enumerate() {
                let weight = 1.0 / (rank + 1) as f64;
                visits[tree_index][*leaf as usize] += weight;
                for (&pair, &count) in &relevant {
                    *scores[tree_index][*leaf as usize].entry(pair).or_default() +=
                        f64::from(count) * weight;
                }
            }
        }
    }
    Ok(scores
        .into_iter()
        .zip(visits)
        .map(|(tree_scores, tree_visits)| {
            tree_scores
                .into_iter()
                .zip(tree_visits)
                .map(|(leaf, visit_weight)| {
                    let mut entries = leaf
                        .into_iter()
                        .map(|(pair, mass)| {
                            (
                                pair,
                                if visit_weight == 0.0 {
                                    0.0
                                } else {
                                    mass / visit_weight
                                },
                            )
                        })
                        .collect::<Vec<_>>();
                    entries.sort_unstable_by(|left, right| {
                        right
                            .1
                            .total_cmp(&left.1)
                            .then_with(|| left.0.cmp(&right.0))
                    });
                    entries.truncate(PAIRS_PER_LEAF);
                    entries
                })
                .collect()
        })
        .collect())
}

fn calibrated_pair_weights(
    trees: &[V37BalancedTree],
    summaries: &[Vec<CalibratedLeafSummary>],
    query: &[f32],
) -> Result<BTreeMap<OwnerPair, f64>> {
    let mut weights = BTreeMap::<OwnerPair, f64>::new();
    for (tree, leaves) in trees.iter().zip(summaries) {
        let selection = crate::v37_relation_router::select_v37_tree_postings_with_limit(
            tree,
            &tree.fma_backend,
            tree.nodes.len().min(1_024),
            VISITED_LEAVES,
            query,
        )?;
        for (rank, leaf) in selection.selected_postings.iter().enumerate() {
            for &(pair, mass) in &leaves[*leaf as usize] {
                *weights.entry(pair).or_default() += mass / (rank + 1) as f64;
            }
        }
    }
    Ok(weights)
}

fn blend_pair_weights(
    calibrated: &BTreeMap<OwnerPair, f64>,
    corpus: &BTreeMap<OwnerPair, f64>,
    calibrated_ppm: u32,
) -> BTreeMap<OwnerPair, f64> {
    let calibrated_sum = calibrated.values().sum::<f64>();
    let corpus_sum = corpus.values().sum::<f64>();
    let alpha = f64::from(calibrated_ppm) / 1_000_000.0;
    let mut result = BTreeMap::new();
    for pair in calibrated.keys().chain(corpus.keys()) {
        let learned = calibrated.get(pair).copied().unwrap_or_default()
            / calibrated_sum.max(f64::MIN_POSITIVE);
        let prior =
            corpus.get(pair).copied().unwrap_or_default() / corpus_sum.max(f64::MIN_POSITIVE);
        result.insert(*pair, alpha * learned + (1.0 - alpha) * prior);
    }
    result
}

fn top_pages(page_weights: &BTreeMap<u32, f64>, limit: usize) -> BTreeSet<u32> {
    let mut pages = page_weights
        .iter()
        .map(|(page, weight)| (*page, *weight))
        .collect::<Vec<_>>();
    pages.sort_unstable_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    pages.truncate(limit);
    pages.into_iter().map(|entry| entry.0).collect()
}

fn weighted_greedy(
    pair_weights: &BTreeMap<OwnerPair, f64>,
    candidates: &BTreeSet<u32>,
) -> BTreeSet<u32> {
    let mut selected = BTreeSet::new();
    while selected.len() < SELECTED_PAGES {
        let mut best = None;
        for page in candidates
            .iter()
            .copied()
            .filter(|page| !selected.contains(page))
        {
            let gain = pair_weights
                .iter()
                .filter(|(pair, _)| {
                    !selected.contains(&pair.0)
                        && (pair.1 == u32::MAX || !selected.contains(&pair.1))
                })
                .filter(|(pair, _)| page == pair.0 || page == pair.1)
                .map(|(_, weight)| *weight)
                .sum::<f64>();
            if best.is_none_or(|(best_gain, best_page)| {
                gain.total_cmp(&best_gain).is_gt() || (gain == best_gain && page < best_page)
            }) {
                best = Some((gain, page));
            }
        }
        let Some((_, page)) = best else { break };
        selected.insert(page);
    }
    selected
}

/// Run the throwaway forest candidate-coverage spike.
#[doc(hidden)]
pub fn run_v41_forest_probe(request: V41ForestProbeRequest) -> Result<Vec<u8>> {
    let relation = fs::read(&request.spill_relation).map_err(|source| BorsukError::Io {
        path: request.spill_relation.clone(),
        source,
    })?;
    let postings = fs::read(&request.spill_postings).map_err(|source| BorsukError::Io {
        path: request.spill_postings.clone(),
        source,
    })?;
    let owner_rows = v38_v40_owner_rows_from_artifacts(
        &relation,
        &postings,
        SOURCE_ROWS,
        PAGE_COUNT,
        MAXIMUM_ROWS_PER_PAGE,
    )?;
    let owners = owner_rows
        .into_iter()
        .map(|(feature, primary, alternate)| (feature, (primary, alternate.unwrap_or(u32::MAX))))
        .collect::<BTreeMap<_, _>>();

    let source_ids = crate::v36_prefix_dataset::load_v36_prefix_source_feature_ids_file(
        File::open(&request.source).map_err(|source| BorsukError::Io {
            path: request.source.clone(),
            source,
        })?,
        &request.source,
        SOURCE_ROWS as u64,
    )?;
    let projected = crate::v36_prefix_dataset::project_v36_prefix_source_resident_file(
        File::open(&request.source).map_err(|source| BorsukError::Io {
            path: request.source.clone(),
            source,
        })?,
        &request.source,
        &source_ids,
        65_536,
    )?;

    let mut trees = Vec::with_capacity(TREE_COUNT);
    let mut summaries = Vec::with_capacity(TREE_COUNT);
    let mut mode_summaries = Vec::with_capacity(TREE_COUNT);
    for tree in 0..TREE_COUNT {
        let trained = crate::v37_relation_router::train_v37_ownership_tree_resident_coordinates(
            projected.projected_coordinates(),
            V37TrainingShape {
                dimensions: 192,
                leaf_count: LEAF_COUNT,
                reservoir_rows: 256,
                two_means_iterations: 8,
            },
            41_000 + tree as u64,
            request.workers,
            65_536,
        )?;
        let summary = retained_leaf_summaries(&trained, projected.feature_ids(), &owners)?;
        mode_summaries.push(local_mode_summaries(
            &trained,
            &summary,
            projected.feature_ids(),
            &owners,
            projected.projected_coordinates(),
            request.workers,
        )?);
        summaries.push(summary);
        trees.push(trained);
    }

    let mut training_queries = Vec::with_capacity(QUERY_ROWS);
    crate::v36_prefix_dataset::scan_v36_prefix_query_parquet(
        &request.training_query,
        QUERY_ROWS as u64,
        |batch| {
            training_queries.extend(crate::v36_prefix_dataset::v36_prefix_query_rows_from_batch(
                &batch,
                0,
                batch.num_rows(),
            )?);
            Ok(())
        },
    )?;
    let training_truth = crate::v37_relation_router::load_v37_feature_ground_truth_file(
        File::open(&request.training_ground_truth).map_err(|source| BorsukError::Io {
            path: request.training_ground_truth.clone(),
            source,
        })?,
        &request.training_ground_truth,
        QUERY_ROWS as u32,
    )?;
    let projection = crate::v36_funnel_geometry::build_v36_srht192_control()?;
    let mut development_examples = Vec::with_capacity(QUERY_ROWS);
    let mut development_oracle_hits = Vec::with_capacity(QUERY_ROWS);
    for (query, truth) in training_queries.iter().zip(&training_truth) {
        let projected_query =
            crate::v35_projection::project_v35_query_simd(&projection, &query.embedding)?;
        let vector = projected_query
            .coordinates()
            .iter()
            .map(|value| if *value == 0.0 { 0.0 } else { *value as f32 })
            .collect::<Vec<_>>();
        let (_, _, page_weights) = weighted_pages(&trees, &summaries, &vector, VISITED_LEAVES)?;
        let candidates = top_pages(&page_weights, 96);
        let observations =
            nonlinear_mode_observations(&trees, &mode_summaries, &vector, &candidates)?;
        let restricted_pages = observations
            .iter()
            .flat_map(|observation| observation.pages)
            .filter(|page| *page != u32::MAX && candidates.contains(page))
            .collect::<BTreeSet<_>>();
        let singleton = gt_singleton_mass(&truth.feature_row_ids, &owners, &restricted_pages)?;
        development_oracle_hits.push(gt_hits(&truth.feature_row_ids, &owners, &singleton)?);
        let candidate_vector = candidates.into_iter().collect::<Vec<_>>();
        development_examples.push(NonlinearExample {
            observations,
            candidates: candidate_vector,
            truth: truth.feature_row_ids.clone(),
            query: normalized_projected_query(&vector),
        });
    }
    let (means, scales) = standardize_observations(&mut development_examples);
    let scalar_initial = load_initial_nonlinear_parameters(&request.initial_model)?;
    let optimization_examples = development_examples
        .iter()
        .enumerate()
        .filter_map(|(index, example)| (index % OPTIMIZATION_QUERY_STRIDE == 0).then_some(example))
        .collect::<Vec<_>>();
    let (control_weights, control_best_union_loss) = fit_nonlinear_mass_model(
        &optimization_examples,
        &mode_summaries,
        &owners,
        scalar_initial.clone(),
    );
    let control_hits = development_examples
        .iter()
        .map(|example| {
            let selected = nonlinear_selected_pages(example, &control_weights, &mode_summaries);
            gt_hits(&example.truth, &owners, &selected)
        })
        .collect::<Result<Vec<_>>>()?;
    let control_development = mode_screen(&control_hits);
    let mut winning_arm = control_development
        .passed
        .then_some("union-coverage-control");
    let mut winning_weights = control_weights;
    let mut directional_best_union_loss = None;
    let mut directional_development = None;
    if winning_arm.is_none() {
        let directional_initial = initial_directional_parameters(&scalar_initial);
        let (directional_weights, loss) = fit_nonlinear_mass_model(
            &optimization_examples,
            &mode_summaries,
            &owners,
            directional_initial,
        );
        let hits = development_examples
            .iter()
            .map(|example| {
                let selected =
                    nonlinear_selected_pages(example, &directional_weights, &mode_summaries);
                gt_hits(&example.truth, &owners, &selected)
            })
            .collect::<Result<Vec<_>>>()?;
        let result = mode_screen(&hits);
        directional_best_union_loss = Some(loss);
        if result.passed {
            winning_arm = Some("rank8-directional-residual");
            winning_weights = directional_weights;
        }
        directional_development = Some(result);
    }
    let development_passed = winning_arm.is_some();

    let local_mode_groups = mode_summaries
        .iter()
        .flat_map(|tree| tree.iter())
        .map(Vec::len)
        .sum::<usize>();
    let local_mode_centers = mode_summaries
        .iter()
        .flat_map(|tree| tree.iter())
        .flat_map(|leaf| leaf.iter())
        .map(|group| group.centers.len() / PROJECTED_DIMENSIONS)
        .sum::<usize>();

    let mut validation = None;
    let mut validation_query_rows = 0;
    if development_passed {
        let mut queries = Vec::with_capacity(QUERY_ROWS);
        crate::v36_prefix_dataset::scan_v36_prefix_query_parquet(
            &request.query,
            QUERY_ROWS as u64,
            |batch| {
                queries.extend(crate::v36_prefix_dataset::v36_prefix_query_rows_from_batch(
                    &batch,
                    0,
                    batch.num_rows(),
                )?);
                Ok(())
            },
        )?;
        let truth = crate::v37_relation_router::load_v37_feature_ground_truth_file(
            File::open(&request.ground_truth).map_err(|source| BorsukError::Io {
                path: request.ground_truth.clone(),
                source,
            })?,
            &request.ground_truth,
            QUERY_ROWS as u32,
        )?;
        let mut validation_examples = Vec::with_capacity(QUERY_ROWS);
        for (query, truth) in queries.iter().zip(&truth) {
            let projected_query =
                crate::v35_projection::project_v35_query_simd(&projection, &query.embedding)?;
            let vector = projected_query
                .coordinates()
                .iter()
                .map(|value| if *value == 0.0 { 0.0 } else { *value as f32 })
                .collect::<Vec<_>>();
            let (_, _, page_weights) = weighted_pages(&trees, &summaries, &vector, VISITED_LEAVES)?;
            let candidates = top_pages(&page_weights, 96);
            let observations =
                nonlinear_mode_observations(&trees, &mode_summaries, &vector, &candidates)?;
            let candidate_vector = candidates.into_iter().collect::<Vec<_>>();
            validation_examples.push(NonlinearExample {
                observations,
                candidates: candidate_vector,
                truth: truth.feature_row_ids.clone(),
                query: normalized_projected_query(&vector),
            });
        }
        apply_observation_standardization(&mut validation_examples, &means, &scales);
        let hits = validation_examples
            .iter()
            .map(|example| {
                let selected = nonlinear_selected_pages(example, &winning_weights, &mode_summaries);
                gt_hits(&example.truth, &owners, &selected)
            })
            .collect::<Result<Vec<_>>>()?;
        validation = Some(mode_screen(&hits));
        validation_query_rows = QUERY_ROWS;
    }

    let result = NonlinearProbeResult {
        schema: "borsuk-v41-nonlinear-mode-mass-probe-v1",
        claim_eligible: false,
        source_rows: SOURCE_ROWS,
        development_query_rows: QUERY_ROWS,
        optimization_query_rows: optimization_examples.len(),
        validation_query_rows,
        trees: TREE_COUNT,
        leaves_per_tree: LEAF_COUNT,
        visited_leaves_per_tree: VISITED_LEAVES,
        retained_pairs_per_leaf: PAIRS_PER_LEAF,
        modes_per_group: LOCAL_MODES,
        ranked_pairs: MODE_RANKED_PAIRS,
        candidate_pages: 96,
        selected_pages: SELECTED_PAGES,
        control_parameter_count: NONLINEAR_PARAMETERS,
        directional_parameter_count: DIRECTIONAL_PARAMETERS,
        epochs: NONLINEAR_EPOCHS,
        objective: "soft-top21-two-owner-union-coverage",
        soft_selection_temperature: SOFT_SELECTION_TEMPERATURE,
        control_best_union_loss,
        control_development,
        directional_best_union_loss,
        directional_development,
        development_singleton_oracle: mode_screen(&development_oracle_hits),
        validation,
        stopped_before_validation: !development_passed,
        local_mode_groups,
        local_mode_centers,
        local_mode_bytes: local_mode_centers * PROJECTED_DIMENSIONS * size_of::<f32>(),
        winning_arm,
    };
    let mut bytes =
        serde_json::to_vec(&result).map_err(|_| invalid("V41 forest result differs"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

const SHARED_LEAVES: u64 = 16_384;
const SHARED_VISITED_LEAVES: usize = 512;
const SHARED_MAXIMUM_NODE_VISITS: usize = 8_192;
const SHARED_SCREEN_STRIDE: usize = 4;

/// Inputs for the disposable shared-dictionary algorithm probe.
#[doc(hidden)]
pub struct V43SharedDictionaryProbeRequest {
    pub source: PathBuf,
    pub development_query: PathBuf,
    pub development_ground_truth: PathBuf,
    pub spill_relation: PathBuf,
    pub spill_postings: PathBuf,
    pub workers: usize,
}

struct SharedLeaf {
    center: Vec<f32>,
    population: u32,
    radius_mean: f32,
    radius_variance: f32,
    pairs: Vec<(OwnerPair, u32)>,
}

#[derive(Serialize)]
struct SharedScreen {
    queries: usize,
    aggregate_recall_ppm: u32,
    minimum_recall_ppm: u32,
    p50_query_us: u64,
    p99_query_us: u64,
    passed: bool,
}

#[derive(Serialize)]
struct SharedDictionaryResult {
    schema: &'static str,
    claim_eligible: bool,
    source_rows: usize,
    dimensions: usize,
    leaves: u64,
    visited_leaves: usize,
    maximum_node_visits: usize,
    complete_owner_pair_records: usize,
    selected_pages: usize,
    construction_elapsed_ms: u64,
    serving_screen: SharedScreen,
    exhaustive_screen: SharedScreen,
    serving_development: Option<SharedScreen>,
    validation_opened: bool,
}

fn shared_leaf_summaries(
    tree: &V37BalancedTree,
    feature_ids: &[u64],
    owners: &BTreeMap<u64, OwnerPair>,
    coordinates: &[f32],
) -> Result<Vec<SharedLeaf>> {
    let leaf_count = tree.leaf_populations.len();
    let mut populations = vec![0_u32; leaf_count];
    let mut sums = vec![0.0_f64; leaf_count * PROJECTED_DIMENSIONS];
    let mut pair_counts = (0..leaf_count)
        .map(|_| HashMap::<OwnerPair, u32>::new())
        .collect::<Vec<_>>();
    for assignment in &tree.assignments {
        let row = usize::try_from(assignment.source_ordinal)
            .map_err(|_| invalid("V43 source ordinal differs"))?;
        let leaf = assignment.posting_ordinal as usize;
        populations[leaf] += 1;
        for (sum, value) in sums[leaf * PROJECTED_DIMENSIONS..(leaf + 1) * PROJECTED_DIMENSIONS]
            .iter_mut()
            .zip(&coordinates[row * PROJECTED_DIMENSIONS..(row + 1) * PROJECTED_DIMENSIONS])
        {
            *sum += f64::from(*value);
        }
        let pair = *owners
            .get(&feature_ids[row])
            .ok_or_else(|| invalid("V43 source owner differs"))?;
        *pair_counts[leaf].entry(pair).or_default() += 1;
    }
    let mut centers = vec![0.0_f32; leaf_count * PROJECTED_DIMENSIONS];
    for leaf in 0..leaf_count {
        let inverse = 1.0 / f64::from(populations[leaf]);
        for (center, sum) in centers[leaf * PROJECTED_DIMENSIONS..(leaf + 1) * PROJECTED_DIMENSIONS]
            .iter_mut()
            .zip(&sums[leaf * PROJECTED_DIMENSIONS..(leaf + 1) * PROJECTED_DIMENSIONS])
        {
            *center = (*sum * inverse) as f32;
        }
    }
    let mut radius_sums = vec![0.0_f64; leaf_count];
    let mut radius_square_sums = vec![0.0_f64; leaf_count];
    for assignment in &tree.assignments {
        let row = assignment.source_ordinal as usize;
        let leaf = assignment.posting_ordinal as usize;
        let distance = f64::from(squared_distance(
            &coordinates[row * PROJECTED_DIMENSIONS..(row + 1) * PROJECTED_DIMENSIONS],
            &centers[leaf * PROJECTED_DIMENSIONS..(leaf + 1) * PROJECTED_DIMENSIONS],
        ));
        radius_sums[leaf] += distance;
        radius_square_sums[leaf] += distance * distance;
    }
    Ok((0..leaf_count)
        .map(|leaf| {
            let population = populations[leaf];
            let mean = radius_sums[leaf] / f64::from(population);
            let variance =
                (radius_square_sums[leaf] / f64::from(population) - mean * mean).max(0.0);
            let mut pairs = pair_counts[leaf].drain().collect::<Vec<_>>();
            pairs.sort_unstable_by_key(|entry| entry.0);
            SharedLeaf {
                center: centers[leaf * PROJECTED_DIMENSIONS..(leaf + 1) * PROJECTED_DIMENSIONS]
                    .to_vec(),
                population,
                radius_mean: mean as f32,
                radius_variance: variance as f32,
                pairs,
            }
        })
        .collect())
}

fn shared_selected_pages(
    leaves: &[SharedLeaf],
    selected_leaves: &[u32],
    query: &[f32],
) -> BTreeSet<u32> {
    let mut moments = Vec::with_capacity(selected_leaves.len());
    let mut low = f64::INFINITY;
    let mut high = f64::NEG_INFINITY;
    for &leaf in selected_leaves {
        let leaf = &leaves[leaf as usize];
        let distance = f64::from(squared_distance(query, &leaf.center));
        let mean = distance + f64::from(leaf.radius_mean);
        let variance = f64::from(leaf.radius_variance)
            + 4.0 * distance * f64::from(leaf.radius_mean) / PROJECTED_DIMENSIONS as f64;
        let scale = (3.0 * variance).sqrt() / std::f64::consts::PI;
        let scale = scale.max(1.0e-6);
        low = low.min(mean - 40.0 * scale);
        high = high.max(mean + 40.0 * scale);
        moments.push((leaf, mean, scale));
    }
    for _ in 0..32 {
        let threshold = (low + high) * 0.5;
        let expected = moments
            .iter()
            .map(|(leaf, mean, scale)| {
                f64::from(leaf.population) / (1.0 + ((mean - threshold) / scale).exp())
            })
            .sum::<f64>();
        if expected < 100.0 {
            low = threshold;
        } else {
            high = threshold;
        }
    }
    let threshold = (low + high) * 0.5;
    let mut weights = BTreeMap::<OwnerPair, f64>::new();
    let mut candidates = BTreeSet::new();
    for (leaf, mean, scale) in moments {
        let probability = 1.0 / (1.0 + ((mean - threshold) / scale).exp());
        for &(pair, count) in &leaf.pairs {
            *weights.entry(pair).or_default() += f64::from(count) * probability;
            candidates.insert(pair.0);
            if pair.1 != u32::MAX {
                candidates.insert(pair.1);
            }
        }
    }
    weighted_greedy(&weights, &candidates)
}

fn shared_screen(
    query_indices: impl Iterator<Item = usize>,
    queries: &[crate::v36_prefix_dataset::V36PrefixQueryRow],
    truth: &[crate::v37_relation_router::V37FeatureGroundTruth],
    owners: &BTreeMap<u64, OwnerPair>,
    projection: &crate::v35_projection::V35Projection,
    tree: &V37BalancedTree,
    leaves: &[SharedLeaf],
    exhaustive: bool,
    promotion: bool,
) -> Result<SharedScreen> {
    let mut hits = Vec::new();
    let mut timings = Vec::new();
    for query_index in query_indices {
        let projected = crate::v35_projection::project_v35_query_simd(
            projection,
            &queries[query_index].embedding,
        )?;
        let query = projected
            .coordinates()
            .iter()
            .map(|value| *value as f32)
            .collect::<Vec<_>>();
        let started = std::time::Instant::now();
        let selected_leaves = if exhaustive {
            let mut distances = leaves
                .iter()
                .enumerate()
                .map(|(leaf, summary)| (squared_distance(&query, &summary.center), leaf as u32))
                .collect::<Vec<_>>();
            distances.select_nth_unstable_by(SHARED_VISITED_LEAVES, |left, right| {
                left.0
                    .total_cmp(&right.0)
                    .then_with(|| left.1.cmp(&right.1))
            });
            distances.truncate(SHARED_VISITED_LEAVES);
            distances.sort_unstable_by(|left, right| {
                left.0
                    .total_cmp(&right.0)
                    .then_with(|| left.1.cmp(&right.1))
            });
            distances.into_iter().map(|entry| entry.1).collect()
        } else {
            crate::v37_relation_router::select_v37_tree_postings_with_limit(
                tree,
                &tree.fma_backend,
                SHARED_MAXIMUM_NODE_VISITS,
                SHARED_VISITED_LEAVES,
                &query,
            )?
            .selected_postings
        };
        let selected = shared_selected_pages(leaves, &selected_leaves, &query);
        timings.push(started.elapsed().as_micros() as u64);
        hits.push(gt_hits(
            &truth[query_index].feature_row_ids,
            owners,
            &selected,
        )?);
    }
    timings.sort_unstable();
    let total = hits.iter().map(|value| u64::from(*value)).sum::<u64>();
    let aggregate = u32::try_from(total * 1_000_000 / (hits.len() as u64 * 100)).unwrap();
    let minimum = hits.iter().copied().min().unwrap() * 10_000;
    let passed = if promotion {
        aggregate >= 995_000 && minimum >= 800_000
    } else {
        aggregate >= 990_000 && minimum >= 700_000
    };
    Ok(SharedScreen {
        queries: hits.len(),
        aggregate_recall_ppm: aggregate,
        minimum_recall_ppm: minimum,
        p50_query_us: timings[timings.len() / 2],
        p99_query_us: timings[(timings.len() * 99 / 100).min(timings.len() - 1)],
        passed,
    })
}

/// Run one shared balanced dictionary with complete two-owner incidence mass.
#[doc(hidden)]
pub fn run_v43_shared_dictionary_probe(
    request: V43SharedDictionaryProbeRequest,
) -> Result<Vec<u8>> {
    let relation = fs::read(&request.spill_relation).map_err(|source| BorsukError::Io {
        path: request.spill_relation.clone(),
        source,
    })?;
    let postings = fs::read(&request.spill_postings).map_err(|source| BorsukError::Io {
        path: request.spill_postings.clone(),
        source,
    })?;
    let owners = v38_v40_owner_rows_from_artifacts(
        &relation,
        &postings,
        SOURCE_ROWS,
        PAGE_COUNT,
        MAXIMUM_ROWS_PER_PAGE,
    )?
    .into_iter()
    .map(|(feature, primary, alternate)| (feature, (primary, alternate.unwrap_or(u32::MAX))))
    .collect::<BTreeMap<_, _>>();
    let source_ids = crate::v36_prefix_dataset::load_v36_prefix_source_feature_ids_file(
        File::open(&request.source).map_err(|source| BorsukError::Io {
            path: request.source.clone(),
            source,
        })?,
        &request.source,
        SOURCE_ROWS as u64,
    )?;
    let projected = crate::v36_prefix_dataset::project_v36_prefix_source_resident_file(
        File::open(&request.source).map_err(|source| BorsukError::Io {
            path: request.source.clone(),
            source,
        })?,
        &request.source,
        &source_ids,
        65_536,
    )?;
    let construction_started = std::time::Instant::now();
    let tree = crate::v37_relation_router::train_v37_ownership_tree_resident_coordinates(
        projected.projected_coordinates(),
        V37TrainingShape {
            dimensions: PROJECTED_DIMENSIONS,
            leaf_count: SHARED_LEAVES,
            reservoir_rows: 256,
            two_means_iterations: 8,
        },
        43_001,
        request.workers,
        65_536,
    )?;
    let leaves = shared_leaf_summaries(
        &tree,
        projected.feature_ids(),
        &owners,
        projected.projected_coordinates(),
    )?;
    let construction_elapsed_ms = construction_started.elapsed().as_millis() as u64;
    let complete_owner_pair_records = leaves.iter().map(|leaf| leaf.pairs.len()).sum();
    let mut queries = Vec::with_capacity(QUERY_ROWS);
    crate::v36_prefix_dataset::scan_v36_prefix_query_parquet(
        &request.development_query,
        QUERY_ROWS as u64,
        |batch| {
            queries.extend(crate::v36_prefix_dataset::v36_prefix_query_rows_from_batch(
                &batch,
                0,
                batch.num_rows(),
            )?);
            Ok(())
        },
    )?;
    let truth = crate::v37_relation_router::load_v37_feature_ground_truth_file(
        File::open(&request.development_ground_truth).map_err(|source| BorsukError::Io {
            path: request.development_ground_truth.clone(),
            source,
        })?,
        &request.development_ground_truth,
        QUERY_ROWS as u32,
    )?;
    let projection = crate::v36_funnel_geometry::build_v36_srht192_control()?;
    let serving_screen = shared_screen(
        (0..QUERY_ROWS).step_by(SHARED_SCREEN_STRIDE),
        &queries,
        &truth,
        &owners,
        &projection,
        &tree,
        &leaves,
        false,
        false,
    )?;
    let exhaustive_screen = shared_screen(
        (0..QUERY_ROWS).step_by(SHARED_SCREEN_STRIDE),
        &queries,
        &truth,
        &owners,
        &projection,
        &tree,
        &leaves,
        true,
        false,
    )?;
    let serving_development = serving_screen
        .passed
        .then(|| {
            shared_screen(
                0..QUERY_ROWS,
                &queries,
                &truth,
                &owners,
                &projection,
                &tree,
                &leaves,
                false,
                true,
            )
        })
        .transpose()?;
    let mut bytes = serde_json::to_vec(&SharedDictionaryResult {
        schema: "borsuk-v43-shared-dictionary-probe-v1",
        claim_eligible: false,
        source_rows: SOURCE_ROWS,
        dimensions: PROJECTED_DIMENSIONS,
        leaves: SHARED_LEAVES,
        visited_leaves: SHARED_VISITED_LEAVES,
        maximum_node_visits: SHARED_MAXIMUM_NODE_VISITS,
        complete_owner_pair_records,
        selected_pages: SELECTED_PAGES,
        construction_elapsed_ms,
        serving_screen,
        exhaustive_screen,
        serving_development,
        validation_opened: false,
    })
    .map_err(|error| invalid(&format!("V43 result encoding failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

const V44_LANDMARKS: usize = 512;

/// Inputs for the disposable nonlinear page-kernel probe.
#[doc(hidden)]
pub struct V44PageKernelProbeRequest {
    pub source: PathBuf,
    pub development_query: PathBuf,
    pub development_ground_truth: PathBuf,
    pub spill_relation: PathBuf,
    pub spill_postings: PathBuf,
    pub workers: usize,
}

#[derive(Serialize)]
struct V44PageKernelResult {
    schema: &'static str,
    claim_eligible: bool,
    source_rows: usize,
    dimensions: usize,
    landmarks: usize,
    bandwidth_squared: f64,
    page_embedding_bytes_at_1m: usize,
    projected_page_embedding_bytes_at_100m: usize,
    selected_pages: usize,
    construction_elapsed_ms: u64,
    screen: SharedScreen,
    development: Option<SharedScreen>,
    validation_opened: bool,
}

fn v44_landmark_rows(row_count: usize) -> Vec<usize> {
    (0..V44_LANDMARKS)
        .map(|index| index * row_count / V44_LANDMARKS)
        .collect()
}

fn v44_bandwidth_squared(coordinates: &[f32], landmark_rows: &[usize]) -> f64 {
    let mut distances = (0..landmark_rows.len() / 2)
        .map(|index| {
            let left = landmark_rows[index * 2];
            let right = landmark_rows[index * 2 + 1];
            f64::from(squared_distance(
                &coordinates[left * PROJECTED_DIMENSIONS..(left + 1) * PROJECTED_DIMENSIONS],
                &coordinates[right * PROJECTED_DIMENSIONS..(right + 1) * PROJECTED_DIMENSIONS],
            ))
        })
        .filter(|distance| distance.is_finite() && *distance > 0.0)
        .collect::<Vec<_>>();
    distances.sort_unstable_by(f64::total_cmp);
    distances[distances.len() / 2]
}

fn v44_features(
    vector: &[f32],
    coordinates: &[f32],
    landmark_rows: &[usize],
    inverse_bandwidth: f64,
    output: &mut [f64],
) {
    for (feature, row) in output.iter_mut().zip(landmark_rows) {
        let distance = f64::from(squared_distance(
            vector,
            &coordinates[row * PROJECTED_DIMENSIONS..(row + 1) * PROJECTED_DIMENSIONS],
        ));
        *feature = (-distance * inverse_bandwidth).exp();
    }
}

fn v44_page_embeddings(
    coordinates: &[f32],
    feature_ids: &[u64],
    landmark_rows: &[usize],
    inverse_bandwidth: f64,
    owners: &BTreeMap<u64, OwnerPair>,
) -> Result<(Vec<f64>, Vec<u64>)> {
    let dimensions = PAGE_COUNT as usize * V44_LANDMARKS;
    let (mut sums, counts) = (0..feature_ids.len())
        .into_par_iter()
        .try_fold(
            || (vec![0.0_f64; dimensions], vec![0_u64; PAGE_COUNT as usize]),
            |(mut sums, mut counts), row| -> Result<_> {
                let mut features = [0.0_f64; V44_LANDMARKS];
                v44_features(
                    &coordinates[row * PROJECTED_DIMENSIONS..(row + 1) * PROJECTED_DIMENSIONS],
                    coordinates,
                    landmark_rows,
                    inverse_bandwidth,
                    &mut features,
                );
                let pair = *owners
                    .get(&feature_ids[row])
                    .ok_or_else(|| invalid("V44 source owner differs"))?;
                for page in [pair.0, pair.1] {
                    if page == u32::MAX {
                        continue;
                    }
                    counts[page as usize] += 1;
                    let target = &mut sums
                        [page as usize * V44_LANDMARKS..(page as usize + 1) * V44_LANDMARKS];
                    for (target, value) in target.iter_mut().zip(features) {
                        *target += value;
                    }
                }
                Ok((sums, counts))
            },
        )
        .try_reduce(
            || (vec![0.0_f64; dimensions], vec![0_u64; PAGE_COUNT as usize]),
            |(mut left_sums, mut left_counts), (right_sums, right_counts)| -> Result<_> {
                for (left, right) in left_sums.iter_mut().zip(right_sums) {
                    *left += right;
                }
                for (left, right) in left_counts.iter_mut().zip(right_counts) {
                    *left += right;
                }
                Ok((left_sums, left_counts))
            },
        )?;
    for page in 0..PAGE_COUNT as usize {
        if counts[page] == 0 {
            return Err(invalid("V44 empty page embedding"));
        }
        let inverse = 1.0 / counts[page] as f64;
        for value in &mut sums[page * V44_LANDMARKS..(page + 1) * V44_LANDMARKS] {
            *value *= inverse;
        }
    }
    Ok((sums, counts))
}

fn v44_screen(
    query_indices: impl Iterator<Item = usize>,
    queries: &[crate::v36_prefix_dataset::V36PrefixQueryRow],
    truth: &[crate::v37_relation_router::V37FeatureGroundTruth],
    owners: &BTreeMap<u64, OwnerPair>,
    projection: &crate::v35_projection::V35Projection,
    coordinates: &[f32],
    landmark_rows: &[usize],
    inverse_bandwidth: f64,
    page_embeddings: &[f64],
    promotion: bool,
) -> Result<SharedScreen> {
    let mut hits = Vec::new();
    let mut timings = Vec::new();
    for query_index in query_indices {
        let projected = crate::v35_projection::project_v35_query_simd(
            projection,
            &queries[query_index].embedding,
        )?;
        let query = projected
            .coordinates()
            .iter()
            .map(|value| *value as f32)
            .collect::<Vec<_>>();
        let started = std::time::Instant::now();
        let mut features = [0.0_f64; V44_LANDMARKS];
        v44_features(
            &query,
            coordinates,
            landmark_rows,
            inverse_bandwidth,
            &mut features,
        );
        let scores = (0..PAGE_COUNT)
            .map(|page| {
                let embedding = &page_embeddings
                    [page as usize * V44_LANDMARKS..(page as usize + 1) * V44_LANDMARKS];
                let score = embedding
                    .iter()
                    .zip(features)
                    .map(|(left, right)| left * right)
                    .sum::<f64>();
                (page, score)
            })
            .collect::<BTreeMap<_, _>>();
        let selected = top_pages(&scores, SELECTED_PAGES);
        timings.push(started.elapsed().as_micros() as u64);
        hits.push(gt_hits(
            &truth[query_index].feature_row_ids,
            owners,
            &selected,
        )?);
    }
    timings.sort_unstable();
    let total = hits.iter().map(|value| u64::from(*value)).sum::<u64>();
    let aggregate = u32::try_from(total * 1_000_000 / (hits.len() as u64 * 100)).unwrap();
    let minimum = hits.iter().copied().min().unwrap() * 10_000;
    let passed = if promotion {
        aggregate >= 995_000 && minimum >= 800_000
    } else {
        aggregate >= 990_000 && minimum >= 700_000
    };
    Ok(SharedScreen {
        queries: hits.len(),
        aggregate_recall_ppm: aggregate,
        minimum_recall_ppm: minimum,
        p50_query_us: timings[timings.len() / 2],
        p99_query_us: timings[(timings.len() * 99 / 100).min(timings.len() - 1)],
        passed,
    })
}

/// Run one corpus-only nonlinear kernel-mean page router probe.
#[doc(hidden)]
pub fn run_v44_page_kernel_probe(request: V44PageKernelProbeRequest) -> Result<Vec<u8>> {
    ThreadPoolBuilder::new()
        .num_threads(request.workers)
        .build_global()
        .map_err(|_| invalid("V44 worker pool differs"))?;
    let relation = fs::read(&request.spill_relation).map_err(|source| BorsukError::Io {
        path: request.spill_relation.clone(),
        source,
    })?;
    let postings = fs::read(&request.spill_postings).map_err(|source| BorsukError::Io {
        path: request.spill_postings.clone(),
        source,
    })?;
    let owners = v38_v40_owner_rows_from_artifacts(
        &relation,
        &postings,
        SOURCE_ROWS,
        PAGE_COUNT,
        MAXIMUM_ROWS_PER_PAGE,
    )?
    .into_iter()
    .map(|(feature, primary, alternate)| (feature, (primary, alternate.unwrap_or(u32::MAX))))
    .collect::<BTreeMap<_, _>>();
    let source_ids = crate::v36_prefix_dataset::load_v36_prefix_source_feature_ids_file(
        File::open(&request.source).map_err(|source| BorsukError::Io {
            path: request.source.clone(),
            source,
        })?,
        &request.source,
        SOURCE_ROWS as u64,
    )?;
    let projected = crate::v36_prefix_dataset::project_v36_prefix_source_resident_file(
        File::open(&request.source).map_err(|source| BorsukError::Io {
            path: request.source.clone(),
            source,
        })?,
        &request.source,
        &source_ids,
        65_536,
    )?;
    let construction_started = std::time::Instant::now();
    let landmark_rows = v44_landmark_rows(projected.row_count());
    let bandwidth_squared =
        v44_bandwidth_squared(projected.projected_coordinates(), &landmark_rows);
    let (page_embeddings, _) = v44_page_embeddings(
        projected.projected_coordinates(),
        projected.feature_ids(),
        &landmark_rows,
        1.0 / bandwidth_squared,
        &owners,
    )?;
    let construction_elapsed_ms = construction_started.elapsed().as_millis() as u64;
    let mut queries = Vec::with_capacity(QUERY_ROWS);
    crate::v36_prefix_dataset::scan_v36_prefix_query_parquet(
        &request.development_query,
        QUERY_ROWS as u64,
        |batch| {
            queries.extend(crate::v36_prefix_dataset::v36_prefix_query_rows_from_batch(
                &batch,
                0,
                batch.num_rows(),
            )?);
            Ok(())
        },
    )?;
    let truth = crate::v37_relation_router::load_v37_feature_ground_truth_file(
        File::open(&request.development_ground_truth).map_err(|source| BorsukError::Io {
            path: request.development_ground_truth.clone(),
            source,
        })?,
        &request.development_ground_truth,
        QUERY_ROWS as u32,
    )?;
    let projection = crate::v36_funnel_geometry::build_v36_srht192_control()?;
    let screen = v44_screen(
        (0..QUERY_ROWS).step_by(SHARED_SCREEN_STRIDE),
        &queries,
        &truth,
        &owners,
        &projection,
        projected.projected_coordinates(),
        &landmark_rows,
        1.0 / bandwidth_squared,
        &page_embeddings,
        false,
    )?;
    let development = screen
        .passed
        .then(|| {
            v44_screen(
                0..QUERY_ROWS,
                &queries,
                &truth,
                &owners,
                &projection,
                projected.projected_coordinates(),
                &landmark_rows,
                1.0 / bandwidth_squared,
                &page_embeddings,
                true,
            )
        })
        .transpose()?;
    let projected_pages_at_100m = PAGE_COUNT as usize * 100;
    let mut bytes = serde_json::to_vec(&V44PageKernelResult {
        schema: "borsuk-v44-page-kernel-probe-v1",
        claim_eligible: false,
        source_rows: SOURCE_ROWS,
        dimensions: PROJECTED_DIMENSIONS,
        landmarks: V44_LANDMARKS,
        bandwidth_squared,
        page_embedding_bytes_at_1m: page_embeddings.len() * std::mem::size_of::<f32>(),
        projected_page_embedding_bytes_at_100m: projected_pages_at_100m
            * V44_LANDMARKS
            * std::mem::size_of::<f32>(),
        selected_pages: SELECTED_PAGES,
        construction_elapsed_ms,
        screen,
        development,
        validation_opened: false,
    })
    .map_err(|error| invalid(&format!("V44 result encoding failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}
