//! Throwaway original-space balanced-tree row-candidate probe on ReLAION2B one million.

use crate::error::{BorsukError, Result};
use crate::v37_relation_router::{V37BalancedTree, V37TrainingShape};
use crate::v38_boundary_spill::v38_v40_owner_rows_from_artifacts;
use arrow_array::{Array, FixedSizeListArray, Float32Array};
use rayon::ThreadPoolBuilder;
use serde::Serialize;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashSet},
    fs::{self, File},
    path::PathBuf,
    time::Instant,
};

const SOURCE_ROWS: usize = 1_000_000;
const QUERY_ROWS: usize = 1_000;
const DIMENSIONS: usize = 768;
const PAGE_COUNT: u32 = 123;
const MAXIMUM_ROWS_PER_PAGE: u32 = 10_240;
const SELECTED_PAGES: usize = 21;
const TREE_COUNT: usize = 4;
const LEAVES_PER_TREE: u64 = 16_384;
const SELECTED_LEAVES_PER_TREE: usize = 64;
const MAXIMUM_NODE_VISITS_PER_TREE: usize = 2_048;
const RESERVOIR_ROWS: usize = 128;
const TWO_MEANS_ITERATIONS: usize = 8;
const FIRST_SEED: u64 = 52_001;
const SCREEN_STRIDE: usize = 4;

type OwnerPair = (u32, u32);

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

/// Inputs for the disposable original-space balanced-tree probe.
#[doc(hidden)]
pub struct V51TreeRowProbeRequest {
    pub source: PathBuf,
    pub development_query: PathBuf,
    pub development_ground_truth: PathBuf,
    pub spill_relation: PathBuf,
    pub spill_postings: PathBuf,
    pub workers: usize,
}

#[derive(Clone, Serialize)]
struct Screen {
    queries: usize,
    aggregate_recall_ppm: u32,
    minimum_recall_ppm: u32,
    candidate_truth_recall_ppm: u32,
    ranked_truth_recall_ppm: u32,
    p50_query_us: u64,
    p99_query_us: u64,
    median_candidate_rows: usize,
    p99_candidate_rows: usize,
    maximum_node_visits: usize,
    passed: bool,
}

#[derive(Serialize)]
struct ProbeResult {
    schema: &'static str,
    claim_eligible: bool,
    source_rows: usize,
    dimensions: usize,
    trees: usize,
    leaves_per_tree: u64,
    selected_leaves_per_tree: usize,
    maximum_node_visits_per_tree: usize,
    reservoir_rows: usize,
    two_means_iterations: usize,
    first_seed: u64,
    selected_pages: usize,
    construction_elapsed_ms: u64,
    tree_bytes: usize,
    screen: Screen,
    development: Option<Screen>,
    validation_opened: bool,
}

#[derive(Clone, Copy, Debug)]
struct QueueEntry {
    penalty: f32,
    node: u32,
}

impl PartialEq for QueueEntry {
    fn eq(&self, other: &Self) -> bool {
        self.penalty.to_bits() == other.penalty.to_bits() && self.node == other.node
    }
}

impl Eq for QueueEntry {}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .penalty
            .total_cmp(&self.penalty)
            .then_with(|| other.node.cmp(&self.node))
    }
}

fn fused_dot(query: &[f32], normal: &[f32], kernel: borsuk_fma::FusedDot8x12) -> Result<f32> {
    if query.len() != DIMENSIONS || normal.len() != DIMENSIONS {
        return Err(invalid("V51 tree dimension differs"));
    }
    let mut score = 0.0_f32;
    for block in 0..DIMENSIONS / 96 {
        let start = block * 96;
        let left: &[f32; 96] = query[start..start + 96]
            .try_into()
            .map_err(|_| invalid("V51 query block differs"))?;
        let right: &[f32; 96] = normal[start..start + 96]
            .try_into()
            .map_err(|_| invalid("V51 normal block differs"))?;
        score += kernel.dot(left, right);
    }
    if !score.is_finite() {
        return Err(invalid("V51 tree score is non-finite"));
    }
    Ok(score)
}

fn selected_tree_leaves(
    tree: &V37BalancedTree,
    query: &[f32],
    kernel: borsuk_fma::FusedDot8x12,
) -> Result<(Vec<u32>, usize)> {
    let mut queue = BinaryHeap::from([QueueEntry {
        penalty: 0.0,
        node: 0,
    }]);
    let mut selected = Vec::with_capacity(SELECTED_LEAVES_PER_TREE);
    let mut visits = 0_usize;
    while selected.len() < SELECTED_LEAVES_PER_TREE {
        let next = queue
            .pop()
            .ok_or_else(|| invalid("V51 tree candidate shortage"))?;
        visits += 1;
        if visits > MAXIMUM_NODE_VISITS_PER_TREE {
            return Err(invalid("V51 tree node visit limit exceeded"));
        }
        let node = tree
            .nodes
            .get(next.node as usize)
            .ok_or_else(|| invalid("V51 tree node differs"))?;
        if let Some(posting) = node.posting_ordinal {
            selected.push(posting);
            continue;
        }
        let score = fused_dot(query, &node.normal, kernel)?;
        let boundary = f32::from_bits(node.boundary_score_bits);
        let margin = score - boundary;
        let sibling_penalty = next.penalty.max(margin * margin);
        let (primary, sibling) = if score.total_cmp(&boundary).is_le() {
            (node.left_node, node.right_node)
        } else {
            (node.right_node, node.left_node)
        };
        queue.push(QueueEntry {
            penalty: next.penalty,
            node: primary.ok_or_else(|| invalid("V51 primary child differs"))?,
        });
        queue.push(QueueEntry {
            penalty: sibling_penalty,
            node: sibling.ok_or_else(|| invalid("V51 sibling child differs"))?,
        });
    }
    Ok((selected, visits))
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

fn exact_top100(query: &[f32], coordinates: &[f32], candidates: &[u32]) -> Vec<u32> {
    let mut ranked = candidates
        .iter()
        .copied()
        .map(|row| {
            let ordinal = row as usize;
            (
                squared_distance(
                    query,
                    &coordinates[ordinal * DIMENSIONS..(ordinal + 1) * DIMENSIONS],
                ),
                row,
            )
        })
        .collect::<Vec<_>>();
    if ranked.len() > 100 {
        ranked.select_nth_unstable_by(100, |left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        ranked.truncate(100);
    }
    ranked.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    ranked.into_iter().map(|entry| entry.1).collect()
}

fn select_pages(
    ranked: &[u32],
    feature_ids: &[u64],
    owners: &BTreeMap<u64, OwnerPair>,
) -> Result<BTreeSet<u32>> {
    let mut evidence = Vec::with_capacity(100);
    let mut candidates = BTreeSet::new();
    for row in ranked {
        let pair = *owners
            .get(&feature_ids[*row as usize])
            .ok_or_else(|| invalid("V51 ranked row owner differs"))?;
        candidates.insert(pair.0);
        if pair.1 != u32::MAX {
            candidates.insert(pair.1);
        }
        evidence.push(pair);
    }
    let mut selected = BTreeSet::new();
    while selected.len() < SELECTED_PAGES {
        let best = candidates
            .iter()
            .copied()
            .filter(|page| !selected.contains(page))
            .map(|page| {
                let gain = evidence
                    .iter()
                    .filter(|(primary, alternate)| {
                        !selected.contains(primary)
                            && (*alternate == u32::MAX || !selected.contains(alternate))
                    })
                    .filter(|(primary, alternate)| page == *primary || page == *alternate)
                    .count();
                (gain, std::cmp::Reverse(page))
            })
            .max();
        let Some((_, std::cmp::Reverse(page))) = best else {
            break;
        };
        selected.insert(page);
    }
    for page in 0..PAGE_COUNT {
        if selected.len() == SELECTED_PAGES {
            break;
        }
        selected.insert(page);
    }
    Ok(selected)
}

#[allow(clippy::too_many_arguments)]
fn evaluate(
    query_indices: impl Iterator<Item = usize>,
    queries: &[crate::v36_prefix_dataset::V36PrefixQueryRow],
    truth: &[crate::v37_relation_router::V37FeatureGroundTruth],
    coordinates: &[f32],
    feature_ids: &[u64],
    owners: &BTreeMap<u64, OwnerPair>,
    trees: &[V37BalancedTree],
    leaf_rows: &[Vec<Vec<u32>>],
    kernel: borsuk_fma::FusedDot8x12,
    promotion: bool,
) -> Result<Screen> {
    let mut final_hits = Vec::new();
    let mut candidate_hits = 0_usize;
    let mut ranked_hits = 0_usize;
    let mut timings = Vec::new();
    let mut candidate_counts = Vec::new();
    let mut maximum_node_visits = 0_usize;
    for query_index in query_indices {
        let started = Instant::now();
        let mut candidates = Vec::new();
        let mut total_visits = 0_usize;
        for (tree, rows) in trees.iter().zip(leaf_rows) {
            let (leaves, visits) =
                selected_tree_leaves(tree, &queries[query_index].embedding, kernel)?;
            total_visits += visits;
            for leaf in leaves {
                candidates.extend_from_slice(&rows[leaf as usize]);
            }
        }
        maximum_node_visits = maximum_node_visits.max(total_visits);
        candidates.sort_unstable();
        candidates.dedup();
        candidate_counts.push(candidates.len());
        let candidate_features = candidates
            .iter()
            .map(|row| feature_ids[*row as usize])
            .collect::<HashSet<_>>();
        candidate_hits += truth[query_index]
            .feature_row_ids
            .iter()
            .filter(|feature| candidate_features.contains(feature))
            .count();
        let ranked = exact_top100(&queries[query_index].embedding, coordinates, &candidates);
        let ranked_features = ranked
            .iter()
            .map(|row| feature_ids[*row as usize])
            .collect::<HashSet<_>>();
        ranked_hits += truth[query_index]
            .feature_row_ids
            .iter()
            .filter(|feature| ranked_features.contains(feature))
            .count();
        let selected = select_pages(&ranked, feature_ids, owners)?;
        timings.push(started.elapsed().as_micros() as u64);
        final_hits.push(
            truth[query_index]
                .feature_row_ids
                .iter()
                .filter(|feature| {
                    owners.get(feature).is_some_and(|pair| {
                        selected.contains(&pair.0)
                            || (pair.1 != u32::MAX && selected.contains(&pair.1))
                    })
                })
                .count(),
        );
    }
    timings.sort_unstable();
    candidate_counts.sort_unstable();
    let queries = final_hits.len();
    let aggregate_recall_ppm =
        u32::try_from(final_hits.iter().sum::<usize>() * 1_000_000 / (queries * 100)).unwrap();
    let minimum_recall_ppm =
        u32::try_from(final_hits.iter().copied().min().unwrap() * 10_000).unwrap();
    let passed = if promotion {
        aggregate_recall_ppm >= 995_000 && minimum_recall_ppm >= 800_000
    } else {
        aggregate_recall_ppm >= 990_000 && minimum_recall_ppm >= 700_000
    };
    Ok(Screen {
        queries,
        aggregate_recall_ppm,
        minimum_recall_ppm,
        candidate_truth_recall_ppm: u32::try_from(candidate_hits * 1_000_000 / (queries * 100))
            .unwrap(),
        ranked_truth_recall_ppm: u32::try_from(ranked_hits * 1_000_000 / (queries * 100)).unwrap(),
        p50_query_us: timings[timings.len() / 2],
        p99_query_us: timings[(timings.len() * 99 / 100).min(timings.len() - 1)],
        median_candidate_rows: candidate_counts[candidate_counts.len() / 2],
        p99_candidate_rows: candidate_counts
            [(candidate_counts.len() * 99 / 100).min(candidate_counts.len() - 1)],
        maximum_node_visits,
        passed,
    })
}

/// Run one original-space balanced-tree row-candidate probe.
#[doc(hidden)]
pub fn run_v51_tree_row_probe(request: V51TreeRowProbeRequest) -> Result<Vec<u8>> {
    ThreadPoolBuilder::new()
        .num_threads(request.workers)
        .build_global()
        .map_err(|_| invalid("V51 worker pool differs"))?;
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
    let mut coordinates = Vec::with_capacity(SOURCE_ROWS * DIMENSIONS);
    crate::v36_prefix_dataset::scan_v36_prefix_source_parquet(
        &request.source,
        &source_ids,
        |batch| {
            let embeddings = batch
                .column(1)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| invalid("V51 source embedding column differs"))?;
            let values = embeddings
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| invalid("V51 source embedding child differs"))?;
            coordinates.extend_from_slice(values.values());
            Ok(())
        },
    )?;
    if coordinates.len() != SOURCE_ROWS * DIMENSIONS {
        return Err(invalid("V51 source embedding count differs"));
    }
    let construction_started = Instant::now();
    let mut trees = Vec::with_capacity(TREE_COUNT);
    for tree in 0..TREE_COUNT {
        let trained = crate::v37_relation_router::train_v37_ownership_tree_resident_coordinates(
            &coordinates,
            V37TrainingShape {
                dimensions: DIMENSIONS,
                leaf_count: LEAVES_PER_TREE,
                reservoir_rows: RESERVOIR_ROWS,
                two_means_iterations: TWO_MEANS_ITERATIONS,
            },
            FIRST_SEED + tree as u64,
            request.workers,
            65_536,
        )?;
        crate::v37_relation_router::validate_v37_tree_geometry(&trained)?;
        trees.push(trained);
    }
    let construction_elapsed_ms = construction_started.elapsed().as_millis() as u64;
    let mut leaf_rows = Vec::with_capacity(TREE_COUNT);
    for tree in &trees {
        let mut rows = (0..LEAVES_PER_TREE).map(|_| Vec::new()).collect::<Vec<_>>();
        for assignment in &tree.assignments {
            rows[assignment.posting_ordinal as usize].push(assignment.source_ordinal as u32);
        }
        leaf_rows.push(rows);
    }
    let tree_bytes = trees
        .iter()
        .flat_map(|tree| &tree.nodes)
        .map(|node| node.normal.len() * size_of::<f32>() + 32)
        .sum();
    let kernel = borsuk_fma::FusedDot8x12::detect()
        .map_err(|_| invalid("V51 fused SIMD backend is unavailable"))?;
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
    let screen = evaluate(
        (0..QUERY_ROWS).step_by(SCREEN_STRIDE),
        &queries,
        &truth,
        &coordinates,
        &source_ids,
        &owners,
        &trees,
        &leaf_rows,
        kernel,
        false,
    )?;
    let development = screen
        .passed
        .then(|| {
            evaluate(
                0..QUERY_ROWS,
                &queries,
                &truth,
                &coordinates,
                &source_ids,
                &owners,
                &trees,
                &leaf_rows,
                kernel,
                true,
            )
        })
        .transpose()?;
    let mut bytes = serde_json::to_vec(&ProbeResult {
        schema: "borsuk-v52-original-tree-forest-row-probe-v1",
        claim_eligible: false,
        source_rows: SOURCE_ROWS,
        dimensions: DIMENSIONS,
        trees: TREE_COUNT,
        leaves_per_tree: LEAVES_PER_TREE,
        selected_leaves_per_tree: SELECTED_LEAVES_PER_TREE,
        maximum_node_visits_per_tree: MAXIMUM_NODE_VISITS_PER_TREE,
        reservoir_rows: RESERVOIR_ROWS,
        two_means_iterations: TWO_MEANS_ITERATIONS,
        first_seed: FIRST_SEED,
        selected_pages: SELECTED_PAGES,
        construction_elapsed_ms,
        tree_bytes,
        screen,
        development,
        validation_opened: false,
    })
    .map_err(|error| invalid(&format!("V51 result encoding failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}
