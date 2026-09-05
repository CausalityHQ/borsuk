use std::{cmp::Ordering, collections::BinaryHeap};

use crate::{BorsukError, Result, V34Rank4Generation, score_v34_rank4_leaf};

const V34_DIMENSIONS: usize = 96;
const V34_MAX_GROUPS: u32 = 64;
const V34_MAX_ROWS: u64 = 262_144;
const V34_MAX_CODE_BYTES: u64 = 8 * 1_048_576;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Immutable row and encoded-byte authority for one dense storage group.
pub struct V34GroupStorage {
    group_ordinal: u32,
    rows: u64,
    code_bytes: u64,
}

impl V34GroupStorage {
    /// Construct one nonempty dense group identity.
    pub fn new(group_ordinal: u32, rows: u64, code_bytes: u64) -> Result<Self> {
        if rows == 0 || code_bytes == 0 {
            return Err(invalid("V34 group storage authority differs"));
        }
        Ok(Self {
            group_ordinal,
            rows,
            code_bytes,
        })
    }

    /// Dense group ordinal.
    pub fn group_ordinal(self) -> u32 {
        self.group_ordinal
    }

    /// Logical rows stored by this group.
    pub fn rows(self) -> u64 {
        self.rows
    }

    /// Exact encoded code-object bytes stored by this group.
    pub fn code_bytes(self) -> u64 {
        self.code_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Fixed complete-prefix serving limits for one V34 route.
pub struct V34RouteBudget {
    max_groups: u32,
    max_rows: u64,
    max_code_bytes: u64,
}

impl V34RouteBudget {
    /// Construct limits no wider than the frozen V34 serving envelope.
    pub fn new(max_groups: u32, max_rows: u64, max_code_bytes: u64) -> Result<Self> {
        if max_groups == 0
            || max_groups > V34_MAX_GROUPS
            || max_rows == 0
            || max_rows > V34_MAX_ROWS
            || max_code_bytes == 0
            || max_code_bytes > V34_MAX_CODE_BYTES
        {
            return Err(invalid("V34 route budget differs"));
        }
        Ok(Self {
            max_groups,
            max_rows,
            max_code_bytes,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
/// One group ordered by its exact minimum rank-four leaf score.
pub struct V34SelectedGroup {
    storage: V34GroupStorage,
    score: f64,
}

impl V34SelectedGroup {
    /// Dense group ordinal.
    pub fn group_ordinal(self) -> u32 {
        self.storage.group_ordinal
    }

    /// Exact minimum score among leaves owned by this group.
    pub fn score(self) -> f64 {
        self.score
    }

    /// Logical rows admitted with this complete group.
    pub fn rows(self) -> u64 {
        self.storage.rows
    }

    /// Encoded code-object bytes admitted with this complete group.
    pub fn code_bytes(self) -> u64 {
        self.storage.code_bytes
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Exact complete route prefix and the first group that did not fit.
pub struct V34RoutePrefix {
    selected_groups: Vec<V34SelectedGroup>,
    overflow: Option<V34SelectedGroup>,
    selected_rows: u64,
    selected_code_bytes: u64,
    exact_leaf_evaluations: usize,
    bound_evaluations: usize,
}

impl V34RoutePrefix {
    /// Admitted groups in exact score order.
    pub fn selected_groups(&self) -> &[V34SelectedGroup] {
        &self.selected_groups
    }

    /// First group rejected by any prefix budget, without considering later groups.
    pub fn overflow(&self) -> Option<&V34SelectedGroup> {
        self.overflow.as_ref()
    }

    /// Checked sum of admitted logical rows.
    pub fn selected_rows(&self) -> u64 {
        self.selected_rows
    }

    /// Checked sum of admitted encoded code-object bytes.
    pub fn selected_code_bytes(&self) -> u64 {
        self.selected_code_bytes
    }

    /// Number of exact leaf scores evaluated by this route.
    pub fn exact_leaf_evaluations(&self) -> usize {
        self.exact_leaf_evaluations
    }

    /// Number of conservative node bounds evaluated by this route.
    pub fn bound_evaluations(&self) -> usize {
        self.bound_evaluations
    }
}

/// Score every leaf once and admit the exact complete group prefix.
pub fn exhaustive_v34_route(
    generation: &V34Rank4Generation,
    query: &[f32; V34_DIMENSIONS],
    groups: &[V34GroupStorage],
    budget: V34RouteBudget,
) -> Result<V34RoutePrefix> {
    if query.iter().any(|value| !value.is_finite())
        || groups.len() != generation.group_count() as usize
    {
        return Err(invalid("V34 route authority differs"));
    }
    for (ordinal, (group, expected)) in groups.iter().zip(generation.group_rows()).enumerate() {
        if group.group_ordinal != ordinal as u32 || group.rows != *expected {
            return Err(invalid("V34 route group storage differs"));
        }
    }

    let mut minima = vec![f64::INFINITY; groups.len()];
    for leaf in generation.leaves() {
        let score = score_v34_rank4_leaf(leaf, query)?;
        let minimum = &mut minima[leaf.group_ordinal() as usize];
        if score.total_cmp(minimum).is_lt() {
            *minimum = score;
        }
    }
    let mut ordered = groups
        .iter()
        .copied()
        .zip(minima)
        .map(|(storage, score)| V34SelectedGroup { storage, score })
        .collect::<Vec<_>>();
    if ordered.iter().any(|group| !group.score.is_finite()) {
        return Err(invalid("V34 route group minimum differs"));
    }
    ordered.sort_by(|left, right| {
        left.score
            .total_cmp(&right.score)
            .then_with(|| left.storage.group_ordinal.cmp(&right.storage.group_ordinal))
    });

    let mut selected_groups = Vec::new();
    let mut selected_rows = 0_u64;
    let mut selected_code_bytes = 0_u64;
    let mut overflow = None;
    for group in ordered {
        let next_rows = selected_rows.checked_add(group.storage.rows);
        let next_code_bytes = selected_code_bytes.checked_add(group.storage.code_bytes);
        if selected_groups.len() >= budget.max_groups as usize
            || next_rows.is_none_or(|rows| rows > budget.max_rows)
            || next_code_bytes.is_none_or(|bytes| bytes > budget.max_code_bytes)
        {
            overflow = Some(group);
            break;
        }
        selected_rows = next_rows.expect("checked above");
        selected_code_bytes = next_code_bytes.expect("checked above");
        selected_groups.push(group);
    }
    Ok(V34RoutePrefix {
        selected_groups,
        overflow,
        selected_rows,
        selected_code_bytes,
        exact_leaf_evaluations: generation.leaves().len(),
        bound_evaluations: 0,
    })
}

fn next_up(value: f64) -> f64 {
    if value == f64::INFINITY {
        value
    } else if value == 0.0 {
        f64::from_bits(1)
    } else if value > 0.0 {
        f64::from_bits(value.to_bits() + 1)
    } else {
        f64::from_bits(value.to_bits() - 1)
    }
}

fn next_down(value: f64) -> f64 {
    if value == f64::NEG_INFINITY {
        value
    } else if value == 0.0 {
        -f64::from_bits(1)
    } else if value > 0.0 {
        f64::from_bits(value.to_bits() - 1)
    } else {
        f64::from_bits(value.to_bits() + 1)
    }
}

fn squared_distance_interval(
    left: &[f32; V34_DIMENSIONS],
    right: &[f32; V34_DIMENSIONS],
) -> (f64, f64) {
    let mut lower = 0.0_f64;
    let mut upper = 0.0_f64;
    for dimension in 0..V34_DIMENSIONS {
        let delta = f64::from(left[dimension]) - f64::from(right[dimension]);
        let square = delta * delta;
        lower = next_down(lower + next_down(square)).max(0.0);
        upper = next_up(upper + next_up(square));
    }
    (lower, upper)
}

fn min_v34_lower_tail_over_interval(
    distance_min: f64,
    distance_max: f64,
    trace_min: f64,
    trace_square_max: f64,
    population_factor_max: f64,
    spectral_bound_max: f64,
) -> f64 {
    let evaluate_interval = |distance_lower: f64, distance_upper: f64| {
        let positive = next_down(distance_lower + trace_min);
        let twice_trace_square = next_up(2.0 * trace_square_max);
        let four_spectral = next_up(4.0 * spectral_bound_max);
        let distance_term = next_up(four_spectral * distance_upper);
        let radicand = next_up(twice_trace_square + distance_term).max(0.0);
        let deviation = next_up(population_factor_max * next_up(radicand.sqrt()));
        next_down(positive - deviation)
    };

    let mut minimum =
        evaluate_interval(next_down(distance_min).max(0.0), next_up(distance_min)).min(
            evaluate_interval(next_down(distance_max).max(0.0), next_up(distance_max)),
        );
    if spectral_bound_max > 0.0 {
        let factor_square_lower = next_down(population_factor_max * population_factor_max);
        let factor_square_upper = next_up(population_factor_max * population_factor_max);
        let first_lower = next_down(factor_square_lower * spectral_bound_max);
        let first_upper = next_up(factor_square_upper * spectral_bound_max);
        let denominator_lower = next_down(2.0 * spectral_bound_max);
        let denominator_upper = next_up(2.0 * spectral_bound_max);
        let second_lower = next_down(trace_square_max / denominator_upper);
        let second_upper = next_up(trace_square_max / denominator_lower);
        let stationary_lower =
            next_down(first_lower - second_upper).clamp(distance_min, distance_max);
        let stationary_upper =
            next_up(first_upper - second_lower).clamp(distance_min, distance_max);
        minimum = minimum.min(evaluate_interval(stationary_lower, stationary_upper));
    }

    // The longest dependent summation chain in the exact leaf scorer has
    // fewer than 600 rounded f64 operations. A 4096-epsilon absolute envelope
    // over all positive contributing terms
    // dominates gamma_600 plus sqrt and final-subtraction rounding.
    let radicand_max = next_up(
        next_up(2.0 * trace_square_max) + next_up(next_up(4.0 * spectral_bound_max) * distance_max),
    )
    .max(0.0);
    let trace_magnitude = next_up((96.0 * trace_square_max).sqrt());
    let score_scale = next_up(
        1.0 + distance_max
            + trace_magnitude
            + next_up(population_factor_max * next_up(radicand_max.sqrt())),
    );
    next_down(minimum - 4096.0 * f64::EPSILON * score_scale)
}

#[derive(Debug, Clone, PartialEq)]
/// One deterministic node in the V34 sixteen-way routing tree.
pub struct V34TreeNode {
    center: [f32; V34_DIMENSIONS],
    radius: f64,
    trace_min: f64,
    trace_square_max: f64,
    population_factor_max: f64,
    spectral_bound_max: f64,
    children: [u32; 16],
    child_count: u8,
    leaf_start: u32,
    leaf_count: u32,
}

impl V34TreeNode {
    /// Whether this node is a terminal bucket of at most sixteen leaves.
    pub fn is_terminal(&self) -> bool {
        self.child_count == 0
    }

    /// Number of direct child nodes.
    pub fn child_count(&self) -> usize {
        usize::from(self.child_count)
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Deterministic tree plus one nonduplicated permutation of leaf ordinals.
pub struct V34RouteTree {
    nodes: Vec<V34TreeNode>,
    leaf_order: Vec<u32>,
    generation_authority_digest: [u8; 32],
}

impl V34RouteTree {
    /// Tree nodes in deterministic pre-order, with the root at zero.
    pub fn nodes(&self) -> &[V34TreeNode] {
        &self.nodes
    }

    /// Root node ordinal.
    pub fn root(&self) -> u32 {
        0
    }

    /// Number of leaves covered exactly once.
    pub fn leaf_count(&self) -> usize {
        self.leaf_order.len()
    }

    /// Resolve one node ordinal.
    pub fn node(&self, ordinal: u32) -> Option<&V34TreeNode> {
        self.nodes.get(ordinal as usize)
    }

    /// Resolve the contiguous descendant-leaf permutation for one node.
    pub fn descendant_leaf_ordinals(&self, ordinal: u32) -> Option<&[u32]> {
        let node = self.node(ordinal)?;
        let start = node.leaf_start as usize;
        let end = start.checked_add(node.leaf_count as usize)?;
        self.leaf_order.get(start..end)
    }
}

fn build_v34_tree_node(
    generation: &V34Rank4Generation,
    ordinals: &mut [u32],
    nodes: &mut Vec<V34TreeNode>,
    leaf_order: &mut Vec<u32>,
) -> Result<u32> {
    let node_ordinal =
        u32::try_from(nodes.len()).map_err(|_| invalid("V34 route tree node count overflows"))?;
    let mut center = [0.0_f32; V34_DIMENSIONS];
    for (dimension, coordinate) in center.iter_mut().enumerate() {
        let sum = ordinals.iter().fold(0.0_f64, |total, ordinal| {
            total + f64::from(generation.leaves()[*ordinal as usize].mean()[dimension])
        });
        *coordinate = (sum / ordinals.len() as f64) as f32;
    }
    let mut radius = 0.0_f64;
    let mut trace_min = f64::INFINITY;
    let mut trace_square_max = 0.0_f64;
    let mut population_factor_max = 0.0_f64;
    let mut spectral_bound_max = 0.0_f64;
    for ordinal in ordinals.iter().copied() {
        let leaf = &generation.leaves()[ordinal as usize];
        let (_, distance_square_upper) = squared_distance_interval(leaf.mean(), &center);
        radius = radius.max(next_up(distance_square_upper.sqrt()));
        trace_min = trace_min.min(leaf.trace());
        trace_square_max = trace_square_max.max(leaf.trace_square());
        population_factor_max = population_factor_max.max(leaf.population_factor());
        spectral_bound_max = spectral_bound_max.max(leaf.spectral_bound());
    }
    let leaf_start = u32::try_from(leaf_order.len())
        .map_err(|_| invalid("V34 route tree leaf order overflows"))?;
    nodes.push(V34TreeNode {
        center,
        radius: next_up(radius),
        trace_min: next_down(trace_min),
        trace_square_max: next_up(trace_square_max),
        population_factor_max: next_up(population_factor_max),
        spectral_bound_max: next_up(spectral_bound_max),
        children: [0; 16],
        child_count: 0,
        leaf_start,
        leaf_count: 0,
    });
    if ordinals.len() <= 16 {
        leaf_order.extend_from_slice(ordinals);
    } else {
        let mut split_dimension = 0_usize;
        let mut largest_variance = f64::NEG_INFINITY;
        let total_population = ordinals.iter().fold(0.0_f64, |total, ordinal| {
            total + f64::from(generation.leaves()[*ordinal as usize].population())
        });
        for dimension in 0..V34_DIMENSIONS {
            let mean = ordinals.iter().fold(0.0_f64, |total, ordinal| {
                let leaf = &generation.leaves()[*ordinal as usize];
                total + f64::from(leaf.population()) * f64::from(leaf.mean()[dimension])
            }) / total_population;
            let variance = ordinals.iter().fold(0.0_f64, |total, ordinal| {
                let leaf = &generation.leaves()[*ordinal as usize];
                let delta = f64::from(leaf.mean()[dimension]) - mean;
                total + f64::from(leaf.population()) * delta * delta
            });
            if variance > largest_variance {
                largest_variance = variance;
                split_dimension = dimension;
            }
        }
        ordinals.sort_by(|left, right| {
            generation.leaves()[*left as usize].mean()[split_dimension]
                .total_cmp(&generation.leaves()[*right as usize].mean()[split_dimension])
                .then_with(|| left.cmp(right))
        });
        let base = ordinals.len() / 16;
        let remainder = ordinals.len() % 16;
        let mut offset = 0;
        for child in 0..16 {
            let length = base + usize::from(child < remainder);
            let child_ordinal = build_v34_tree_node(
                generation,
                &mut ordinals[offset..offset + length],
                nodes,
                leaf_order,
            )?;
            nodes[node_ordinal as usize].children[child] = child_ordinal;
            offset += length;
        }
        nodes[node_ordinal as usize].child_count = 16;
    }
    nodes[node_ordinal as usize].leaf_count = u32::try_from(leaf_order.len())
        .ok()
        .and_then(|end| end.checked_sub(leaf_start))
        .ok_or_else(|| invalid("V34 route tree leaf extent differs"))?;
    Ok(node_ordinal)
}

/// Build the deterministic sixteen-way tree over one validated generation.
pub fn build_v34_route_tree(generation: &V34Rank4Generation) -> Result<V34RouteTree> {
    let mut ordinals = (0..generation.leaves().len() as u32).collect::<Vec<_>>();
    let mut tree = V34RouteTree {
        nodes: Vec::new(),
        leaf_order: Vec::with_capacity(ordinals.len()),
        generation_authority_digest: generation.authority_digest(),
    };
    let root = build_v34_tree_node(
        generation,
        &mut ordinals,
        &mut tree.nodes,
        &mut tree.leaf_order,
    )?;
    if root != 0 || tree.leaf_order.len() != generation.leaves().len() {
        return Err(invalid("V34 route tree coverage differs"));
    }
    Ok(tree)
}

/// Compute a conservative lower bound for every exact score below one node.
pub fn bound_v34_node(
    tree: &V34RouteTree,
    node_ordinal: u32,
    query: &[f32; V34_DIMENSIONS],
) -> Result<f64> {
    let node = tree
        .node(node_ordinal)
        .ok_or_else(|| invalid("V34 route tree node differs"))?;
    if query.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V34 route tree query differs"));
    }
    let (distance_square_lower, distance_square_upper) =
        squared_distance_interval(query, &node.center);
    let distance_lower = next_down(distance_square_lower.sqrt()).max(0.0);
    let distance_upper = next_up(distance_square_upper.sqrt());
    let low = (next_down(distance_lower - node.radius)).max(0.0);
    let high = next_up(distance_upper + node.radius);
    let distance_min = next_down(low * low).max(0.0);
    let distance_max = next_up(high * high);
    let bound = min_v34_lower_tail_over_interval(
        distance_min,
        distance_max,
        node.trace_min,
        node.trace_square_max,
        node.population_factor_max,
        node.spectral_bound_max,
    );
    if bound.is_finite() {
        Ok(bound)
    } else {
        Err(invalid("V34 route tree bound differs"))
    }
}

#[derive(Debug, Clone, Copy)]
struct NodeCandidate {
    bound: f64,
    ordinal: u32,
}

impl PartialEq for NodeCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.bound.to_bits() == other.bound.to_bits() && self.ordinal == other.ordinal
    }
}

impl Eq for NodeCandidate {}

impl PartialOrd for NodeCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NodeCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .bound
            .total_cmp(&self.bound)
            .then_with(|| other.ordinal.cmp(&self.ordinal))
    }
}

#[derive(Debug, Clone, Copy)]
struct LeafCandidate {
    score: f64,
    group_ordinal: u32,
    ordinal: u32,
}

impl PartialEq for LeafCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits()
            && self.group_ordinal == other.group_ordinal
            && self.ordinal == other.ordinal
    }
}

impl Eq for LeafCandidate {}

impl PartialOrd for LeafCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LeafCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| other.group_ordinal.cmp(&self.group_ordinal))
            .then_with(|| other.ordinal.cmp(&self.ordinal))
    }
}

/// Traverse conservative bounds until the exact complete group prefix is certified.
pub fn hierarchical_v34_route(
    generation: &V34Rank4Generation,
    tree: &V34RouteTree,
    query: &[f32; V34_DIMENSIONS],
    groups: &[V34GroupStorage],
    budget: V34RouteBudget,
) -> Result<V34RoutePrefix> {
    if query.iter().any(|value| !value.is_finite())
        || groups.len() != generation.group_count() as usize
        || tree.leaf_count() != generation.leaves().len()
        || tree.generation_authority_digest != generation.authority_digest()
    {
        return Err(invalid("V34 hierarchical route authority differs"));
    }
    if groups
        .iter()
        .zip(generation.group_rows())
        .enumerate()
        .any(|(ordinal, (group, rows))| {
            group.group_ordinal != ordinal as u32 || group.rows != *rows
        })
    {
        return Err(invalid("V34 route group storage differs"));
    }

    let mut nodes = BinaryHeap::new();
    nodes.push(NodeCandidate {
        bound: bound_v34_node(tree, tree.root(), query)?,
        ordinal: tree.root(),
    });
    let mut bound_evaluations = 1_usize;
    let mut exact_leaf_evaluations = 0_usize;
    let mut leaves = BinaryHeap::<LeafCandidate>::new();
    let mut emitted = vec![false; groups.len()];
    let mut selected_groups = Vec::new();
    let mut selected_rows = 0_u64;
    let mut selected_code_bytes = 0_u64;
    loop {
        let leaf_is_certified = match (leaves.peek(), nodes.peek()) {
            (Some(leaf), Some(node)) => leaf.score.total_cmp(&node.bound).is_lt(),
            (Some(_), None) => true,
            (None, _) => false,
        };
        if leaf_is_certified {
            let leaf = leaves.pop().expect("certified leaf exists");
            let group_ordinal = generation.leaves()[leaf.ordinal as usize].group_ordinal() as usize;
            if emitted[group_ordinal] {
                continue;
            }
            emitted[group_ordinal] = true;
            let group = V34SelectedGroup {
                storage: groups[group_ordinal],
                score: leaf.score,
            };
            let next_rows = selected_rows.checked_add(group.storage.rows);
            let next_bytes = selected_code_bytes.checked_add(group.storage.code_bytes);
            if selected_groups.len() >= budget.max_groups as usize
                || next_rows.is_none_or(|rows| rows > budget.max_rows)
                || next_bytes.is_none_or(|bytes| bytes > budget.max_code_bytes)
            {
                return Ok(V34RoutePrefix {
                    selected_groups,
                    overflow: Some(group),
                    selected_rows,
                    selected_code_bytes,
                    exact_leaf_evaluations,
                    bound_evaluations,
                });
            }
            selected_rows = next_rows.expect("checked above");
            selected_code_bytes = next_bytes.expect("checked above");
            selected_groups.push(group);
            if selected_groups.len() == groups.len() {
                return Ok(V34RoutePrefix {
                    selected_groups,
                    overflow: None,
                    selected_rows,
                    selected_code_bytes,
                    exact_leaf_evaluations,
                    bound_evaluations,
                });
            }
            continue;
        }
        let Some(candidate) = nodes.pop() else {
            return Ok(V34RoutePrefix {
                selected_groups,
                overflow: None,
                selected_rows,
                selected_code_bytes,
                exact_leaf_evaluations,
                bound_evaluations,
            });
        };
        let node = tree
            .node(candidate.ordinal)
            .ok_or_else(|| invalid("V34 hierarchical node differs"))?;
        if node.is_terminal() {
            for ordinal in tree
                .descendant_leaf_ordinals(candidate.ordinal)
                .ok_or_else(|| invalid("V34 hierarchical leaf extent differs"))?
            {
                exact_leaf_evaluations += 1;
                let group_ordinal = generation.leaves()[*ordinal as usize].group_ordinal();
                leaves.push(LeafCandidate {
                    score: score_v34_rank4_leaf(&generation.leaves()[*ordinal as usize], query)?,
                    group_ordinal,
                    ordinal: *ordinal,
                });
            }
        } else {
            for child in node.children[..node.child_count()].iter().copied() {
                bound_evaluations += 1;
                nodes.push(NodeCandidate {
                    bound: bound_v34_node(tree, child, query)?,
                    ordinal: child,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        V34GroupStorage, V34RouteBudget, bound_v34_node, build_v34_route_tree,
        exhaustive_v34_route, hierarchical_v34_route, min_v34_lower_tail_over_interval,
    };
    use crate::{V34Rank4LeafInput, build_v34_rank4_generation};

    const DIMENSIONS: usize = 96;
    const MIB: u64 = 1_048_576;

    fn leaf(
        leaf_ordinal: u32,
        group_ordinal: u32,
        logical_start: u64,
        mean: f32,
    ) -> V34Rank4LeafInput {
        let mut center = [0.0; DIMENSIONS];
        center[0] = mean;
        V34Rank4LeafInput {
            leaf_ordinal,
            group_ordinal,
            logical_start,
            population: 1,
            mean: center,
            residual_diagonal: [0.0; DIMENSIONS],
            eigenvalues: [0.0; 4],
            directions: [[0.0; DIMENSIONS]; 4],
        }
    }

    fn fixture() -> (crate::V34Rank4Generation, Vec<V34GroupStorage>) {
        let generation = build_v34_rank4_generation(vec![
            leaf(0, 0, 0, 3.0),
            leaf(1, 1, 1, 1.0),
            leaf(2, 0, 2, 0.5),
            leaf(3, 2, 3, 2.0),
        ])
        .unwrap();
        let groups = vec![
            V34GroupStorage::new(0, 2, 100).unwrap(),
            V34GroupStorage::new(1, 1, 200).unwrap(),
            V34GroupStorage::new(2, 1, 300).unwrap(),
        ];
        (generation, groups)
    }

    #[test]
    fn v34_route_exhaustive_uses_one_minimum_per_group_and_stable_ties() {
        // Break caught: duplicate leaves overwrite rather than minimize, or
        // equal scores are ordered by discovery instead of group ordinal.
        let (generation, groups) = fixture();
        let query = [0.0; DIMENSIONS];
        let route = exhaustive_v34_route(
            &generation,
            &query,
            &groups,
            V34RouteBudget::new(3, 4, 600).unwrap(),
        )
        .unwrap();
        assert_eq!(
            route
                .selected_groups()
                .iter()
                .map(|group| (group.group_ordinal(), group.score().to_bits()))
                .collect::<Vec<_>>(),
            vec![
                (0, 0.25_f64.to_bits()),
                (1, 1.0_f64.to_bits()),
                (2, 4.0_f64.to_bits())
            ]
        );
        assert_eq!(route.selected_rows(), 4);
        assert_eq!(route.selected_code_bytes(), 600);
        assert_eq!(route.exact_leaf_evaluations(), 4);
        assert_eq!(route.bound_evaluations(), 0);
        assert!(route.overflow().is_none());

        let tied =
            build_v34_rank4_generation(vec![leaf(0, 1, 0, 1.0), leaf(1, 0, 1, -1.0)]).unwrap();
        let tied_groups = vec![
            V34GroupStorage::new(0, 1, 10).unwrap(),
            V34GroupStorage::new(1, 1, 10).unwrap(),
        ];
        let route = exhaustive_v34_route(
            &tied,
            &query,
            &tied_groups,
            V34RouteBudget::new(2, 2, 20).unwrap(),
        )
        .unwrap();
        assert_eq!(
            route
                .selected_groups()
                .iter()
                .map(|group| group.group_ordinal())
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    #[test]
    fn v34_route_exhaustive_stops_at_first_overflow_without_skipping() {
        // Break caught: a row- or byte-heavy group is skipped so a later group
        // can fit, violating exact complete-prefix authority.
        let (generation, groups) = fixture();
        let query = [0.0; DIMENSIONS];
        for budget in [
            V34RouteBudget::new(1, 4, 600).unwrap(),
            V34RouteBudget::new(3, 2, 600).unwrap(),
            V34RouteBudget::new(3, 4, 250).unwrap(),
            V34RouteBudget::new(2, 2, 250).unwrap(),
        ] {
            let route = exhaustive_v34_route(&generation, &query, &groups, budget).unwrap();
            assert_eq!(route.selected_groups().len(), 1);
            assert_eq!(route.selected_groups()[0].group_ordinal(), 0);
            assert_eq!(route.overflow().unwrap().group_ordinal(), 1);
            assert_eq!(route.selected_rows(), 2);
            assert_eq!(route.selected_code_bytes(), 100);
        }
    }

    #[test]
    fn v34_route_exhaustive_rejects_invalid_authority_and_checked_limits() {
        // Break caught: malformed query/group authority or arithmetic overflow
        // reaches scoring/admission, or serving caps are silently widened.
        let (generation, groups) = fixture();
        assert!(V34RouteBudget::new(0, 1, 1).is_err());
        assert!(V34RouteBudget::new(65, 262_144, 8 * MIB).is_err());
        assert!(V34RouteBudget::new(64, 262_145, 8 * MIB).is_err());
        assert!(V34RouteBudget::new(64, 262_144, 8 * MIB + 1).is_err());

        let mut nonfinite = [0.0; DIMENSIONS];
        nonfinite[7] = f32::NAN;
        assert!(
            exhaustive_v34_route(
                &generation,
                &nonfinite,
                &groups,
                V34RouteBudget::new(3, 4, 600).unwrap(),
            )
            .is_err()
        );
        assert!(
            exhaustive_v34_route(
                &generation,
                &[0.0; DIMENSIONS],
                &groups[..2],
                V34RouteBudget::new(3, 4, 600).unwrap(),
            )
            .is_err()
        );
        let wrong_rows = vec![
            V34GroupStorage::new(0, 1, 100).unwrap(),
            V34GroupStorage::new(1, 1, 200).unwrap(),
            V34GroupStorage::new(2, 1, 300).unwrap(),
        ];
        assert!(
            exhaustive_v34_route(
                &generation,
                &[0.0; DIMENSIONS],
                &wrong_rows,
                V34RouteBudget::new(3, 4, 600).unwrap(),
            )
            .is_err()
        );
    }

    fn tree_fixture(count: u32) -> crate::V34Rank4Generation {
        let mut leaves = Vec::new();
        let mut logical_start = 0_u64;
        for ordinal in 0..count {
            let mut value = leaf(ordinal, ordinal % 4, logical_start, ordinal as f32);
            value.population = 2 + ordinal % 3;
            logical_start += u64::from(value.population);
            value.mean[1] = (ordinal % 3) as f32;
            value.residual_diagonal[0] = 0.25 + ordinal as f32 / 100.0;
            value.eigenvalues[0] = 0.5;
            value.directions[0][1] = 1.0;
            leaves.push(value);
        }
        build_v34_rank4_generation(leaves).unwrap()
    }

    #[test]
    fn v34_route_tree_constructs_deterministic_balanced_sixteen_way_coverage() {
        // Break caught: construction uses unstable partitioning, loses leaves,
        // or creates terminal buckets wider than sixteen leaves.
        let generation = tree_fixture(257);
        let first = build_v34_route_tree(&generation).unwrap();
        let second = build_v34_route_tree(&generation).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.leaf_count(), 257);
        assert_eq!(first.root(), 0);
        assert_eq!(first.node(0).unwrap().child_count(), 16);
        let mut covered = first
            .nodes()
            .iter()
            .enumerate()
            .filter(|(_, node)| node.is_terminal())
            .flat_map(|(ordinal, _)| {
                first
                    .descendant_leaf_ordinals(ordinal as u32)
                    .unwrap()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        covered.sort_unstable();
        assert_eq!(covered, (0..257).collect::<Vec<_>>());
        assert!(
            first
                .nodes()
                .iter()
                .enumerate()
                .filter(|(_, node)| node.is_terminal())
                .all(|(ordinal, _)| {
                    (1..=16).contains(
                        &first
                            .descendant_leaf_ordinals(ordinal as u32)
                            .unwrap()
                            .len(),
                    )
                })
        );

        let singleton = tree_fixture(1);
        let singleton_tree = build_v34_route_tree(&singleton).unwrap();
        assert!(singleton_tree.node(0).unwrap().is_terminal());
        assert_eq!(singleton_tree.descendant_leaf_ordinals(0).unwrap(), &[0]);

        let constant = build_v34_rank4_generation(
            (0..33)
                .map(|ordinal| leaf(ordinal, ordinal % 4, u64::from(ordinal), 1.0))
                .collect(),
        )
        .unwrap();
        assert_eq!(
            build_v34_route_tree(&constant).unwrap(),
            build_v34_route_tree(&constant).unwrap()
        );
    }

    #[test]
    fn v34_route_tree_bound_never_exceeds_any_descendant_exact_score() {
        // Break caught: signed lower-tail terms or rounded enclosing geometry
        // make a node bound unsafe and allow a true next leaf to be pruned.
        let generation = tree_fixture(257);
        let tree = build_v34_route_tree(&generation).unwrap();
        for query_value in [-10.0_f32, 0.0, 20.5, 512.0] {
            let mut query = [0.0; DIMENSIONS];
            query[0] = query_value;
            query[1] = -0.75;
            for (node_ordinal, _) in tree.nodes().iter().enumerate() {
                let bound = bound_v34_node(&tree, node_ordinal as u32, &query).unwrap();
                for leaf_ordinal in tree.descendant_leaf_ordinals(node_ordinal as u32).unwrap() {
                    let exact = crate::score_v34_rank4_leaf(
                        &generation.leaves()[*leaf_ordinal as usize],
                        &query,
                    )
                    .unwrap();
                    assert!(
                        bound <= exact,
                        "node={node_ordinal} leaf={leaf_ordinal} bound={bound} exact={exact}"
                    );
                }
            }
        }
    }

    #[test]
    fn v34_route_tree_bound_includes_interior_minimizer_and_endpoints() {
        // f(D) = D + 1 - 2*sqrt(4 + 4D) has its minimum -4 at D=3.
        let bound = min_v34_lower_tail_over_interval(0.0, 9.0, 1.0, 2.0, 2.0, 1.0);
        assert!(bound <= -4.0);
        assert!((-4.0 - bound) < 1.0e-10, "bound={bound}");

        let monotone = min_v34_lower_tail_over_interval(4.0, 9.0, 1.0, 2.0, 2.0, 0.0);
        let expected = 5.0 - 2.0 * 4.0_f64.sqrt();
        assert!(monotone <= expected);
        assert!((expected - monotone) < 1.0e-10, "bound={monotone}");
    }

    #[test]
    fn v34_route_tree_bound_survives_adversarial_numeric_fixtures() {
        // Break caught: subnormal, large, singular, or rounded-direction input
        // consumes the conservative rounding slack and hides a true minimum.
        let mut inputs = Vec::new();
        let mut logical_start = 0_u64;
        for ordinal in 0..33_u32 {
            let mut input = leaf(ordinal, ordinal % 4, logical_start, 0.0);
            input.population = 1 + ordinal % 3;
            logical_start += u64::from(input.population);
            match ordinal % 4 {
                0 => {}
                1 => {
                    input.mean[0] = f32::from_bits(ordinal + 1);
                    input.residual_diagonal[0] = f32::from_bits(1);
                }
                2 => {
                    input.mean[0] = if ordinal % 8 == 2 { 1.0e18 } else { -1.0e18 };
                    input.residual_diagonal[0] = 1.0e10;
                }
                _ => {
                    input.mean[0] = ordinal as f32 / 7.0;
                    input.residual_diagonal[3] = 0.125;
                    input.eigenvalues[0] = 0.75;
                    input.directions[0][0] = 0.577_350_26;
                    input.directions[0][1] = 0.577_350_26;
                    input.directions[0][2] = 0.577_350_26;
                }
            }
            inputs.push(input);
        }
        let generation = build_v34_rank4_generation(inputs).unwrap();
        let tree = build_v34_route_tree(&generation).unwrap();
        for query_value in [f32::from_bits(1), 0.0, 1.0e18, -1.0e18] {
            let mut query = [0.0; DIMENSIONS];
            query[0] = query_value;
            query[1] = -0.25;
            for (node_ordinal, _) in tree.nodes().iter().enumerate() {
                let bound = bound_v34_node(&tree, node_ordinal as u32, &query).unwrap();
                for leaf_ordinal in tree.descendant_leaf_ordinals(node_ordinal as u32).unwrap() {
                    let exact = crate::score_v34_rank4_leaf(
                        &generation.leaves()[*leaf_ordinal as usize],
                        &query,
                    )
                    .unwrap();
                    assert!(
                        bound <= exact,
                        "query={query_value} node={node_ordinal} leaf={leaf_ordinal} bound={bound} exact={exact}"
                    );
                }
            }
        }
    }

    #[test]
    fn v34_route_differential_matches_every_exact_prefix_field() {
        let generation = tree_fixture(257);
        let tree = build_v34_route_tree(&generation).unwrap();
        let groups = (0..4)
            .map(|group| {
                let rows = generation
                    .leaves()
                    .iter()
                    .filter(|leaf| leaf.group_ordinal() == group)
                    .map(|leaf| u64::from(leaf.population()))
                    .sum();
                V34GroupStorage::new(group, rows, 100 + u64::from(group)).unwrap()
            })
            .collect::<Vec<_>>();
        for value in [-50.0_f32, 0.0, 72.25, 300.0] {
            let mut query = [0.0; DIMENSIONS];
            query[0] = value;
            for budget in [
                V34RouteBudget::new(4, generation.logical_rows(), 1_000).unwrap(),
                V34RouteBudget::new(2, generation.logical_rows(), 1_000).unwrap(),
                V34RouteBudget::new(4, generation.logical_rows() / 2, 1_000).unwrap(),
                V34RouteBudget::new(4, generation.logical_rows(), 100).unwrap(),
            ] {
                let exact = exhaustive_v34_route(&generation, &query, &groups, budget).unwrap();
                let actual =
                    hierarchical_v34_route(&generation, &tree, &query, &groups, budget).unwrap();
                assert!(actual.exact_leaf_evaluations() <= generation.leaves().len());
                assert!(actual.bound_evaluations() > 0);
                assert_eq!(actual.selected_groups(), exact.selected_groups());
                assert_eq!(actual.overflow(), exact.overflow());
                assert_eq!(actual.selected_rows(), exact.selected_rows());
                assert_eq!(actual.selected_code_bytes(), exact.selected_code_bytes());
            }
        }
        let mut other_inputs = generation
            .leaves()
            .iter()
            .map(|leaf| V34Rank4LeafInput {
                leaf_ordinal: leaf.leaf_ordinal(),
                group_ordinal: leaf.group_ordinal(),
                logical_start: leaf.logical_start(),
                population: leaf.population(),
                mean: *leaf.mean(),
                residual_diagonal: *leaf.residual_diagonal(),
                eigenvalues: *leaf.eigenvalues(),
                directions: *leaf.directions(),
            })
            .collect::<Vec<_>>();
        other_inputs[0].mean[0] += 1.0;
        let other = build_v34_rank4_generation(other_inputs).unwrap();
        assert!(
            hierarchical_v34_route(
                &other,
                &tree,
                &[0.0; DIMENSIONS],
                &groups,
                V34RouteBudget::new(4, other.logical_rows(), 1_000).unwrap(),
            )
            .is_err()
        );
    }

    #[test]
    fn v34_route_differential_matches_group_ordinal_ties() {
        // Break caught: the leaf heap uses leaf ordinal to resolve equal scores,
        // while the authoritative exhaustive route uses group ordinal.
        let generation =
            build_v34_rank4_generation(vec![leaf(0, 1, 0, 1.0), leaf(1, 0, 1, -1.0)]).unwrap();
        let tree = build_v34_route_tree(&generation).unwrap();
        let groups = vec![
            V34GroupStorage::new(0, 1, 10).unwrap(),
            V34GroupStorage::new(1, 1, 10).unwrap(),
        ];
        let budget = V34RouteBudget::new(1, 2, 20).unwrap();
        let exact = exhaustive_v34_route(&generation, &[0.0; DIMENSIONS], &groups, budget).unwrap();
        let actual =
            hierarchical_v34_route(&generation, &tree, &[0.0; DIMENSIONS], &groups, budget)
                .unwrap();
        assert_eq!(actual.selected_groups(), exact.selected_groups());
        assert_eq!(actual.overflow(), exact.overflow());
    }

    #[test]
    fn v34_route_differential_stops_when_every_group_is_admitted() {
        // Break caught: a complete one-group route drains the remaining tree
        // and degenerates to an exhaustive leaf scan.
        let generation = build_v34_rank4_generation(
            (0..256)
                .map(|ordinal| leaf(ordinal, 0, u64::from(ordinal), ordinal as f32))
                .collect(),
        )
        .unwrap();
        let tree = build_v34_route_tree(&generation).unwrap();
        let groups = vec![V34GroupStorage::new(0, 256, 1_024).unwrap()];
        let route = hierarchical_v34_route(
            &generation,
            &tree,
            &[0.0; DIMENSIONS],
            &groups,
            V34RouteBudget::new(1, 256, 1_024).unwrap(),
        )
        .unwrap();
        assert_eq!(route.selected_groups().len(), 1);
        assert!(route.overflow().is_none());
        assert!(route.exact_leaf_evaluations() < generation.leaves().len());
    }
}
