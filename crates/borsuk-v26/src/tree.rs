use std::collections::{BTreeSet, HashSet};

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{Result, V26Error, V26LayoutAuthority, invalid, validate_v26_vector};

#[derive(Debug, Clone, PartialEq)]
pub struct V26ConstructionRow {
    pub source_ordinal: u64,
    pub vector: [f32; 96],
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26Node {
    pub node_ordinal: u32,
    pub left: Option<u32>,
    pub right: Option<u32>,
    pub direction_ordinal: u8,
    pub threshold: f32,
    pub split_gap: f32,
    pub leaf_page: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26Tree {
    pub seed: u64,
    pub root: u32,
    pub nodes: Vec<V26Node>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26RowPages {
    pub source_ordinal: u64,
    pub primary_page: u32,
    pub replica_page: u32,
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn direction_sign(seed: u64, node: u32, direction: u8, dimension: usize) -> f32 {
    let key = seed
        ^ u64::from(node).wrapping_mul(0xd6e8_feb8_6659_fd93)
        ^ u64::from(direction).wrapping_mul(0xa5a3_564e_27f8_864d)
        ^ (dimension as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    if splitmix64(key) & 1 == 0 { -1.0 } else { 1.0 }
}

fn make_direction(seed: u64, node: u32, direction: u8) -> [f32; 96] {
    std::array::from_fn(|dimension| direction_sign(seed, node, direction, dimension))
}

fn score(row: &V26ConstructionRow, direction: &[f32; 96]) -> f32 {
    row.vector
        .iter()
        .zip(direction)
        .fold(0.0_f32, |sum, (coordinate, sign)| sum + coordinate * sign)
}

fn score_query(query: &[f32; 96], direction: &[f32; 96]) -> f32 {
    query
        .iter()
        .zip(direction)
        .fold(0.0_f32, |sum, (coordinate, sign)| sum + coordinate * sign)
}

fn validate_router_tree(tree: &V26Tree, expected_seed: u64) -> Result<BTreeSet<u32>> {
    if tree.seed != expected_seed
        || usize::try_from(tree.root)
            .ok()
            .is_none_or(|root| root >= tree.nodes.len())
        || tree.nodes.is_empty()
    {
        return Err(invalid("V26 router tree authority differs"));
    }
    let mut pending = vec![tree.root];
    let mut visited = HashSet::new();
    let mut pages = BTreeSet::new();
    while let Some(node_ordinal) = pending.pop() {
        let index = usize::try_from(node_ordinal)
            .map_err(|_| invalid("V26 router node ordinal overflows"))?;
        let node = tree
            .nodes
            .get(index)
            .ok_or_else(|| invalid("V26 router tree topology differs"))?;
        if node.node_ordinal != node_ordinal
            || !visited.insert(node_ordinal)
            || !node.threshold.is_finite()
            || !node.split_gap.is_finite()
            || node.split_gap < 0.0
        {
            return Err(invalid("V26 router tree topology differs"));
        }
        match (node.left, node.right, node.leaf_page) {
            (Some(left), Some(right), None) if left != right && node.direction_ordinal < 16 => {
                pending.push(right);
                pending.push(left);
            }
            (None, None, Some(page))
                if node.direction_ordinal == 0
                    && node.threshold.to_bits() == 0
                    && node.split_gap.to_bits() == 0 =>
            {
                if !pages.insert(page) {
                    return Err(invalid("V26 router leaf page repeats"));
                }
            }
            _ => return Err(invalid("V26 router node shape differs")),
        }
    }
    if visited.len() != tree.nodes.len() || pages.is_empty() {
        return Err(invalid("V26 router tree inventory differs"));
    }
    Ok(pages)
}

#[derive(Clone, Copy)]
struct RouterFrontier {
    margin: f32,
    tree_ordinal: u8,
    node_ordinal: u32,
}

fn descend_router_branch(
    tree: &V26Tree,
    tree_ordinal: u8,
    mut node_ordinal: u32,
    inherited_margin: f32,
    query: &[f32; 96],
    frontier: &mut Vec<RouterFrontier>,
) -> Result<u32> {
    loop {
        let node = &tree.nodes[usize::try_from(node_ordinal)
            .map_err(|_| invalid("V26 router node ordinal overflows"))?];
        if let Some(page) = node.leaf_page {
            return Ok(page);
        }
        let direction = make_direction(tree.seed, node.node_ordinal, node.direction_ordinal);
        let query_score = score_query(query, &direction);
        let margin = (query_score - node.threshold).abs();
        if !query_score.is_finite() || !margin.is_finite() {
            return Err(invalid("V26 router projection differs"));
        }
        let (near, far) = if query_score <= node.threshold {
            (node.left.unwrap(), node.right.unwrap())
        } else {
            (node.right.unwrap(), node.left.unwrap())
        };
        frontier.push(RouterFrontier {
            margin: inherited_margin.max(margin),
            tree_ordinal,
            node_ordinal: far,
        });
        node_ordinal = near;
    }
}

fn rank_v26_tree_pages_to_limit(
    primary: &V26Tree,
    replica: &V26Tree,
    query: &[f32; 96],
    limit: Option<usize>,
) -> Result<Vec<u32>> {
    validate_v26_vector(query)?;
    let primary_pages = validate_router_tree(primary, 0x5632_362d_5452_4545)?;
    let replica_pages = validate_router_tree(replica, 0x5632_362d_5245_504c)?;
    if !primary_pages.is_disjoint(&replica_pages) {
        return Err(invalid("V26 router page inventory differs"));
    }
    let page_count = primary_pages
        .len()
        .checked_add(replica_pages.len())
        .ok_or_else(|| invalid("V26 router page inventory overflows"))?;
    let target = limit.unwrap_or(page_count);
    if target == 0 || target > page_count {
        return Err(invalid("V26 router page inventory differs"));
    }
    let mut frontier = vec![
        RouterFrontier {
            margin: 0.0,
            tree_ordinal: 0,
            node_ordinal: primary.root,
        },
        RouterFrontier {
            margin: 0.0,
            tree_ordinal: 1,
            node_ordinal: replica.root,
        },
    ];
    let mut ranked = Vec::with_capacity(page_count);
    while ranked.len() < target {
        let next = frontier
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| {
                left.margin
                    .total_cmp(&right.margin)
                    .then_with(|| left.tree_ordinal.cmp(&right.tree_ordinal))
                    .then_with(|| left.node_ordinal.cmp(&right.node_ordinal))
            })
            .map(|(index, _)| index)
            .ok_or_else(|| invalid("V26 router frontier exhausted"))?;
        let branch = frontier.swap_remove(next);
        let tree = if branch.tree_ordinal == 0 {
            primary
        } else {
            replica
        };
        let page = descend_router_branch(
            tree,
            branch.tree_ordinal,
            branch.node_ordinal,
            branch.margin,
            query,
            &mut frontier,
        )?;
        ranked.push(page);
    }
    if ranked.len() != target || ranked.iter().copied().collect::<BTreeSet<_>>().len() != target {
        return Err(invalid("V26 router ranked page inventory differs"));
    }
    Ok(ranked)
}

pub fn rank_v26_tree_pages(
    primary: &V26Tree,
    replica: &V26Tree,
    query: &[f32; 96],
) -> Result<Vec<u32>> {
    rank_v26_tree_pages_to_limit(primary, replica, query, None)
}

pub(crate) fn rank_v26_tree_page_prefix(
    primary: &V26Tree,
    replica: &V26Tree,
    query: &[f32; 96],
    candidate_page_limit: usize,
) -> Result<Vec<u32>> {
    rank_v26_tree_pages_to_limit(primary, replica, query, Some(candidate_page_limit))
}

pub fn route_v26_pages(
    primary: &V26Tree,
    replica: &V26Tree,
    query: &[f32; 96],
    page_budget: usize,
) -> Result<Vec<u32>> {
    if page_budget != 8 {
        return Err(invalid("V26 router page budget differs"));
    }
    let mut selected = rank_v26_tree_pages_to_limit(primary, replica, query, Some(page_budget))?;
    selected.sort_unstable();
    Ok(selected)
}

fn compare_ranked(left: &RankedRow, right: &RankedRow) -> std::cmp::Ordering {
    left.0
        .total_cmp(&right.0)
        .then_with(|| left.1.cmp(&right.1))
}

struct TreeBuilder<'a> {
    seed: u64,
    page_offset: u32,
    next_page: u32,
    rows: &'a [V26ConstructionRow],
    pool: Option<&'a rayon::ThreadPool>,
    nodes: Vec<V26Node>,
    row_pages: Vec<u32>,
}

type RankedRow = (f32, u64, usize);
type SelectedDirection = (f32, u8, Vec<RankedRow>);

fn preferred_direction(left: SelectedDirection, right: SelectedDirection) -> SelectedDirection {
    if right.0.total_cmp(&left.0).is_gt()
        || (right.0.total_cmp(&left.0).is_eq() && right.1 < left.1)
    {
        right
    } else {
        left
    }
}

impl TreeBuilder<'_> {
    fn build_node(&mut self, row_indexes: Vec<usize>, leaves: u32, capacity: u32) -> Result<u32> {
        let node_ordinal = u32::try_from(self.nodes.len())
            .map_err(|_| V26Error("V26 node count overflows".to_owned()))?;
        self.nodes.push(V26Node {
            node_ordinal,
            left: None,
            right: None,
            direction_ordinal: 0,
            threshold: 0.0,
            split_gap: 0.0,
            leaf_page: None,
        });
        if leaves == 1 {
            let page = self
                .page_offset
                .checked_add(self.next_page)
                .ok_or_else(|| V26Error("V26 page count overflows".to_owned()))?;
            self.next_page += 1;
            for row_index in row_indexes {
                self.row_pages[row_index] = page;
            }
            self.nodes[node_ordinal as usize].leaf_page = Some(page);
            return Ok(node_ordinal);
        }

        let left_leaves = leaves / 2;
        let right_leaves = leaves - left_leaves;
        let left_rows = (row_indexes.len() - right_leaves as usize)
            .min(left_leaves as usize * capacity as usize);
        let evaluate = |direction: u8| -> Result<SelectedDirection> {
            let plane = make_direction(self.seed, node_ordinal, direction);
            let mut ranked = row_indexes
                .iter()
                .map(|index| {
                    let value = score(&self.rows[*index], &plane);
                    (value, self.rows[*index].source_ordinal, *index)
                })
                .collect::<Vec<_>>();
            if ranked.iter().any(|(value, _, _)| !value.is_finite()) {
                return Err(V26Error("V26 projection score is not finite".to_owned()));
            }
            ranked.select_nth_unstable_by(left_rows - 1, compare_ranked);
            let left_max = ranked[left_rows - 1];
            let right_min = *ranked[left_rows..]
                .iter()
                .min_by(|left, right| compare_ranked(left, right))
                .ok_or_else(|| V26Error("V26 split rank differs".to_owned()))?;
            let gap = right_min.0 - left_max.0;
            if !gap.is_finite() || gap < 0.0 {
                return Err(V26Error("V26 split gap is not finite".to_owned()));
            }
            Ok((gap, direction, ranked))
        };
        let selected = if let Some(pool) = self.pool {
            pool.install(|| {
                (0_u8..16)
                    .into_par_iter()
                    .map(evaluate)
                    .try_reduce_with(|left, right| Ok(preferred_direction(left, right)))
            })
            .ok_or_else(|| V26Error("V26 split direction is absent".to_owned()))??
        } else {
            let mut selected = None;
            for direction in 0_u8..16 {
                let candidate = evaluate(direction)?;
                selected = Some(match selected {
                    Some(current) => preferred_direction(current, candidate),
                    None => candidate,
                });
            }
            selected.ok_or_else(|| V26Error("V26 split direction is absent".to_owned()))?
        };
        let (split_gap, direction, mut ranked) = selected;
        let threshold = ranked[left_rows - 1].0;
        let right = ranked.split_off(left_rows);
        let left_indexes = ranked.into_iter().map(|(_, _, index)| index).collect();
        let right_indexes = right.into_iter().map(|(_, _, index)| index).collect();
        let left = self.build_node(left_indexes, left_leaves, capacity)?;
        let right = self.build_node(right_indexes, right_leaves, capacity)?;
        self.nodes[node_ordinal as usize] = V26Node {
            node_ordinal,
            left: Some(left),
            right: Some(right),
            direction_ordinal: direction,
            threshold,
            split_gap,
            leaf_page: None,
        };
        Ok(node_ordinal)
    }
}

fn build_tree(
    seed: u64,
    page_offset: u32,
    leaves: u32,
    capacity: u32,
    rows: &[V26ConstructionRow],
    pool: Option<&rayon::ThreadPool>,
) -> Result<(V26Tree, Vec<u32>)> {
    let mut builder = TreeBuilder {
        seed,
        page_offset,
        next_page: 0,
        rows,
        pool,
        nodes: Vec::new(),
        row_pages: vec![u32::MAX; rows.len()],
    };
    let root = builder.build_node((0..rows.len()).collect(), leaves, capacity)?;
    Ok((
        V26Tree {
            seed,
            root,
            nodes: builder.nodes,
        },
        builder.row_pages,
    ))
}

pub fn build_v26_dual_tree_layout(
    authority: &V26LayoutAuthority,
    rows: &[V26ConstructionRow],
) -> Result<(V26Tree, V26Tree, Vec<V26RowPages>)> {
    build_v26_dual_tree_layout_inner(authority, rows, None)
}

pub(crate) fn build_v26_dual_tree_layout_with_workers(
    authority: &V26LayoutAuthority,
    rows: &[V26ConstructionRow],
    worker_count: usize,
) -> Result<(V26Tree, V26Tree, Vec<V26RowPages>)> {
    if worker_count == 0 {
        return Err(invalid("V26 worker count differs"));
    }
    if worker_count == 1 {
        return build_v26_dual_tree_layout_inner(authority, rows, None);
    }
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(worker_count)
        .build()
        .map_err(|error| V26Error(format!("V26 worker pool failed: {error}")))?;
    build_v26_dual_tree_layout_inner(authority, rows, Some(&pool))
}

fn build_v26_dual_tree_layout_inner(
    authority: &V26LayoutAuthority,
    rows: &[V26ConstructionRow],
    pool: Option<&rayon::ThreadPool>,
) -> Result<(V26Tree, V26Tree, Vec<V26RowPages>)> {
    if authority.schema != crate::V26_LAYOUT_SCHEMA
        || authority.primary_seed != 0x5632_362d_5452_4545
        || authority.replica_seed != 0x5632_362d_5245_504c
        || !crate::V26_PAGE_CAPACITY_LADDER.contains(&authority.page_capacity)
        || authority.expected_rows != rows.len() as u64
        || rows.is_empty()
    {
        return Err(invalid("V26 tree authority differs"));
    }
    let mut prior = None;
    for row in rows {
        if row.vector.iter().any(|coordinate| !coordinate.is_finite())
            || prior.is_some_and(|ordinal| row.source_ordinal <= ordinal)
        {
            return Err(invalid("V26 construction row authority differs"));
        }
        prior = Some(row.source_ordinal);
    }
    let leaves_u64 = (rows.len() as u64).div_ceil(u64::from(authority.page_capacity));
    let leaves = u32::try_from(leaves_u64).map_err(|_| invalid("V26 tree page count overflows"))?;
    let (primary, primary_pages) = build_tree(
        authority.primary_seed,
        0,
        leaves,
        authority.page_capacity,
        rows,
        pool,
    )?;
    let (replica, replica_pages) = build_tree(
        authority.replica_seed,
        leaves,
        leaves,
        authority.page_capacity,
        rows,
        pool,
    )?;
    let assignments = rows
        .iter()
        .enumerate()
        .map(|(index, row)| V26RowPages {
            source_ordinal: row.source_ordinal,
            primary_page: primary_pages[index],
            replica_page: replica_pages[index],
        })
        .collect::<Vec<_>>();
    validate_v26_dual_tree_layout(authority, &primary, &replica, &assignments)?;
    Ok((primary, replica, assignments))
}

fn validate_tree(tree: &V26Tree, seed: u64, first_page: u32, leaves: u32) -> Result<()> {
    let expected_nodes = leaves
        .checked_mul(2)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| invalid("V26 tree node count overflows"))?;
    if tree.seed != seed || tree.root != 0 || tree.nodes.len() != expected_nodes as usize {
        return Err(invalid("V26 tree topology differs"));
    }
    let end_page = first_page
        .checked_add(leaves)
        .ok_or_else(|| invalid("V26 page count overflows"))?;
    let mut referenced = vec![false; tree.nodes.len()];
    referenced[0] = true;
    let mut pages = BTreeSet::new();
    for (index, node) in tree.nodes.iter().enumerate() {
        if node.node_ordinal as usize != index
            || !node.threshold.is_finite()
            || !node.split_gap.is_finite()
            || node.split_gap < 0.0
        {
            return Err(invalid("V26 tree node authority differs"));
        }
        match (node.left, node.right, node.leaf_page) {
            (None, None, Some(page)) => {
                if page < first_page
                    || page >= end_page
                    || !pages.insert(page)
                    || node.direction_ordinal != 0
                    || node.threshold != 0.0
                    || node.split_gap != 0.0
                {
                    return Err(invalid("V26 tree leaf authority differs"));
                }
            }
            (Some(left), Some(right), None) => {
                if node.direction_ordinal >= 16
                    || left <= node.node_ordinal
                    || right <= node.node_ordinal
                    || left as usize >= tree.nodes.len()
                    || right as usize >= tree.nodes.len()
                    || referenced[left as usize]
                    || referenced[right as usize]
                {
                    return Err(invalid("V26 tree edge authority differs"));
                }
                referenced[left as usize] = true;
                referenced[right as usize] = true;
            }
            _ => return Err(invalid("V26 tree node shape differs")),
        }
    }
    if referenced.iter().any(|value| !value) || pages.len() != leaves as usize {
        return Err(invalid("V26 tree inventory differs"));
    }
    Ok(())
}

pub fn validate_v26_dual_tree_layout(
    authority: &V26LayoutAuthority,
    primary: &V26Tree,
    replica: &V26Tree,
    assignments: &[V26RowPages],
) -> Result<()> {
    let leaves_u64 = authority
        .expected_rows
        .div_ceil(u64::from(authority.page_capacity));
    let leaves = u32::try_from(leaves_u64).map_err(|_| invalid("V26 page count overflows"))?;
    let page_count = leaves
        .checked_mul(2)
        .ok_or_else(|| invalid("V26 page count overflows"))?;
    validate_tree(primary, authority.primary_seed, 0, leaves)?;
    validate_tree(replica, authority.replica_seed, leaves, leaves)?;
    if assignments.len() as u64 != authority.expected_rows {
        return Err(invalid("V26 assignment count differs"));
    }
    let mut prior = None;
    let mut counts = vec![0_u32; page_count as usize];
    for assignment in assignments {
        if prior.is_some_and(|ordinal| assignment.source_ordinal <= ordinal)
            || assignment.primary_page >= leaves
            || assignment.replica_page < leaves
            || assignment.replica_page >= page_count
            || assignment.primary_page == assignment.replica_page
        {
            return Err(invalid("V26 assignment authority differs"));
        }
        prior = Some(assignment.source_ordinal);
        for page in [assignment.primary_page, assignment.replica_page] {
            counts[page as usize] = counts[page as usize]
                .checked_add(1)
                .ok_or_else(|| invalid("V26 page occupancy overflows"))?;
        }
    }
    if counts
        .iter()
        .any(|count| *count == 0 || *count > authority.page_capacity)
    {
        return Err(invalid("V26 page capacity differs"));
    }
    Ok(())
}
