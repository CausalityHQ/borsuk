use std::{cmp::Ordering, collections::BinaryHeap};

use crate::{BorsukError, Result, V35ArtifactIdentity, V35Dimensions, V35RoutingGeneration};
use sha2::{Digest, Sha256};

const MAX_GROUPS: u32 = 64;
const MAX_ROWS: u64 = 262_144;
const MAX_CODE_BYTES: u64 = 8 * 1_048_576;
const FANOUT: usize = 16;
const TREE_FORMAT: &str = "borsuk-v35-route-tree-v1";

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Immutable directory-root, snapshot, and code-schema binding for remote routing.
pub struct V35RemoteDirectoryBinding {
    pub(crate) directory_root_digest: [u8; 32],
    pub(crate) snapshot_digest: [u8; 32],
    pub(crate) code_schema_digest: [u8; 32],
}

impl V35RemoteDirectoryBinding {
    /// Construct a complete nonzero remote-directory binding.
    pub fn new(
        directory_root_digest: [u8; 32],
        snapshot_digest: [u8; 32],
        code_schema_digest: [u8; 32],
    ) -> Result<Self> {
        if [directory_root_digest, snapshot_digest, code_schema_digest].contains(&[0; 32]) {
            return Err(invalid("V35 remote directory binding differs"));
        }
        Ok(Self {
            directory_root_digest,
            snapshot_digest,
            code_schema_digest,
        })
    }
}

pub(crate) fn v35_artifact_authority_digest(identity: &V35ArtifactIdentity) -> Result<[u8; 32]> {
    if identity.role != "code-directory-block"
        || identity.digest_algorithm != "sha256"
        || identity.digest.len() != 64
        || !identity
            .digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || identity.length == 0
        || !identity.uri.starts_with("s3://")
    {
        return Err(invalid("V35 code-directory block authority differs"));
    }
    let mut hasher = Sha256::new();
    for value in [
        identity.role.as_bytes(),
        identity.digest_algorithm.as_bytes(),
        identity.digest.as_bytes(),
        identity.uri.as_bytes(),
    ] {
        hasher.update((value.len() as u64).to_le_bytes());
        hasher.update(value);
    }
    hasher.update(identity.length.to_le_bytes());
    Ok(hasher.finalize().into())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Immutable row and byte authority for one dense remote-code group.
pub struct V35GroupStorage {
    group_ordinal: u32,
    rows: u64,
    code_bytes: u64,
    remote_binding: Option<V35RemoteDirectoryBinding>,
    pub(crate) directory_block_authority_digest: [u8; 32],
}

impl V35GroupStorage {
    /// Construct one nonempty dense group descriptor.
    pub fn new(group_ordinal: u32, rows: u64, code_bytes: u64) -> Result<Self> {
        if rows == 0 || code_bytes == 0 {
            return Err(invalid("V35 group storage authority differs"));
        }
        Ok(Self {
            group_ordinal,
            rows,
            code_bytes,
            remote_binding: None,
            directory_block_authority_digest: [0; 32],
        })
    }

    /// Construct group storage derived from one authenticated directory-root entry.
    pub fn new_bound(
        group_ordinal: u32,
        rows: u64,
        code_bytes: u64,
        remote_binding: V35RemoteDirectoryBinding,
        directory_block: &V35ArtifactIdentity,
    ) -> Result<Self> {
        if rows == 0 || code_bytes == 0 {
            return Err(invalid("V35 group storage authority differs"));
        }
        Ok(Self {
            group_ordinal,
            rows,
            code_bytes,
            remote_binding: Some(remote_binding),
            directory_block_authority_digest: v35_artifact_authority_digest(directory_block)?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Exact complete-prefix serving limits.
pub struct V35RouteBudget {
    max_groups: u32,
    max_rows: u64,
    max_code_bytes: u64,
}

impl V35RouteBudget {
    /// Construct limits within the frozen V35 serving envelope.
    pub fn new(max_groups: u32, max_rows: u64, max_code_bytes: u64) -> Result<Self> {
        if max_groups == 0
            || max_groups > MAX_GROUPS
            || max_rows == 0
            || max_rows > MAX_ROWS
            || max_code_bytes == 0
            || max_code_bytes > MAX_CODE_BYTES
        {
            return Err(invalid("V35 route budget differs"));
        }
        Ok(Self {
            max_groups,
            max_rows,
            max_code_bytes,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
/// One group ordered by its exact minimum leaf score.
pub struct V35SelectedGroup {
    storage: V35GroupStorage,
    score: f64,
    leaf_ordinal: u32,
}

impl V35SelectedGroup {
    /// Dense group ordinal.
    pub fn group_ordinal(self) -> u32 {
        self.storage.group_ordinal
    }
    /// Exact minimum decoded-patch score.
    pub fn score(self) -> f64 {
        self.score
    }
    /// Leaf supplying the exact group minimum.
    pub fn leaf_ordinal(self) -> u32 {
        self.leaf_ordinal
    }
    /// Authenticated logical rows in the complete selected group.
    pub fn rows(self) -> u64 {
        self.storage.rows
    }
    /// Authenticated encoded code bytes in the complete selected group.
    pub fn code_bytes(self) -> u64 {
        self.storage.code_bytes
    }
    pub(crate) fn directory_block_authority_digest(self) -> [u8; 32] {
        self.storage.directory_block_authority_digest
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Exact admitted group prefix plus its first overflow.
pub struct V35RoutePrefix {
    selected_groups: Vec<V35SelectedGroup>,
    overflow: Option<V35SelectedGroup>,
    selected_rows: u64,
    selected_code_bytes: u64,
    exact_leaf_evaluations: usize,
    bound_evaluations: usize,
    generation_digest: [u8; 32],
    query_digest: [u8; 32],
    remote_binding: Option<V35RemoteDirectoryBinding>,
}

impl V35RoutePrefix {
    /// Complete groups admitted in exact order.
    pub fn selected_groups(&self) -> &[V35SelectedGroup] {
        &self.selected_groups
    }
    /// First complete group that exceeds a serving limit.
    pub fn overflow(&self) -> Option<&V35SelectedGroup> {
        self.overflow.as_ref()
    }
    /// Checked admitted logical-row total.
    pub fn selected_rows(&self) -> u64 {
        self.selected_rows
    }
    /// Checked admitted encoded code-object bytes.
    pub fn selected_code_bytes(&self) -> u64 {
        self.selected_code_bytes
    }
    /// Number of exact leaf scores evaluated.
    pub fn exact_leaf_evaluations(&self) -> usize {
        self.exact_leaf_evaluations
    }
    /// Number of conservative tree bounds evaluated.
    pub fn bound_evaluations(&self) -> usize {
        self.bound_evaluations
    }
    pub(crate) fn generation_digest(&self) -> [u8; 32] {
        self.generation_digest
    }
    pub(crate) fn query_digest(&self) -> [u8; 32] {
        self.query_digest
    }
    pub(crate) fn remote_binding(&self) -> Option<V35RemoteDirectoryBinding> {
        self.remote_binding
    }
}

fn route_query_digest(query: &[f64], omitted: f64) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update((query.len() as u64).to_le_bytes());
    for value in query {
        hasher.update(value.to_bits().to_le_bytes());
    }
    hasher.update(omitted.to_bits().to_le_bytes());
    hasher.finalize().into()
}

fn validate_query(generation: &V35RoutingGeneration, query: &[f64], omitted: f64) -> Result<()> {
    if query.len() != usize::from(generation.dimensions().routing)
        || query.iter().any(|value| !value.is_finite())
        || !omitted.is_finite()
        || omitted < 0.0
    {
        return Err(invalid("V35 route query differs"));
    }
    Ok(())
}

fn validate_groups(generation: &V35RoutingGeneration, groups: &[V35GroupStorage]) -> Result<()> {
    let rows = generation.route_group_rows();
    if rows.len() != groups.len() {
        return Err(invalid("V35 route group storage differs"));
    }
    if groups
        .iter()
        .zip(rows)
        .enumerate()
        .any(|(ordinal, (group, rows))| {
            group.group_ordinal != u32::try_from(ordinal).unwrap_or(u32::MAX) || group.rows != *rows
        })
    {
        return Err(invalid("V35 route group storage differs"));
    }
    let binding = groups.first().and_then(|group| group.remote_binding);
    if groups.iter().any(|group| {
        group.remote_binding != binding
            || group.remote_binding.is_some() != (group.directory_block_authority_digest != [0; 32])
    }) {
        return Err(invalid("V35 route remote binding differs"));
    }
    Ok(())
}

fn exact_group_order(
    generation: &V35RoutingGeneration,
    query: &[f64],
    omitted: f64,
    groups: &[V35GroupStorage],
) -> Result<Vec<V35SelectedGroup>> {
    let mut minima = vec![(f64::INFINITY, u32::MAX); groups.len()];
    for leaf in 0..generation.leaf_count() {
        let score = generation.route_score_leaf(leaf, query, omitted)?;
        let group = usize::try_from(generation.route_group_ordinal(leaf)?)
            .map_err(|_| invalid("V35 route group ordinal overflows"))?;
        let leaf = u32::try_from(leaf).map_err(|_| invalid("V35 route leaf ordinal overflows"))?;
        if score.total_cmp(&minima[group].0).is_lt()
            || (score.to_bits() == minima[group].0.to_bits() && leaf < minima[group].1)
        {
            minima[group] = (score, leaf);
        }
    }
    let mut ordered = groups
        .iter()
        .copied()
        .zip(minima)
        .map(|(storage, (score, leaf_ordinal))| V35SelectedGroup {
            storage,
            score,
            leaf_ordinal,
        })
        .collect::<Vec<_>>();
    if ordered.iter().any(|group| !group.score.is_finite()) {
        return Err(invalid("V35 route group minimum differs"));
    }
    ordered.sort_by(|left, right| {
        left.score
            .total_cmp(&right.score)
            .then_with(|| left.storage.group_ordinal.cmp(&right.storage.group_ordinal))
            .then_with(|| left.leaf_ordinal.cmp(&right.leaf_ordinal))
    });
    Ok(ordered)
}

fn admit_prefix(
    ordered: Vec<V35SelectedGroup>,
    budget: V35RouteBudget,
    exact_leaf_evaluations: usize,
    bound_evaluations: usize,
    generation_digest: [u8; 32],
    query_digest: [u8; 32],
    remote_binding: Option<V35RemoteDirectoryBinding>,
) -> Result<V35RoutePrefix> {
    let mut selected_groups = Vec::new();
    let mut selected_rows = 0_u64;
    let mut selected_code_bytes = 0_u64;
    let mut overflow = None;
    for group in ordered {
        let next_rows = selected_rows.checked_add(group.storage.rows);
        let next_bytes = selected_code_bytes.checked_add(group.storage.code_bytes);
        if selected_groups.len() >= budget.max_groups as usize
            || next_rows.is_none_or(|rows| rows > budget.max_rows)
            || next_bytes.is_none_or(|bytes| bytes > budget.max_code_bytes)
        {
            overflow = Some(group);
            break;
        }
        selected_rows = next_rows.ok_or_else(|| invalid("V35 route rows overflow"))?;
        selected_code_bytes = next_bytes.ok_or_else(|| invalid("V35 route bytes overflow"))?;
        selected_groups.push(group);
    }
    Ok(V35RoutePrefix {
        selected_groups,
        overflow,
        selected_rows,
        selected_code_bytes,
        exact_leaf_evaluations,
        bound_evaluations,
        generation_digest,
        query_digest,
        remote_binding,
    })
}

/// Score every leaf and admit the exact complete group prefix.
pub fn exhaustive_v35_route(
    generation: &V35RoutingGeneration,
    query: &[f64],
    omitted: f64,
    groups: &[V35GroupStorage],
    budget: V35RouteBudget,
) -> Result<V35RoutePrefix> {
    validate_query(generation, query, omitted)?;
    validate_groups(generation, groups)?;
    admit_prefix(
        exact_group_order(generation, query, omitted, groups)?,
        budget,
        generation.leaf_count(),
        0,
        generation.route_authority_digest(),
        route_query_digest(query, omitted),
        groups.first().and_then(|group| group.remote_binding),
    )
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

fn next_up_f32(value: f32) -> f32 {
    if value == f32::INFINITY {
        value
    } else if value == 0.0 {
        f32::from_bits(1)
    } else if value > 0.0 {
        f32::from_bits(value.to_bits() + 1)
    } else {
        f32::from_bits(value.to_bits() - 1)
    }
}

fn next_down_f32(value: f32) -> f32 {
    if value == f32::NEG_INFINITY {
        value
    } else if value == 0.0 {
        -f32::from_bits(1)
    } else if value > 0.0 {
        f32::from_bits(value.to_bits() - 1)
    } else {
        f32::from_bits(value.to_bits() + 1)
    }
}

fn squared_distance_interval(left: &[f64], right: &[f64]) -> (f64, f64) {
    let mut lower = 0.0_f64;
    let mut upper = 0.0_f64;
    for (left, right) in left.iter().zip(right) {
        let delta = *left - *right;
        let square = delta * delta;
        lower = next_down(lower + next_down(square)).max(0.0);
        upper = next_up(upper + next_up(square));
    }
    (lower, upper)
}

fn min_lower_tail(
    dimensions: usize,
    distance_min: f64,
    distance_max: f64,
    trace_min: f64,
    trace_square_max: f64,
    population_factor_max: f64,
    spectral_bound_max: f64,
) -> f64 {
    let evaluate = |lower: f64, upper: f64| {
        let positive = next_down(lower + trace_min);
        let radicand = next_up(
            next_up(2.0 * trace_square_max) + next_up(next_up(4.0 * spectral_bound_max) * upper),
        )
        .max(0.0);
        next_down(positive - next_up(population_factor_max * next_up(radicand.sqrt())))
    };
    let mut minimum = evaluate(next_down(distance_min).max(0.0), next_up(distance_min)).min(
        evaluate(next_down(distance_max).max(0.0), next_up(distance_max)),
    );
    if spectral_bound_max > 0.0 {
        let factor_low = next_down(population_factor_max * population_factor_max);
        let factor_high = next_up(population_factor_max * population_factor_max);
        let denominator_low = next_down(2.0 * spectral_bound_max);
        let denominator_high = next_up(2.0 * spectral_bound_max);
        let stationary_low = next_down(
            next_down(factor_low * spectral_bound_max)
                - next_up(trace_square_max / denominator_low),
        )
        .clamp(distance_min, distance_max);
        let stationary_high = next_up(
            next_up(factor_high * spectral_bound_max)
                - next_down(trace_square_max / denominator_high),
        )
        .clamp(distance_min, distance_max);
        minimum = minimum.min(evaluate(stationary_low, stationary_high));
    }
    let radicand_max = next_up(
        next_up(2.0 * trace_square_max) + next_up(next_up(4.0 * spectral_bound_max) * distance_max),
    )
    .max(0.0);
    let trace_magnitude = next_up((dimensions as f64 * trace_square_max).sqrt());
    let scale = next_up(
        1.0 + distance_max
            + trace_magnitude
            + next_up(population_factor_max * next_up(radicand_max.sqrt())),
    );
    next_down(minimum - 4096.0 * f64::EPSILON * scale)
}

fn quantize_center(values: &[f64]) -> Result<(f32, Vec<i8>, Vec<f64>)> {
    let maximum = values
        .iter()
        .fold(0.0_f64, |maximum, value| maximum.max(value.abs()));
    if !maximum.is_finite() {
        return Err(invalid("V35 route node center differs"));
    }
    let scale = (maximum / 127.0) as f32;
    if maximum == 0.0 || scale == 0.0 {
        return Ok((0.0, vec![0; values.len()], vec![0.0; values.len()]));
    }
    let codes = values
        .iter()
        .map(|value| {
            (value / f64::from(scale))
                .round_ties_even()
                .clamp(-127.0, 127.0) as i8
        })
        .collect::<Vec<_>>();
    let decoded = codes
        .iter()
        .map(|code| f64::from(*code) * f64::from(scale))
        .collect();
    Ok((scale, codes, decoded))
}

#[derive(Debug, Clone, PartialEq)]
/// One compact quantized node in the deterministic sixteen-way tree.
pub struct V35TreeNode {
    center_codes: Vec<i8>,
    center_scale: f32,
    radius: f64,
    trace_min: f64,
    trace_square_max: f64,
    population_factor_max: f64,
    spectral_bound_max: f64,
    omitted_energy_min: f32,
    omitted_energy_max: f32,
    children: [u32; FANOUT],
    child_count: u8,
    leaf_start: u32,
    leaf_count: u32,
}

impl V35TreeNode {
    /// Whether the node directly covers at most sixteen leaves.
    pub fn is_terminal(&self) -> bool {
        self.child_count == 0
    }
    /// Exact fixed numeric envelope for this routing width.
    pub fn encoded_numeric_bytes(&self) -> usize {
        self.center_codes.len() + 128
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Deterministic tree and nonduplicated leaf permutation.
pub struct V35RouteTree {
    format: &'static str,
    authority_digest: [u8; 32],
    dimensions: V35Dimensions,
    nodes: Vec<V35TreeNode>,
    leaf_order: Vec<u32>,
    leaf_count: usize,
    patches_per_leaf: u8,
    projection_checksum: [u8; 32],
    route_authority_digest: [u8; 32],
    source_archive_sha256: String,
}

impl V35RouteTree {
    /// Exact incompatible V35 tree format.
    pub fn format(&self) -> &str {
        self.format
    }

    /// Logical SHA-256 over every node, edge, permutation, and generation binding.
    pub fn authority_digest(&self) -> [u8; 32] {
        self.authority_digest
    }

    /// Nodes in deterministic preorder.
    pub fn nodes(&self) -> &[V35TreeNode] {
        &self.nodes
    }
    /// Root node ordinal.
    pub fn root(&self) -> u32 {
        0
    }
    /// Logical leaves covered exactly once.
    pub fn leaf_count(&self) -> usize {
        self.leaf_count
    }
    /// Resolve the contiguous descendant-leaf permutation.
    pub fn descendant_leaf_ordinals(&self, ordinal: u32) -> Option<&[u32]> {
        let node = self.nodes.get(ordinal as usize)?;
        let start = node.leaf_start as usize;
        self.leaf_order
            .get(start..start.checked_add(node.leaf_count as usize)?)
    }
}

fn tree_authority_digest(tree: &V35RouteTree) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(TREE_FORMAT.as_bytes());
    hasher.update([0]);
    hasher.update(tree.dimensions.source.to_le_bytes());
    hasher.update(tree.dimensions.routing.to_le_bytes());
    hasher.update(tree.projection_checksum);
    hasher.update(tree.route_authority_digest);
    hasher.update(tree.source_archive_sha256.as_bytes());
    hasher.update((tree.leaf_count as u64).to_le_bytes());
    hasher.update([tree.patches_per_leaf]);
    for node in &tree.nodes {
        hasher.update(
            node.center_codes
                .iter()
                .map(|value| *value as u8)
                .collect::<Vec<_>>(),
        );
        hasher.update(node.center_scale.to_bits().to_le_bytes());
        for value in [
            node.radius,
            node.trace_min,
            node.trace_square_max,
            node.population_factor_max,
            node.spectral_bound_max,
        ] {
            hasher.update(value.to_bits().to_le_bytes());
        }
        hasher.update(node.omitted_energy_min.to_bits().to_le_bytes());
        hasher.update(node.omitted_energy_max.to_bits().to_le_bytes());
        for child in node.children {
            hasher.update(child.to_le_bytes());
        }
        hasher.update([node.child_count]);
        hasher.update(node.leaf_start.to_le_bytes());
        hasher.update(node.leaf_count.to_le_bytes());
    }
    for leaf in &tree.leaf_order {
        hasher.update(leaf.to_le_bytes());
    }
    hasher.finalize().into()
}

/// Project the fixed tree bytes for an authenticated node count and width.
pub fn project_v35_route_tree_bytes(node_count: u64, routing: u16) -> Result<u64> {
    if node_count == 0 || !matches!(routing, 64 | 128 | 192) {
        return Err(invalid("V35 route tree projection differs"));
    }
    node_count
        .checked_mul(u64::from(routing) + 128)
        .ok_or_else(|| invalid("V35 route tree bytes overflow"))
}

fn build_tree_node(
    dimensions: usize,
    generation: &V35RoutingGeneration,
    ordinals: &mut [u32],
    nodes: &mut Vec<V35TreeNode>,
    leaf_order: &mut Vec<u32>,
) -> Result<u32> {
    let node_ordinal =
        u32::try_from(nodes.len()).map_err(|_| invalid("V35 tree nodes overflow"))?;
    let patch_count = ordinals.iter().try_fold(0_usize, |sum, ordinal| {
        sum.checked_add(generation.route_patch_summaries(*ordinal as usize)?.len())
            .ok_or_else(|| invalid("V35 tree patches overflow"))
    })?;
    let mut center = vec![0.0_f64; dimensions];
    for ordinal in ordinals.iter().copied() {
        for patch in generation.route_patch_summaries(ordinal as usize)? {
            for (sum, value) in center.iter_mut().zip(&patch.mean) {
                *sum += *value;
            }
        }
    }
    center
        .iter_mut()
        .for_each(|value| *value /= patch_count as f64);
    let (center_scale, center_codes, decoded_center) = quantize_center(&center)?;
    let mut radius = 0.0_f64;
    let mut trace_min = f64::INFINITY;
    let mut trace_square_max: f64 = 0.0;
    let mut population_factor_max: f64 = 0.0;
    let mut spectral_bound_max: f64 = 0.0;
    let mut omitted_energy_min = f32::INFINITY;
    let mut omitted_energy_max = 0.0_f32;
    for ordinal in ordinals.iter().copied() {
        for patch in generation.route_patch_summaries(ordinal as usize)? {
            radius = radius.max(next_up(
                squared_distance_interval(&decoded_center, &patch.mean)
                    .1
                    .sqrt(),
            ));
            trace_min = trace_min.min(patch.trace);
            trace_square_max = trace_square_max.max(patch.trace_square);
            population_factor_max = population_factor_max.max(patch.population_factor);
            spectral_bound_max = spectral_bound_max.max(patch.spectral_bound);
            omitted_energy_min = omitted_energy_min.min(patch.omitted_energy);
            omitted_energy_max = omitted_energy_max.max(patch.omitted_energy);
        }
    }
    let leaf_start =
        u32::try_from(leaf_order.len()).map_err(|_| invalid("V35 tree order overflows"))?;
    nodes.push(V35TreeNode {
        center_codes,
        center_scale,
        radius: next_up(radius),
        trace_min: next_down(trace_min),
        trace_square_max: next_up(trace_square_max),
        population_factor_max: next_up(population_factor_max),
        spectral_bound_max: next_up(spectral_bound_max),
        omitted_energy_min: next_down_f32(omitted_energy_min).max(0.0),
        omitted_energy_max: next_up_f32(omitted_energy_max),
        children: [0; FANOUT],
        child_count: 0,
        leaf_start,
        leaf_count: 0,
    });
    if ordinals.len() <= FANOUT {
        leaf_order.extend_from_slice(ordinals);
    } else {
        let representative = |ordinal: u32, dimension: usize| -> Result<(f64, f64)> {
            let patches = generation.route_patch_summaries(ordinal as usize)?;
            let population = patches
                .iter()
                .map(|patch| f64::from(patch.population))
                .sum::<f64>();
            let coordinate = patches
                .iter()
                .map(|patch| f64::from(patch.population) * patch.mean[dimension])
                .sum::<f64>()
                / population;
            Ok((coordinate, population))
        };
        let mut means = vec![0.0_f64; dimensions];
        let mut total_population = 0.0_f64;
        for ordinal in ordinals.iter().copied() {
            let patches = generation.route_patch_summaries(ordinal as usize)?;
            let population = patches
                .iter()
                .map(|patch| f64::from(patch.population))
                .sum::<f64>();
            total_population += population;
            for (dimension, mean) in means.iter_mut().enumerate() {
                *mean += patches.iter().fold(0.0_f64, |sum, patch| {
                    f64::from(patch.population).mul_add(patch.mean[dimension], sum)
                });
            }
        }
        means.iter_mut().for_each(|mean| *mean /= total_population);
        let mut variances = vec![0.0_f64; dimensions];
        for ordinal in ordinals.iter().copied() {
            let patches = generation.route_patch_summaries(ordinal as usize)?;
            let population = patches
                .iter()
                .map(|patch| f64::from(patch.population))
                .sum::<f64>();
            for dimension in 0..dimensions {
                let coordinate = patches.iter().fold(0.0_f64, |sum, patch| {
                    f64::from(patch.population).mul_add(patch.mean[dimension], sum)
                }) / population;
                let delta = coordinate - means[dimension];
                variances[dimension] = population.mul_add(delta * delta, variances[dimension]);
            }
        }
        let mut split_dimension = 0;
        let mut largest_variance = f64::NEG_INFINITY;
        for (dimension, variance) in variances.into_iter().enumerate() {
            if variance > largest_variance {
                largest_variance = variance;
                split_dimension = dimension;
            }
        }
        let mut keys = ordinals
            .iter()
            .copied()
            .map(|ordinal| Ok((ordinal, representative(ordinal, split_dimension)?.0)))
            .collect::<Result<Vec<_>>>()?;
        keys.sort_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        for (output, (ordinal, _)) in ordinals.iter_mut().zip(keys) {
            *output = ordinal;
        }
        let base = ordinals.len() / FANOUT;
        let remainder = ordinals.len() % FANOUT;
        let mut offset = 0;
        for child in 0..FANOUT {
            let length = base + usize::from(child < remainder);
            let child_ordinal = build_tree_node(
                dimensions,
                generation,
                &mut ordinals[offset..offset + length],
                nodes,
                leaf_order,
            )?;
            nodes[node_ordinal as usize].children[child] = child_ordinal;
            offset += length;
        }
        nodes[node_ordinal as usize].child_count = FANOUT as u8;
    }
    nodes[node_ordinal as usize].leaf_count = u32::try_from(leaf_order.len())
        .ok()
        .and_then(|end| end.checked_sub(leaf_start))
        .ok_or_else(|| invalid("V35 tree extent differs"))?;
    Ok(node_ordinal)
}

/// Build the deterministic sixteen-way tree over one authenticated generation.
pub fn build_v35_route_tree(generation: &V35RoutingGeneration) -> Result<V35RouteTree> {
    let mut ordinals = (0..generation.leaf_count())
        .map(|ordinal| u32::try_from(ordinal).map_err(|_| invalid("V35 leaf ordinal overflows")))
        .collect::<Result<Vec<_>>>()?;
    let mut tree = V35RouteTree {
        format: TREE_FORMAT,
        authority_digest: [0; 32],
        dimensions: generation.dimensions(),
        nodes: Vec::new(),
        leaf_order: Vec::with_capacity(ordinals.len()),
        leaf_count: generation.leaf_count(),
        patches_per_leaf: generation.patches_per_leaf(),
        projection_checksum: generation.projection_checksum(),
        route_authority_digest: generation.route_authority_digest(),
        source_archive_sha256: generation.source_archive_sha256().to_owned(),
    };
    let root = build_tree_node(
        usize::from(tree.dimensions.routing),
        generation,
        &mut ordinals,
        &mut tree.nodes,
        &mut tree.leaf_order,
    )?;
    if root != 0 || tree.leaf_order.len() != tree.leaf_count {
        return Err(invalid("V35 tree coverage differs"));
    }
    tree.authority_digest = tree_authority_digest(&tree);
    Ok(tree)
}

/// Compute a conservative lower bound for every descendant patch score.
pub fn bound_v35_node(
    tree: &V35RouteTree,
    node_ordinal: u32,
    query: &[f64],
    omitted: f64,
) -> Result<f64> {
    if query.len() != usize::from(tree.dimensions.routing)
        || query.iter().any(|value| !value.is_finite())
        || !omitted.is_finite()
        || omitted < 0.0
    {
        return Err(invalid("V35 tree query differs"));
    }
    let node = tree
        .nodes
        .get(node_ordinal as usize)
        .ok_or_else(|| invalid("V35 tree node differs"))?;
    let center = node
        .center_codes
        .iter()
        .map(|code| f64::from(*code) * f64::from(node.center_scale))
        .collect::<Vec<_>>();
    let (square_low, square_high) = squared_distance_interval(query, &center);
    let low = next_down(next_down(square_low.sqrt()).max(0.0) - node.radius).max(0.0);
    let high = next_up(next_up(square_high.sqrt()) + node.radius);
    let base = min_lower_tail(
        center.len(),
        next_down(low * low).max(0.0),
        next_up(high * high),
        node.trace_min,
        node.trace_square_max,
        node.population_factor_max,
        node.spectral_bound_max,
    );
    let query_root = omitted.sqrt();
    let interval_low = next_down(f64::from(node.omitted_energy_min).sqrt()).max(0.0);
    let interval_high = next_up(f64::from(node.omitted_energy_max).sqrt());
    let delta = if query_root < interval_low {
        next_down(interval_low - query_root).max(0.0)
    } else if query_root > interval_high {
        next_down(query_root - interval_high).max(0.0)
    } else {
        0.0
    };
    let complement = next_down(delta * delta).max(0.0);
    let bound = next_down(base + complement);
    if bound.is_finite() {
        Ok(bound)
    } else {
        Err(invalid("V35 tree bound differs"))
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
    leaf_ordinal: u32,
}

impl PartialEq for LeafCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits()
            && self.group_ordinal == other.group_ordinal
            && self.leaf_ordinal == other.leaf_ordinal
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
            .then_with(|| other.leaf_ordinal.cmp(&self.leaf_ordinal))
    }
}

/// Certify and admit the exact group prefix using conservative tree bounds.
pub fn hierarchical_v35_route(
    generation: &V35RoutingGeneration,
    tree: &V35RouteTree,
    query: &[f64],
    omitted: f64,
    groups: &[V35GroupStorage],
    budget: V35RouteBudget,
) -> Result<V35RoutePrefix> {
    if tree.format != TREE_FORMAT
        || tree.dimensions != generation.dimensions()
        || tree.leaf_count != generation.leaf_count()
        || tree.patches_per_leaf != generation.patches_per_leaf()
        || tree.projection_checksum != generation.projection_checksum()
        || tree.route_authority_digest != generation.route_authority_digest()
        || tree.source_archive_sha256 != generation.source_archive_sha256()
    {
        return Err(invalid("V35 route tree authority differs"));
    }
    validate_query(generation, query, omitted)?;
    validate_groups(generation, groups)?;
    let generation_digest = generation.route_authority_digest();
    let query_digest = route_query_digest(query, omitted);
    let remote_binding = groups.first().and_then(|group| group.remote_binding);
    let mut nodes = BinaryHeap::new();
    nodes.push(NodeCandidate {
        bound: bound_v35_node(tree, 0, query, omitted)?,
        ordinal: 0,
    });
    let mut bound_evaluations = 1;
    let mut exact_leaf_evaluations = 0;
    let mut leaves = vec![None::<LeafCandidate>; groups.len()];
    let mut emitted = vec![false; groups.len()];
    let mut selected_groups = Vec::new();
    let mut selected_rows = 0_u64;
    let mut selected_code_bytes = 0_u64;
    loop {
        let next_leaf = leaves.iter().flatten().copied().min_by(|left, right| {
            left.score
                .total_cmp(&right.score)
                .then_with(|| left.group_ordinal.cmp(&right.group_ordinal))
                .then_with(|| left.leaf_ordinal.cmp(&right.leaf_ordinal))
        });
        let leaf_is_certified = match (next_leaf, nodes.peek()) {
            (Some(leaf), Some(node)) => leaf.score.total_cmp(&node.bound).is_lt(),
            (Some(_), None) => true,
            (None, _) => false,
        };
        if leaf_is_certified {
            let leaf = next_leaf.ok_or_else(|| invalid("V35 certified leaf differs"))?;
            let group = usize::try_from(leaf.group_ordinal)
                .map_err(|_| invalid("V35 route group ordinal overflows"))?;
            leaves[group] = None;
            if emitted[group] {
                continue;
            }
            emitted[group] = true;
            let selected = V35SelectedGroup {
                storage: groups[group],
                score: leaf.score,
                leaf_ordinal: leaf.leaf_ordinal,
            };
            let next_rows = selected_rows.checked_add(selected.storage.rows);
            let next_bytes = selected_code_bytes.checked_add(selected.storage.code_bytes);
            if selected_groups.len() >= budget.max_groups as usize
                || next_rows.is_none_or(|rows| rows > budget.max_rows)
                || next_bytes.is_none_or(|bytes| bytes > budget.max_code_bytes)
            {
                return Ok(V35RoutePrefix {
                    selected_groups,
                    overflow: Some(selected),
                    selected_rows,
                    selected_code_bytes,
                    exact_leaf_evaluations,
                    bound_evaluations,
                    generation_digest,
                    query_digest,
                    remote_binding,
                });
            }
            selected_rows = next_rows.ok_or_else(|| invalid("V35 route rows overflow"))?;
            selected_code_bytes = next_bytes.ok_or_else(|| invalid("V35 route bytes overflow"))?;
            selected_groups.push(selected);
            if selected_groups.len() == groups.len() {
                return Ok(V35RoutePrefix {
                    selected_groups,
                    overflow: None,
                    selected_rows,
                    selected_code_bytes,
                    exact_leaf_evaluations,
                    bound_evaluations,
                    generation_digest,
                    query_digest,
                    remote_binding,
                });
            }
            continue;
        }
        let Some(candidate) = nodes.pop() else {
            return Ok(V35RoutePrefix {
                selected_groups,
                overflow: None,
                selected_rows,
                selected_code_bytes,
                exact_leaf_evaluations,
                bound_evaluations,
                generation_digest,
                query_digest,
                remote_binding,
            });
        };
        let node = tree
            .nodes
            .get(candidate.ordinal as usize)
            .ok_or_else(|| invalid("V35 tree node differs"))?;
        if node.is_terminal() {
            for leaf_ordinal in tree
                .descendant_leaf_ordinals(candidate.ordinal)
                .ok_or_else(|| invalid("V35 tree leaf extent differs"))?
            {
                exact_leaf_evaluations += 1;
                let candidate = LeafCandidate {
                    score: generation.route_score_leaf(*leaf_ordinal as usize, query, omitted)?,
                    group_ordinal: generation.route_group_ordinal(*leaf_ordinal as usize)?,
                    leaf_ordinal: *leaf_ordinal,
                };
                let group = candidate.group_ordinal as usize;
                if emitted[group] {
                    continue;
                }
                if leaves[group].is_none_or(|current| {
                    candidate.score.total_cmp(&current.score).is_lt()
                        || (candidate.score.to_bits() == current.score.to_bits()
                            && candidate.leaf_ordinal < current.leaf_ordinal)
                }) {
                    leaves[group] = Some(candidate);
                }
            }
        } else {
            for child in node.children[..usize::from(node.child_count)]
                .iter()
                .copied()
            {
                bound_evaluations += 1;
                nodes.push(NodeCandidate {
                    bound: bound_v35_node(tree, child, query, omitted)?,
                    ordinal: child,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::quantize_center;

    #[test]
    fn v35_route_quantized_center_matches_serving_f64_decode() {
        let (scale, codes, decoded) = quantize_center(&[0.1, 1.0]).unwrap();
        for (code, value) in codes.iter().zip(decoded) {
            assert_eq!(f64::from(value), f64::from(*code) * f64::from(scale));
        }
    }
}
