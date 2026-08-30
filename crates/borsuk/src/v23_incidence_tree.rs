use half::f16;
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result, v23_incidence::V23FmaBackend};

pub(crate) const V23_INCIDENCE_RESERVOIR_ROWS: usize = 2_097_152;
pub(crate) const V23_INCIDENCE_TREE_DEPTH: usize = 16;
pub(crate) const V23_INCIDENCE_LEAVES: usize = 65_536;

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

fn validate_rows(rows: &[V23TrainingRow], shape: V23IncidenceTrainingShape) -> Result<()> {
    if shape.dimensions != 96
        || shape.reservoir_rows == 0
        || shape.reservoir_rows > rows.len()
        || shape.depth == 0
        || shape.depth > 16
        || shape.lloyd_iterations != 4
        || shape.reservoir_rows < (1_usize << shape.depth)
    {
        return Err(invalid("V23 incidence training shape differs"));
    }
    let mut ordinals = rows
        .iter()
        .map(|row| row.source_ordinal)
        .collect::<Vec<_>>();
    ordinals.sort_unstable();
    if ordinals.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid("V23 incidence source ordinal is duplicated"));
    }
    for row in rows {
        normalized(&row.vector)?;
    }
    Ok(())
}

pub(crate) fn select_reservoir(
    rows: &[V23TrainingRow],
    shape: V23IncidenceTrainingShape,
    seed: u64,
) -> Result<Vec<V23TrainingRow>> {
    validate_rows(rows, shape)?;
    let mut keyed = rows
        .iter()
        .map(|row| {
            (
                splitmix64(row.source_ordinal ^ seed),
                row.source_ordinal,
                row,
            )
        })
        .collect::<Vec<_>>();
    keyed.sort_unstable_by_key(|(key, ordinal, _)| (*key, *ordinal));
    let mut selected = keyed
        .into_iter()
        .take(shape.reservoir_rows)
        .map(|(_, _, row)| V23TrainingRow {
            source_ordinal: row.source_ordinal,
            vector: normalized(&row.vector).unwrap(),
        })
        .collect::<Vec<_>>();
    selected.sort_unstable_by_key(|row| row.source_ordinal);
    Ok(selected)
}

fn centroid(rows: &[usize], reservoir: &[V23TrainingRow]) -> Result<[f32; 96]> {
    if rows.is_empty() {
        return Err(invalid("V23 incidence tree node is empty"));
    }
    let mut ordered = rows.to_vec();
    ordered.sort_unstable_by_key(|index| reservoir[*index].source_ordinal);
    let mut partials = ordered
        .chunks(4096)
        .map(|chunk| {
            let mut partial = [0.0_f64; 96];
            for index in chunk {
                for (sum, value) in partial.iter_mut().zip(reservoir[*index].vector) {
                    *sum += f64::from(value);
                }
            }
            partial
        })
        .collect::<Vec<_>>();
    partials.resize(partials.len().next_power_of_two(), [0.0_f64; 96]);
    while partials.len() > 1 {
        let mut merged = Vec::with_capacity(partials.len() / 2);
        for pair in partials.chunks_exact(2) {
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
    reservoir: &[V23TrainingRow],
    shape: V23IncidenceTrainingShape,
    use_fused: bool,
) -> Result<(V23TreeNode, Vec<usize>, Vec<usize>)> {
    if members.len() < 2 {
        return Err(invalid("V23 incidence tree split is empty"));
    }
    let first = *members
        .iter()
        .min_by_key(|index| reservoir[**index].source_ordinal)
        .unwrap();
    let first_vector = reservoir[first].vector;
    let second = *members
        .iter()
        .filter(|index| **index != first)
        .max_by(|left, right| {
            let left_distance =
                1.0 - training_dot(use_fused, &first_vector, &reservoir[**left].vector).unwrap();
            let right_distance =
                1.0 - training_dot(use_fused, &first_vector, &reservoir[**right].vector).unwrap();
            left_distance.total_cmp(&right_distance).then_with(|| {
                reservoir[**right]
                    .source_ordinal
                    .cmp(&reservoir[**left].source_ordinal)
            })
        })
        .unwrap();
    let mut zero = reservoir[first].vector;
    let mut one = reservoir[second].vector;
    for _ in 0..shape.lloyd_iterations {
        let mut zero_members = Vec::new();
        let mut one_members = Vec::new();
        for index in members {
            let zero_distance = 1.0 - training_dot(use_fused, &reservoir[*index].vector, &zero)?;
            let one_distance = 1.0 - training_dot(use_fused, &reservoir[*index].vector, &one)?;
            if zero_distance.total_cmp(&one_distance).is_le() {
                zero_members.push(*index);
            } else {
                one_members.push(*index);
            }
        }
        zero = centroid(&zero_members, reservoir)?;
        one = centroid(&one_members, reservoir)?;
    }
    let (child_zero, child_zero_inverse_norm) = roundtrip_centroid(&zero)?;
    let (child_one, child_one_inverse_norm) = roundtrip_centroid(&one)?;
    let mut scored = members
        .iter()
        .map(|index| {
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
            Ok((
                if use_fused {
                    split_score_simd(&placeholder, &reservoir[*index].vector)?.0
                } else {
                    split_score_scalar(&placeholder, &reservoir[*index].vector)
                },
                reservoir[*index].source_ordinal,
                *index,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
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

fn leaf(members: &[usize], reservoir: &[V23TrainingRow], use_fused: bool) -> Result<V23TreeLeaf> {
    let center = centroid(members, reservoir)?;
    let (centroid, inverse_norm) = roundtrip_centroid(&center)?;
    let decoded = centroid.map(f16::to_f32);
    let mut residual = 0.0_f64;
    for index in members {
        let distance =
            1.0 - training_dot(use_fused, &reservoir[*index].vector, &decoded)? * inverse_norm;
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
    if threads == 0 || batch_rows == 0 {
        return Err(invalid("V23 incidence execution shape differs"));
    }
    if use_fused {
        borsuk_fma::fused_dot_8x12(&[0.0; 96], &[0.0; 96])
            .map_err(|_| invalid("V23 incidence fused SIMD backend is unavailable"))?;
    }
    let seed = reservoir_seed("77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d")?;
    let reservoir = select_reservoir(rows, shape, seed)?;
    let mut groups = vec![(0..reservoir.len()).collect::<Vec<_>>()];
    let node_count = (1_usize << shape.depth) - 1;
    let mut nodes = Vec::with_capacity(node_count);
    for level in 0..shape.depth {
        let mut next = Vec::with_capacity(groups.len() * 2);
        for (group_index, group) in groups.iter().enumerate() {
            let (mut node, zero, one) = train_split(group, &reservoir, shape, use_fused)?;
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
    }
    let leaves = groups
        .iter()
        .map(|group| leaf(group, &reservoir, use_fused))
        .collect::<Result<Vec<_>>>()?;
    Ok(V23IncidenceTree {
        shape,
        reservoir_seed: seed,
        work: training_work(shape)?,
        nodes,
        leaves,
    })
}

pub(crate) fn train_incidence_tree(
    rows: &[V23TrainingRow],
    threads: usize,
    batch_rows: usize,
) -> Result<V23IncidenceTree> {
    train_incidence_tree_internal(
        rows,
        V23IncidenceTrainingShape::PRODUCTION,
        threads,
        batch_rows,
        true,
    )
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
    let row = normalized(vector)?;
    let node_count = tree.nodes.len();
    let mut index = 0_usize;
    while index < node_count {
        let node = &tree.nodes[index];
        let score = split_score_simd(node, &row)?.0;
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
    let row = normalized(vector)?;
    let node_count = tree.nodes.len();
    let mut candidates = vec![(0_usize, 0.0_f32)];
    for _ in 0..tree.shape.depth {
        let mut next = Vec::with_capacity(candidates.len() * 2);
        for (index, penalty) in candidates {
            let node = tree
                .nodes
                .get(index)
                .ok_or_else(|| invalid("V23 incidence beam node differs"))?;
            let score = split_score_simd(node, &row)?.0;
            let boundary = f32::from_bits(node.boundary_score_bits);
            let zero = take_zero(node, score, source_ordinal);
            next.push((
                if zero {
                    node.child_zero_index as usize
                } else {
                    node.child_one_index as usize
                },
                penalty,
            ));
            next.push((
                if zero {
                    node.child_one_index as usize
                } else {
                    node.child_zero_index as usize
                },
                penalty + (score - boundary).abs(),
            ));
        }
        next.sort_unstable_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        next.dedup_by_key(|entry| entry.0);
        next.truncate(2);
        candidates = next;
    }
    if candidates.len() != 2 {
        return Err(invalid("V23 incidence beam leaves differ"));
    }
    let mut leaves = [0_u16; 2];
    for (output, (index, _)) in leaves.iter_mut().zip(candidates) {
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
        BeamSelectedLeaves, V23IncidenceTrainingShape, V23TrainingRow, V23TreeNode,
        assign_one_leaf, assign_two_beam_leaves, decode_incidence_tree, encode_incidence_tree,
        reservoir_seed, split_score_scalar, split_score_simd, train_incidence_tree_with_shape,
        train_incidence_tree_with_shape_fused, training_work,
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
        let BeamSelectedLeaves(pair) =
            assign_two_beam_leaves(&tree, &rows[9].vector, rows[9].source_ordinal).unwrap();
        assert_eq!(pair[0], leaf);
        assert_ne!(pair[0], pair[1]);

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
