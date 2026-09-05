use std::collections::{BTreeMap, BTreeSet};

use crate::{BorsukError, Result};

const DIMENSIONS: usize = 96;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, PartialEq)]
struct V33LeafPopulation {
    routing_leaf_ordinal: u32,
    group_ordinal: u32,
    rows: Vec<(u64, [f32; DIMENSIONS])>,
}

#[derive(Debug, Clone, PartialEq)]
struct V33LeafShape {
    routing_leaf_ordinal: u32,
    group_ordinal: u32,
    population: u64,
    mean: [f32; DIMENSIONS],
    diagonal_variance: [f32; DIMENSIONS],
    scalar_moment: f32,
    split_dimension: usize,
    split_centers: [[f32; DIMENSIONS]; 2],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum V33ShapeArm {
    Centroid,
    ScalarMoment,
    DiagonalMoment,
    SplitCentroid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct V33GroupPopulation {
    ordinal: u32,
    rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct V33ShapeControlBytes {
    scalar_summary_bytes: usize,
    scalar_extra_centers: usize,
    scalar_padding_bytes: usize,
    diagonal_summary_bytes: usize,
    diagonal_control_bytes: usize,
}

fn v33_shape_control_bytes(leaf_count: usize) -> Result<V33ShapeControlBytes> {
    if leaf_count == 0 {
        return Err(invalid("V33 shape leaf count differs"));
    }
    let center_bytes = DIMENSIONS * size_of::<f32>();
    let scalar_extra_bytes = leaf_count
        .checked_mul(size_of::<f32>())
        .ok_or_else(|| invalid("V33 scalar control bytes overflow"))?;
    let scalar_extra_centers = scalar_extra_bytes / center_bytes;
    let scalar_padding_bytes = scalar_extra_bytes % center_bytes;
    let scalar_summary_bytes = leaf_count
        .checked_mul(center_bytes + size_of::<f32>())
        .ok_or_else(|| invalid("V33 scalar summary bytes overflow"))?;
    let diagonal_summary_bytes = leaf_count
        .checked_mul(center_bytes * 2)
        .ok_or_else(|| invalid("V33 diagonal summary bytes overflow"))?;
    Ok(V33ShapeControlBytes {
        scalar_summary_bytes,
        scalar_extra_centers,
        scalar_padding_bytes,
        diagonal_summary_bytes,
        diagonal_control_bytes: diagonal_summary_bytes,
    })
}

fn summarize_v33_leaf(population: &V33LeafPopulation) -> Result<V33LeafShape> {
    if population.rows.is_empty()
        || population
            .rows
            .iter()
            .any(|(_, row)| row.iter().any(|value| !value.is_finite()))
        || population
            .rows
            .iter()
            .map(|(ordinal, _)| *ordinal)
            .collect::<BTreeSet<_>>()
            .len()
            != population.rows.len()
    {
        return Err(invalid("V33 leaf population differs"));
    }
    let count = population.rows.len() as f64;
    let mut mean64 = [0.0_f64; DIMENSIONS];
    for (_, row) in &population.rows {
        for dimension in 0..DIMENSIONS {
            mean64[dimension] += f64::from(row[dimension]);
        }
    }
    for value in &mut mean64 {
        *value /= count;
    }
    let mut variance64 = [0.0_f64; DIMENSIONS];
    for (_, row) in &population.rows {
        for dimension in 0..DIMENSIONS {
            let delta = f64::from(row[dimension]) - mean64[dimension];
            variance64[dimension] += delta * delta;
        }
    }
    for value in &mut variance64 {
        *value /= count;
    }
    let scalar64 = variance64.iter().sum::<f64>();
    if mean64
        .iter()
        .chain(variance64.iter())
        .chain(std::iter::once(&scalar64))
        .any(|value| !value.is_finite())
    {
        return Err(invalid("V33 leaf moment is nonfinite"));
    }
    let split_dimension = variance64
        .iter()
        .enumerate()
        .max_by(|left, right| left.1.total_cmp(right.1).then_with(|| right.0.cmp(&left.0)))
        .unwrap()
        .0;
    let mut ordered = population.rows.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        left.1[split_dimension]
            .total_cmp(&right.1[split_dimension])
            .then_with(|| left.0.cmp(&right.0))
    });
    let mean = mean64.map(|value| value as f32);
    let diagonal_variance = variance64.map(|value| value as f32);
    let mut split_centers = [mean; 2];
    if ordered.len() > 1 {
        let cut = ordered.len() / 2;
        for (slot, rows) in [&ordered[..cut], &ordered[cut..]].into_iter().enumerate() {
            let mut center = [0.0_f64; DIMENSIONS];
            for (_, row) in rows {
                for dimension in 0..DIMENSIONS {
                    center[dimension] += f64::from(row[dimension]);
                }
            }
            for dimension in 0..DIMENSIONS {
                split_centers[slot][dimension] = (center[dimension] / rows.len() as f64) as f32;
            }
        }
    }
    Ok(V33LeafShape {
        routing_leaf_ordinal: population.routing_leaf_ordinal,
        group_ordinal: population.group_ordinal,
        population: population.rows.len() as u64,
        mean,
        diagonal_variance,
        scalar_moment: scalar64 as f32,
        split_dimension,
        split_centers,
    })
}

fn squared_distance(left: &[f32; DIMENSIONS], right: &[f32; DIMENSIONS]) -> Result<f64> {
    let mut distance = 0.0_f64;
    for dimension in 0..DIMENSIONS {
        let delta = f64::from(left[dimension]) - f64::from(right[dimension]);
        distance += delta * delta;
    }
    if !distance.is_finite() {
        return Err(invalid("V33 shape distance is nonfinite"));
    }
    Ok(if distance == 0.0 { 0.0 } else { distance })
}

fn score_v33_leaf(leaf: &V33LeafShape, query: &[f32; DIMENSIONS], arm: V33ShapeArm) -> Result<f64> {
    if leaf.population == 0
        || query.iter().any(|value| !value.is_finite())
        || leaf.mean.iter().any(|value| !value.is_finite())
        || leaf
            .diagonal_variance
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
        || !leaf.scalar_moment.is_finite()
        || leaf.scalar_moment < 0.0
    {
        return Err(invalid("V33 shape score authority differs"));
    }
    let distance = squared_distance(&leaf.mean, query)?;
    let score = match arm {
        V33ShapeArm::Centroid => distance,
        V33ShapeArm::SplitCentroid => squared_distance(&leaf.split_centers[0], query)?
            .min(squared_distance(&leaf.split_centers[1], query)?),
        V33ShapeArm::ScalarMoment => {
            let moment = f64::from(leaf.scalar_moment);
            let variance = 2.0 * moment * moment / DIMENSIONS as f64
                + 4.0 * moment * distance / DIMENSIONS as f64;
            distance + moment - extreme_factor(leaf.population) * variance.sqrt()
        }
        V33ShapeArm::DiagonalMoment => {
            let mut moment = 0.0_f64;
            let mut variance_square = 0.0_f64;
            let mut directional = 0.0_f64;
            for dimension in 0..DIMENSIONS {
                let variance = f64::from(leaf.diagonal_variance[dimension]);
                let delta = f64::from(query[dimension]) - f64::from(leaf.mean[dimension]);
                moment += variance;
                variance_square += variance * variance;
                directional += delta * delta * variance;
            }
            distance + moment
                - extreme_factor(leaf.population)
                    * (2.0 * variance_square + 4.0 * directional).sqrt()
        }
    };
    if !score.is_finite() {
        return Err(invalid("V33 shape score is nonfinite"));
    }
    Ok(if score == 0.0 { 0.0 } else { score })
}

fn extreme_factor(population: u64) -> f64 {
    if population <= 1 {
        0.0
    } else {
        (2.0 * (population as f64).ln()).sqrt()
    }
}

fn rank_v33_groups(
    leaves: &[V33LeafShape],
    query: &[f32; DIMENSIONS],
    arm: V33ShapeArm,
) -> Result<Vec<u32>> {
    if leaves.is_empty() {
        return Err(invalid("V33 shape leaf summaries differ"));
    }
    let mut scores = BTreeMap::<u32, f64>::new();
    for leaf in leaves {
        let score = score_v33_leaf(leaf, query, arm)?;
        scores
            .entry(leaf.group_ordinal)
            .and_modify(|current| *current = current.min(score))
            .or_insert(score);
    }
    let mut ranked = scores.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    Ok(ranked.into_iter().map(|(ordinal, _)| ordinal).collect())
}

fn select_v33_group_prefix(
    groups: &[V33GroupPopulation],
    ranked: &[u32],
    row_limit: u64,
    group_limit: usize,
) -> Result<Vec<u32>> {
    if groups.is_empty() || row_limit == 0 || group_limit == 0 {
        return Err(invalid("V33 group prefix bounds differ"));
    }
    let by_ordinal = groups
        .iter()
        .map(|group| (group.ordinal, group.rows))
        .collect::<BTreeMap<_, _>>();
    if by_ordinal.len() != groups.len() || groups.iter().any(|group| group.rows == 0) {
        return Err(invalid("V33 group population authority differs"));
    }
    let mut seen = BTreeSet::new();
    let mut selected = Vec::new();
    let mut rows = 0_u64;
    for ordinal in ranked.iter().copied() {
        if !seen.insert(ordinal) {
            return Err(invalid("V33 ranked group authority differs"));
        }
        let population = *by_ordinal
            .get(&ordinal)
            .ok_or_else(|| invalid("V33 ranked group authority differs"))?;
        let next = rows
            .checked_add(population)
            .ok_or_else(|| invalid("V33 selected rows overflow"))?;
        if selected.len() == group_limit || next > row_limit {
            break;
        }
        selected.push(ordinal);
        rows = next;
    }
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::{
        V33GroupPopulation, V33LeafPopulation, V33ShapeArm, rank_v33_groups, score_v33_leaf,
        select_v33_group_prefix, summarize_v33_leaf, v33_shape_control_bytes,
    };

    fn row(logical_ordinal: u64, first: f32, second: f32) -> (u64, [f32; 96]) {
        let mut values = [0.0; 96];
        values[0] = first;
        values[1] = second;
        (logical_ordinal, values)
    }

    #[test]
    fn v33_group_shape_moments_use_complete_gaussian_variance_without_clamp() {
        let leaf = V33LeafPopulation {
            routing_leaf_ordinal: 7,
            group_ordinal: 3,
            rows: vec![row(10, 1.0, 0.0), row(11, 3.0, 0.0)],
        };
        let summary = summarize_v33_leaf(&leaf).unwrap();
        assert_eq!(summary.population, 2);
        assert_eq!(summary.mean[0], 2.0);
        assert_eq!(summary.diagonal_variance[0], 1.0);
        assert_eq!(summary.scalar_moment, 1.0);
        assert_eq!(summary.split_centers[0][0], 1.0);
        assert_eq!(summary.split_centers[1][0], 3.0);

        let query = row(0, 4.0, 0.0).1;
        let a = (2.0_f64 * 2.0_f64.ln()).sqrt();
        let scalar_expected = 5.0 - a * (18.0_f64 / 96.0).sqrt();
        let diagonal_expected = 5.0 - a * 18.0_f64.sqrt();
        assert_eq!(
            score_v33_leaf(&summary, &query, V33ShapeArm::ScalarMoment).unwrap(),
            scalar_expected
        );
        assert_eq!(
            score_v33_leaf(&summary, &query, V33ShapeArm::DiagonalMoment).unwrap(),
            diagonal_expected
        );

        let far_spread = V33LeafPopulation {
            routing_leaf_ordinal: 8,
            group_ordinal: 4,
            rows: vec![row(12, -100.0, 0.0), row(13, 100.0, 0.0)],
        };
        let signed = score_v33_leaf(
            &summarize_v33_leaf(&far_spread).unwrap(),
            &[0.0; 96],
            V33ShapeArm::DiagonalMoment,
        )
        .unwrap();
        assert!(
            signed < 0.0,
            "negative ranking evidence must not be clamped"
        );
    }

    #[test]
    fn v33_group_shape_equal_byte_controls_are_exact_and_deterministic() {
        let bytes = v33_shape_control_bytes(4_141).unwrap();
        assert_eq!(bytes.scalar_summary_bytes, 4_141 * 388);
        assert_eq!(bytes.scalar_extra_centers, 43);
        assert_eq!(bytes.scalar_padding_bytes, 52);
        assert_eq!(bytes.diagonal_summary_bytes, 4_141 * 768);
        assert_eq!(bytes.diagonal_control_bytes, bytes.diagonal_summary_bytes);

        let leaf = V33LeafPopulation {
            routing_leaf_ordinal: 2,
            group_ordinal: 1,
            rows: vec![
                row(9, 2.0, 0.0),
                row(4, -2.0, 0.0),
                row(7, 1.0, 0.0),
                row(5, -1.0, 0.0),
            ],
        };
        let summary = summarize_v33_leaf(&leaf).unwrap();
        assert_eq!(summary.split_dimension, 0);
        assert_eq!(summary.split_centers[0][0], -1.5);
        assert_eq!(summary.split_centers[1][0], 1.5);
    }

    #[test]
    fn v33_group_shape_group_min_ties_overflow_and_duplicate_truth_are_preserved() {
        let leaves = vec![
            summarize_v33_leaf(&V33LeafPopulation {
                routing_leaf_ordinal: 2,
                group_ordinal: 1,
                rows: vec![row(0, 1.0, 0.0)],
            })
            .unwrap(),
            summarize_v33_leaf(&V33LeafPopulation {
                routing_leaf_ordinal: 0,
                group_ordinal: 0,
                rows: vec![row(1, 1.0, 0.0)],
            })
            .unwrap(),
            summarize_v33_leaf(&V33LeafPopulation {
                routing_leaf_ordinal: 1,
                group_ordinal: 0,
                rows: vec![row(2, 4.0, 0.0)],
            })
            .unwrap(),
        ];
        let ranked = rank_v33_groups(&leaves, &[0.0; 96], V33ShapeArm::Centroid).unwrap();
        assert_eq!(ranked, vec![0, 1]);

        let groups = vec![
            V33GroupPopulation {
                ordinal: 0,
                rows: 7,
            },
            V33GroupPopulation {
                ordinal: 1,
                rows: 6,
            },
            V33GroupPopulation {
                ordinal: 2,
                rows: 1,
            },
        ];
        assert_eq!(
            select_v33_group_prefix(&groups, &[0, 1, 2], 12, 3).unwrap(),
            vec![0]
        );

        let truth_groups = [0_u32, 0, 1, 0, 1, 1, 0, 1, 0, 1];
        let selected = [0_u32];
        assert_eq!(
            truth_groups
                .iter()
                .filter(|group| selected.contains(group))
                .count(),
            5
        );
    }
}
