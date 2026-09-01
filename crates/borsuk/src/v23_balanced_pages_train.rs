use std::collections::BTreeSet;

use half::f16;

use crate::{
    BorsukError, Result,
    v23_incidence_tree::{
        V23IncidenceTrainingShape, V23IncidenceTree, V23ReservoirRow,
        assign_boundary_runner_up_leaves, encode_incidence_tree, normalize_v23_incidence_vector,
        train_incidence_tree_from_reservoir,
    },
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23BalancedTrainingRow {
    pub(crate) source_ordinal: u64,
    pub(crate) vector: [f32; 96],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V23BalancedRoutedSupercells {
    pub(crate) primary_supercell: u32,
    pub(crate) runner_up_supercell: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23SupercellModel {
    tree: V23IncidenceTree,
    training_ordinals: BTreeSet<u64>,
    pseudoquery_ordinals: BTreeSet<u64>,
}

impl V23SupercellModel {
    pub(crate) fn canonical_tree_bytes(&self) -> Result<Vec<u8>> {
        encode_incidence_tree(&self.tree)
    }

    pub(crate) fn training_ordinals(&self) -> &BTreeSet<u64> {
        &self.training_ordinals
    }

    pub(crate) fn pseudoquery_ordinals(&self) -> &BTreeSet<u64> {
        &self.pseudoquery_ordinals
    }
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(format!("V23 balanced page training {message}"))
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub(crate) fn train_v23_balanced_tree(
    rows: Vec<V23BalancedTrainingRow>,
    pseudoquery_rows: usize,
    supercells: usize,
    seed: u64,
    threads: usize,
    batch_rows: usize,
) -> Result<V23SupercellModel> {
    if rows.len() <= pseudoquery_rows
        || pseudoquery_rows == 0
        || supercells == 0
        || supercells > 8_192
        || !supercells.is_power_of_two()
    {
        return Err(invalid("shape differs"));
    }
    let mut seen = BTreeSet::new();
    let mut ranked = Vec::with_capacity(rows.len());
    for row in rows {
        if !seen.insert(row.source_ordinal) {
            return Err(invalid("source ordinal duplicates"));
        }
        let vector = normalize_v23_incidence_vector(&row.vector)?;
        ranked.push((
            splitmix64(row.source_ordinal ^ seed),
            row.source_ordinal,
            vector,
        ));
    }
    ranked.sort_unstable_by_key(|entry| (entry.0, entry.1));
    let pseudo_start = ranked.len() - pseudoquery_rows;
    let pseudoquery_ordinals = ranked[pseudo_start..]
        .iter()
        .map(|entry| entry.1)
        .collect::<BTreeSet<_>>();
    let mut training = ranked[..pseudo_start]
        .iter()
        .map(|entry| V23ReservoirRow {
            source_ordinal: entry.1,
            vector: entry.2.map(f16::from_f32),
        })
        .collect::<Vec<_>>();
    training.sort_unstable_by_key(|row| row.source_ordinal);
    let training_ordinals = training
        .iter()
        .map(|row| row.source_ordinal)
        .collect::<BTreeSet<_>>();
    if training.len() < supercells * 2 {
        return Err(invalid("training population is too small"));
    }
    let shape = V23IncidenceTrainingShape {
        dimensions: 96,
        reservoir_rows: training.len(),
        depth: usize::try_from(supercells.trailing_zeros()).unwrap(),
        lloyd_iterations: 4,
    };
    let tree =
        train_incidence_tree_from_reservoir(training, shape, seed, threads, batch_rows, true)?;
    Ok(V23SupercellModel {
        tree,
        training_ordinals,
        pseudoquery_ordinals,
    })
}

fn score_all_v23_supercells(
    model: &V23SupercellModel,
    query: &[f32; 96],
) -> Result<Vec<(f32, u32)>> {
    let query = normalize_v23_incidence_vector(query)?;
    model
        .tree
        .leaves
        .iter()
        .enumerate()
        .map(|(ordinal, leaf)| {
            if !leaf.inverse_norm.is_finite() || leaf.inverse_norm <= 0.0 {
                return Err(invalid("supercell inverse norm differs"));
            }
            let centroid = leaf.centroid.map(f16::to_f32);
            let dot = borsuk_fma::fused_dot_8x12(&query, &centroid)
                .map_err(|_| invalid("fused SIMD backend unavailable"))?
                .0;
            let distance: f32 = 1.0 - dot * leaf.inverse_norm;
            if !distance.is_finite() {
                return Err(invalid("supercell distance is non-finite"));
            }
            Ok((
                distance,
                u32::try_from(ordinal).map_err(|_| invalid("supercell ordinal overflows"))?,
            ))
        })
        .collect()
}

pub(crate) fn score_all_v23_supercells_f64_reference(
    model: &V23SupercellModel,
    query: &[f32; 96],
) -> Result<Vec<(f64, u32)>> {
    let query = normalize_v23_incidence_vector(query)?;
    model
        .tree
        .leaves
        .iter()
        .enumerate()
        .map(|(ordinal, leaf)| {
            let centroid = leaf.centroid.map(f16::to_f32);
            let dot = query
                .iter()
                .zip(centroid)
                .map(|(left, right)| f64::from(*left) * f64::from(right))
                .sum::<f64>();
            let squared_norm = centroid
                .iter()
                .map(|value| f64::from(*value) * f64::from(*value))
                .sum::<f64>();
            if !dot.is_finite() || !squared_norm.is_finite() || squared_norm <= 0.0 {
                return Err(invalid("f64 supercell reference differs"));
            }
            Ok((
                1.0 - dot / squared_norm.sqrt(),
                u32::try_from(ordinal).map_err(|_| invalid("supercell ordinal overflows"))?,
            ))
        })
        .collect()
}

pub(crate) fn score_all_v23_supercells_fused(
    model: &V23SupercellModel,
    query: &[f32; 96],
) -> Result<Vec<(f32, u32)>> {
    score_all_v23_supercells(model, query)
}

pub(crate) fn route_v23_supercell_beam2(
    model: &V23SupercellModel,
    vector: &[f32; 96],
    source_ordinal: u64,
) -> Result<V23BalancedRoutedSupercells> {
    let beam = assign_boundary_runner_up_leaves(&model.tree, vector, source_ordinal)?.0;
    let beam = beam.map(u32::from);
    if beam[1] == beam[0] {
        return Err(invalid("beam-two primary authority differs"));
    }
    Ok(V23BalancedRoutedSupercells {
        primary_supercell: beam[0],
        runner_up_supercell: beam[1],
    })
}

#[cfg(test)]
mod tests {
    use super::{
        V23BalancedTrainingRow, route_v23_supercell_beam2, score_all_v23_supercells_f64_reference,
        score_all_v23_supercells_fused, train_v23_balanced_tree,
    };
    use crate::v23_incidence_tree::{
        V23IncidenceTrainingShape, V23IncidenceTree, V23TrainingWork, V23TreeLeaf, V23TreeNode,
        production_codec_shape_is_allowed,
    };
    use half::f16;

    fn rows() -> Vec<V23BalancedTrainingRow> {
        (0_u64..64)
            .map(|source_ordinal| {
                let cluster = usize::try_from(source_ordinal % 8).unwrap();
                let mut vector = [0.0_f32; 96];
                vector[cluster] = 1.0;
                vector[8 + cluster] = 0.25 + source_ordinal as f32 * 0.0001;
                V23BalancedTrainingRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect()
    }

    #[test]
    fn v23_balanced_training_production_shapes_are_serializable() {
        for depth in [10, 13] {
            assert!(production_codec_shape_is_allowed(
                V23IncidenceTrainingShape {
                    dimensions: 96,
                    reservoir_rows: 2_096_128,
                    depth,
                    lloyd_iterations: 4,
                }
            ));
        }
        assert!(!production_codec_shape_is_allowed(
            V23IncidenceTrainingShape {
                dimensions: 96,
                reservoir_rows: 2_096_128,
                depth: 12,
                lloyd_iterations: 4,
            }
        ));
    }

    #[test]
    fn v23_balanced_training_excludes_pseudoqueries_and_is_worker_deterministic() {
        let one = train_v23_balanced_tree(rows(), 8, 8, 0x1234_5678, 1, 7).unwrap();
        let four = train_v23_balanced_tree(rows(), 8, 8, 0x1234_5678, 4, 5).unwrap();
        assert_eq!(
            one.canonical_tree_bytes().unwrap(),
            four.canonical_tree_bytes().unwrap()
        );
        assert_eq!(one.training_ordinals().len(), 56);
        assert_eq!(one.pseudoquery_ordinals().len(), 8);
        assert!(
            one.training_ordinals()
                .is_disjoint(one.pseudoquery_ordinals())
        );
        assert_eq!(one.pseudoquery_ordinals(), four.pseudoquery_ordinals());
    }

    #[test]
    fn v23_balanced_training_scores_every_supercell_with_independent_f64_order_control() {
        let model = train_v23_balanced_tree(rows(), 8, 8, 0x1234_5678, 2, 7).unwrap();
        let query = rows()[3].vector;
        let mut reference = score_all_v23_supercells_f64_reference(&model, &query).unwrap();
        let mut fused = score_all_v23_supercells_fused(&model, &query).unwrap();
        reference.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        fused.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        assert_eq!(
            fused.iter().map(|entry| entry.1).collect::<Vec<_>>(),
            reference.iter().map(|entry| entry.1).collect::<Vec<_>>()
        );
        assert_eq!(fused.len(), 8);
        assert_eq!(
            fused
                .iter()
                .map(|entry| entry.1)
                .collect::<std::collections::BTreeSet<_>>(),
            (0_u32..8).collect::<std::collections::BTreeSet<_>>()
        );
    }

    #[test]
    fn v23_balanced_training_beam_two_preserves_primary_and_rejects_bad_input() {
        let model = train_v23_balanced_tree(rows(), 8, 8, 0x1234_5678, 2, 7).unwrap();
        let row = &rows()[11];
        let routed = route_v23_supercell_beam2(&model, &row.vector, row.source_ordinal).unwrap();
        assert_ne!(routed.primary_supercell, routed.runner_up_supercell);
        assert!(routed.primary_supercell < 8);
        assert!(routed.runner_up_supercell < 8);

        let mut nonfinite = row.vector;
        nonfinite[0] = f32::NAN;
        assert!(route_v23_supercell_beam2(&model, &nonfinite, row.source_ordinal).is_err());
        assert!(train_v23_balanced_tree(rows(), 8, 7, 0x1234_5678, 2, 7).is_err());
    }

    #[test]
    fn v23_balanced_training_runner_up_uses_the_primary_boundary_partition() {
        let mut zero = [f16::ZERO; 96];
        zero[0] = f16::ONE;
        let mut one = [f16::ZERO; 96];
        one[1] = f16::ONE;
        let model = super::V23SupercellModel {
            tree: V23IncidenceTree {
                shape: V23IncidenceTrainingShape {
                    dimensions: 96,
                    reservoir_rows: 2,
                    depth: 1,
                    lloyd_iterations: 4,
                },
                reservoir_seed: 1,
                work: V23TrainingWork {
                    farthest_seed_dimensions: 0,
                    lloyd_dimensions: 0,
                    repartition_dimensions: 0,
                    total_distance_dimensions: 0,
                },
                nodes: vec![V23TreeNode {
                    child_zero: zero,
                    child_one: one,
                    child_zero_inverse_norm: 1.0,
                    child_one_inverse_norm: 1.0,
                    boundary_score_bits: 2.0_f32.to_bits(),
                    boundary_source_ordinal: u64::MAX,
                    child_zero_index: 1,
                    child_one_index: 2,
                }],
                leaves: vec![
                    V23TreeLeaf {
                        centroid: zero,
                        inverse_norm: 1.0,
                        population: 1,
                        mean_squared_residual: 0.0,
                    },
                    V23TreeLeaf {
                        centroid: one,
                        inverse_norm: 1.0,
                        population: 1,
                        mean_squared_residual: 0.0,
                    },
                ],
            },
            training_ordinals: [0].into_iter().collect(),
            pseudoquery_ordinals: [1].into_iter().collect(),
        };
        let mut vector = [0.0_f32; 96];
        vector[1] = 1.0;

        assert_eq!(
            route_v23_supercell_beam2(&model, &vector, 0).unwrap(),
            super::V23BalancedRoutedSupercells {
                primary_supercell: 0,
                runner_up_supercell: 1,
            }
        );
    }
}
