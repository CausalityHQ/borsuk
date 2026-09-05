//! Focused authority, algebra, and projection tests for V34 rank-four routing.

use borsuk::{
    V34Rank4LeafInput, build_v34_rank4_generation, project_v34_serving_memory, score_v34_rank4_leaf,
};

const DIMENSIONS: usize = 96;

fn coherent_leaf() -> V34Rank4LeafInput {
    let mut residual_diagonal = [0.0_f32; DIMENSIONS];
    residual_diagonal[0] = 1.0;
    residual_diagonal[1] = 3.0;
    let mut directions = [[0.0_f32; DIMENSIONS]; 4];
    directions[0][0] = 1.0;
    directions[1][0] = 0.5;
    directions[1][1] = 0.5;
    V34Rank4LeafInput {
        leaf_ordinal: 0,
        group_ordinal: 0,
        logical_start: 0,
        population: 2,
        mean: [0.0; DIMENSIONS],
        residual_diagonal,
        eigenvalues: [2.0, 1.0, 0.0, 0.0],
        directions,
    }
}

#[test]
fn v34_rank4_score_matches_hand_reduced_nonorthogonal_covariance() {
    // Break caught: the rank-four score treats persisted directions as
    // orthogonal, omits the residual diagonal, or changes reduction order.
    let generation = build_v34_rank4_generation(vec![coherent_leaf()]).unwrap();
    let leaf = &generation.leaves()[0];
    let mut query = [0.0_f32; DIMENSIONS];
    query[0] = 2.0;
    query[1] = -1.0;
    let expected = 11.5 - (2.0_f64 * 2.0_f64.ln()).sqrt() * 103.5_f64.sqrt();
    assert_eq!(score_v34_rank4_leaf(leaf, &query).unwrap(), expected);

    query[0] = f32::NAN;
    assert!(score_v34_rank4_leaf(leaf, &query).is_err());
}

#[test]
fn v34_rank4_generation_recomputes_authority_and_logical_coverage() {
    // Break caught: persisted cached moments, component signs, or logical
    // intervals are trusted instead of independently authenticated.
    let first = coherent_leaf();
    let mut second = coherent_leaf();
    second.leaf_ordinal = 1;
    second.group_ordinal = 1;
    second.logical_start = 2;
    let generation = build_v34_rank4_generation(vec![first.clone(), second]).unwrap();
    assert_eq!(generation.leaves().len(), 2);
    assert_eq!(generation.logical_rows(), 4);
    assert_eq!(generation.group_count(), 2);

    assert_eq!(generation.leaves()[0].trace(), 6.5);
    assert_eq!(generation.leaves()[0].trace_square(), 21.25);
    assert!(generation.leaves()[0].spectral_bound() >= 5.5);

    let mutations: [fn(&mut V34Rank4LeafInput); 9] = [
        |leaf: &mut V34Rank4LeafInput| leaf.leaf_ordinal = 1,
        |leaf: &mut V34Rank4LeafInput| leaf.logical_start = 1,
        |leaf: &mut V34Rank4LeafInput| leaf.population = 0,
        |leaf: &mut V34Rank4LeafInput| leaf.group_ordinal = 1,
        |leaf: &mut V34Rank4LeafInput| leaf.residual_diagonal[0] = -1.0,
        |leaf: &mut V34Rank4LeafInput| leaf.eigenvalues.swap(0, 1),
        |leaf: &mut V34Rank4LeafInput| leaf.directions[0][0] = -1.0,
        |leaf: &mut V34Rank4LeafInput| leaf.mean[0] = f32::INFINITY,
        |leaf: &mut V34Rank4LeafInput| leaf.directions[0][0] = f32::NAN,
    ];
    for mutate in mutations {
        let mut invalid = first.clone();
        mutate(&mut invalid);
        assert!(build_v34_rank4_generation(vec![invalid]).is_err());
    }
}

#[test]
fn v34_rank4_projection_locks_100m_memory_and_work_bounds() {
    // Break caught: the resident projection drops the retiring generation,
    // confuses decimal and binary units, or hides directional score work.
    let projection = project_v34_serving_memory(414_100, 69_905).unwrap();
    assert_eq!(projection.rank_four_numeric_bytes, 960_712_000);
    assert_eq!(projection.leaf_identity_bytes, 9_938_400);
    assert_eq!(projection.cached_scalar_bytes, 13_251_200);
    assert_eq!(projection.tree_bytes, 35_791_360);
    assert_eq!(projection.active_generation_cap_bytes, 1_040 * 1_048_576);
    assert_eq!(projection.retiring_generation_cap_bytes, 1_040 * 1_048_576);
    assert_eq!(projection.shared_cache_cap_bytes, 128 * 1_048_576);
    assert_eq!(projection.runtime_cap_bytes, 160 * 1_048_576);
    assert_eq!(projection.query_workspace_cap_bytes, 512 * 1_048_576);
    assert_eq!(projection.unallocated_headroom_bytes, 96 * 1_048_576);
    assert_eq!(projection.admission_budget_bytes, 2_976 * 1_048_576);
    assert_eq!(projection.hard_limit_bytes, 3_072 * 1_048_576);
    assert!(projection.admission_budget_bytes < projection.hard_limit_bytes);
    assert_eq!(projection.exhaustive_directional_macs, 414_100 * 4 * 96);

    assert!(project_v34_serving_memory(u64::MAX, 1).is_err());
    assert!(project_v34_serving_memory(1, u64::MAX).is_err());
}

#[test]
fn v34_rank4_generation_canonicalizes_signed_zero_before_scoring() {
    // Break caught: semantically equal leaves retain different f32/f64 bytes,
    // producing different Arrow identities across languages.
    let mut leaf = coherent_leaf();
    leaf.mean[2] = -0.0;
    leaf.residual_diagonal[2] = -0.0;
    leaf.eigenvalues[2] = -0.0;
    leaf.directions[2][2] = -0.0;
    let generation = build_v34_rank4_generation(vec![leaf]).unwrap();
    let canonical = &generation.leaves()[0];
    assert!(!canonical.mean()[2].is_sign_negative());
    assert!(!canonical.residual_diagonal()[2].is_sign_negative());
    assert!(!canonical.eigenvalues()[2].is_sign_negative());
    assert!(!canonical.directions()[2][2].is_sign_negative());
}

#[test]
fn v34_rank4_zero_covariance_and_singleton_have_euclidean_score() {
    // Break caught: a zero radicand or ln(1) is treated as an invalid score.
    let input = V34Rank4LeafInput {
        leaf_ordinal: 0,
        group_ordinal: 0,
        logical_start: 0,
        population: 1,
        mean: [0.0; DIMENSIONS],
        residual_diagonal: [0.0; DIMENSIONS],
        eigenvalues: [0.0; 4],
        directions: [[0.0; DIMENSIONS]; 4],
    };
    let generation = build_v34_rank4_generation(vec![input]).unwrap();
    let mut query = [0.0; DIMENSIONS];
    query[0] = 3.0;
    assert_eq!(generation.leaves()[0].population_factor(), 0.0);
    assert_eq!(
        score_v34_rank4_leaf(&generation.leaves()[0], &query).unwrap(),
        9.0
    );
}

#[test]
fn v34_rank4_spectral_bound_rounds_outward() {
    // Break caught: a round-to-nearest sum falls below the exact covariance
    // norm, allowing a later hierarchical lower bound to prune a valid leaf.
    let mut input = coherent_leaf();
    input.residual_diagonal = [0.0; DIMENSIONS];
    input.residual_diagonal[0] = 1.0;
    input.eigenvalues = [f32::EPSILON, 0.0, 0.0, 0.0];
    input.directions = [[0.0; DIMENSIONS]; 4];
    input.directions[0][0] = 1.0;
    let generation = build_v34_rank4_generation(vec![input]).unwrap();
    let exact = 1.0 + f64::from(f32::EPSILON);
    assert!(generation.leaves()[0].spectral_bound() >= exact);
}
