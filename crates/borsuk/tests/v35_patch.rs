//! V35 compact projected leaf-patch contracts.

use borsuk::{
    V35Dimensions, V35LeafPatchBuildRequest, build_v35_equal_byte_centroid_control,
    build_v35_leaf_patch, build_v35_leaf_patch_arm, project_v35_leaf_moment_bytes,
    score_v35_equal_byte_centroid_control, score_v35_leaf_patch, score_v35_leaf_patch_arm,
};

fn request(rows: Vec<Vec<f32>>, omitted: Vec<f64>) -> V35LeafPatchBuildRequest {
    V35LeafPatchBuildRequest {
        assignment_max: 17,
        assignment_min: 3,
        dimensions: V35Dimensions {
            source: 384,
            routing: 64,
        },
        group_ordinal: 2,
        leaf_ordinal: 7,
        logical_start: 1_024,
        omitted_energies: omitted,
        projected_rows: rows,
    }
}

#[test]
fn v35_patch_quantizes_once_and_recomputes_cached_moments_from_decoded_planes() {
    // Break caught: construction retains decoded f32 planes, trusts pre-round
    // moments, or exceeds the exact 8*M+128 resident byte envelope.
    let rows = (0..8)
        .map(|row| {
            (0..64)
                .map(|dimension| ((row * 29 + dimension * 17) as f32 - 70.0) / 31.0)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let patch = build_v35_leaf_patch(&request(
        rows,
        vec![0.0, 0.25, 1.0, 2.25, 4.0, 6.25, 9.0, 12.25],
    ))
    .unwrap();

    assert_eq!(patch.leaf_ordinal(), 7);
    assert_eq!(patch.group_ordinal(), 2);
    assert_eq!(patch.logical_start(), 1_024);
    assert_eq!(patch.population(), 8);
    assert_eq!(patch.assignment_bounds(), (3, 17));
    assert_eq!(patch.encoded_numeric_bytes(), 8 * 64 + 128);
    assert_eq!(patch.mean_codes().len(), 64);
    assert_eq!(patch.residual_codes().len(), 64);
    assert_eq!(patch.direction_codes().len(), 6 * 64);
    assert_eq!(patch.direction_scales().len(), 6);
    assert_eq!(patch.weights().len(), 6);
    assert!(
        patch
            .weights()
            .iter()
            .all(|value| value.is_finite() && *value >= 0.0)
    );
    assert!(patch.trace().is_finite() && patch.trace() >= 0.0);
    assert!(patch.trace_square().is_finite() && patch.trace_square() >= 0.0);
    assert!(patch.spectral_bound().is_finite() && patch.spectral_bound() >= 0.0);
    let residual = patch
        .residual_codes()
        .iter()
        .map(|code| f64::from(*code) * f64::from(patch.residual_scale()))
        .collect::<Vec<_>>();
    let directions = (0..6)
        .map(|component| {
            patch.direction_codes()[component * 64..(component + 1) * 64]
                .iter()
                .map(|code| f64::from(*code) * f64::from(patch.direction_scales()[component]))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut covariance = vec![0.0_f64; 64 * 64];
    for left in 0..64 {
        covariance[left * 64 + left] = residual[left];
        for right in 0..64 {
            for (component, direction) in directions.iter().enumerate() {
                covariance[left * 64 + right] +=
                    f64::from(patch.weights()[component]) * direction[left] * direction[right];
            }
        }
    }
    let decoded_trace = (0..64)
        .map(|dimension| covariance[dimension * 64 + dimension])
        .sum::<f64>();
    let decoded_trace_square = covariance.iter().map(|value| value * value).sum::<f64>();
    assert!((patch.trace() - decoded_trace).abs() <= decoded_trace.abs().max(1.0) * 1e-12);
    assert!(
        (patch.trace_square() - decoded_trace_square).abs()
            <= decoded_trace_square.abs().max(1.0) * 1e-12
    );
    assert_eq!(patch.omitted_energy(), 4.375);
    assert_eq!(project_v35_leaf_moment_bytes(192).unwrap(), 148_224);
    assert_eq!(project_v35_leaf_moment_bytes(64).unwrap(), 16_640);
    assert!(project_v35_leaf_moment_bytes(0).is_err());
    assert!(project_v35_leaf_moment_bytes(193).is_err());
}

#[test]
fn v35_patch_singleton_score_matches_literal_projected_plus_complement_formula() {
    // Break caught: complement energy is mixed into projected covariance or a
    // singleton takes an undefined/singular covariance path.
    let mut row = vec![0.0_f32; 64];
    row[0] = 127.0;
    row[1] = -127.0;
    let patch = build_v35_leaf_patch(&request(vec![row], vec![9.0])).unwrap();
    let mut query = vec![0.0_f64; 64];
    query[0] = 128.0;
    query[1] = -125.0;
    let score = score_v35_leaf_patch(&patch, &query, 25.0).unwrap();
    // Projected squared distance is 1^2+2^2=5. Complement term is (5-3)^2=4.
    assert!((score - 9.0).abs() <= 1e-12, "score={score}");
    assert!(score_v35_leaf_patch(&patch, &query[..63], 25.0).is_err());
    assert!(score_v35_leaf_patch(&patch, &query, -1.0).is_err());
}

#[test]
fn v35_patch_rejects_nonfinite_shape_and_leaf_bound_violations() {
    let coherent = request(vec![vec![0.0; 64]], vec![0.0]);
    let mut empty = coherent.clone();
    empty.projected_rows.clear();
    empty.omitted_energies.clear();
    assert!(build_v35_leaf_patch(&empty).is_err());

    let mut too_many = coherent.clone();
    too_many.projected_rows = vec![vec![0.0; 64]; 257];
    too_many.omitted_energies = vec![0.0; 257];
    assert!(build_v35_leaf_patch(&too_many).is_err());

    let mut wrong_width = coherent.clone();
    wrong_width.projected_rows[0].pop();
    assert!(build_v35_leaf_patch(&wrong_width).is_err());

    let mut nonfinite = coherent.clone();
    nonfinite.projected_rows[0][63] = f32::NAN;
    assert!(build_v35_leaf_patch(&nonfinite).is_err());

    let mut negative_omitted = coherent.clone();
    negative_omitted.omitted_energies[0] = -1.0;
    assert!(build_v35_leaf_patch(&negative_omitted).is_err());

    let mut bad_bounds = coherent;
    bad_bounds.assignment_min = 18;
    assert!(build_v35_leaf_patch(&bad_bounds).is_err());
}

#[test]
fn v35_patch_rounds_ties_to_even_saturates_extrema_and_preserves_negative_scores() {
    let mut row = vec![0.0_f32; 64];
    row[0] = 127.0;
    row[1] = 0.5;
    row[2] = 1.5;
    row[3] = -127.0;
    let singleton = build_v35_leaf_patch(&request(vec![row], vec![0.0])).unwrap();
    assert_eq!(singleton.mean_scale(), 1.0);
    assert_eq!(&singleton.mean_codes()[..4], &[127, 0, 2, -127]);
    assert_eq!(singleton.population_factor(), 0.0);

    let rows = (0_usize..32)
        .map(|row| {
            let mut values = vec![0.0_f32; 64];
            values[0] = if row.is_multiple_of(2) { -1.0 } else { 1.0 };
            values
        })
        .collect::<Vec<_>>();
    let patch = build_v35_leaf_patch(&request(rows, vec![0.0; 32])).unwrap();
    let score = score_v35_leaf_patch(&patch, &[0.0; 64], 0.0).unwrap();
    assert!(
        score.is_finite() && score < 0.0,
        "negative lower-tail score was clamped"
    );
    assert!(patch.residual_scale().is_finite());
}

fn next_up(value: f64) -> f64 {
    if value == 0.0 {
        f64::from_bits(1)
    } else if value.is_finite() && value > 0.0 {
        f64::from_bits(value.to_bits() + 1)
    } else {
        value
    }
}

fn multiply_nonnegative_up(left: f64, right: f64) -> f64 {
    if left == 0.0 || right == 0.0 {
        0.0
    } else {
        next_up(left * right)
    }
}

fn add_nonnegative_up(left: f64, right: f64) -> f64 {
    if right == 0.0 {
        left
    } else {
        next_up(left + right)
    }
}

#[test]
fn v35_patch_rounds_omitted_energy_once_and_spectral_bound_outward() {
    // Break caught: upstream f32 rounding destroys the required ordered-f64
    // mean, or an ordinary rounded sum understates the decoded covariance.
    let mut rows = vec![vec![0.0_f32; 64]; 2];
    rows[0][0] = -1.0;
    rows[1][0] = 1.0;
    let patch = build_v35_leaf_patch(&request(
        rows,
        vec![1.0 + 2.0_f64.powi(-24), 1.0 + 2.0_f64.powi(-23)],
    ))
    .unwrap();
    assert_eq!(patch.omitted_energy(), 1.000_000_1_f32);

    let residual = patch
        .residual_codes()
        .iter()
        .map(|code| f64::from(*code) * f64::from(patch.residual_scale()))
        .collect::<Vec<_>>();
    let mut expected = residual.iter().copied().fold(0.0_f64, f64::max);
    for component in 0..6 {
        let start = component * 64;
        let scale = f64::from(patch.direction_scales()[component]);
        let norm = patch.direction_codes()[start..start + 64]
            .iter()
            .fold(0.0_f64, |sum, code| {
                let value = f64::from(*code) * scale;
                add_nonnegative_up(sum, multiply_nonnegative_up(value.abs(), value.abs()))
            });
        let term = multiply_nonnegative_up(f64::from(patch.weights()[component]), norm);
        expected = add_nonnegative_up(expected, term);
    }
    assert!(
        patch.spectral_bound() >= expected,
        "bound={} outward={expected}",
        patch.spectral_bound()
    );
}

#[test]
fn v35_patch_repeated_eigenspace_is_canonical_under_row_permutation() {
    // Break caught: a repeated covariance eigenspace inherits arbitrary
    // LAPACK eigenvector orientation or source-row arrival order.
    let mut rows = Vec::new();
    for dimension in 0..8 {
        let mut positive = vec![0.0_f32; 64];
        positive[dimension] = 1.0;
        rows.push(positive.clone());
        positive[dimension] = -1.0;
        rows.push(positive);
    }
    let forward = build_v35_leaf_patch(&request(rows.clone(), vec![0.0; rows.len()])).unwrap();
    rows.reverse();
    let reverse = build_v35_leaf_patch(&request(rows.clone(), vec![0.0; rows.len()])).unwrap();
    assert_eq!(forward.mean_codes(), reverse.mean_codes());
    assert_eq!(forward.direction_codes(), reverse.direction_codes());
    assert_eq!(forward.direction_scales(), reverse.direction_scales());
    assert_eq!(forward.weights(), reverse.weights());
}

#[test]
fn v35_patch_finite_subnormal_plane_quantizes_to_zero() {
    // Break caught: a positive finite plane whose f32 scale underflows is
    // rejected instead of being represented by the canonical zero plane.
    let mut row = vec![0.0_f32; 64];
    row[0] = f32::from_bits(1);
    let patch = build_v35_leaf_patch(&request(vec![row], vec![0.0])).unwrap();
    assert_eq!(patch.mean_scale(), 0.0);
    assert!(patch.mean_codes().iter().all(|code| *code == 0));
}

#[test]
fn v35_patch_two_mode_arm_and_equal_byte_centroids_share_exact_budget() {
    // Break caught: a two-patch arm gets hidden bytes, a different leaf
    // population, or a cheaper centroid comparator.
    let rows = (0..32)
        .map(|ordinal| {
            let mut row = vec![0.0_f32; 64];
            row[0] = if ordinal < 16 { -20.0 } else { 20.0 };
            row[1] = (ordinal % 16) as f32 / 16.0;
            row
        })
        .collect::<Vec<_>>();
    let build = request(rows, vec![0.0; 32]);
    let one = build_v35_leaf_patch_arm(&build, 1).unwrap();
    let two = build_v35_leaf_patch_arm(&build, 2).unwrap();
    let one_centroids = build_v35_equal_byte_centroid_control(&build, 1).unwrap();
    let two_centroids = build_v35_equal_byte_centroid_control(&build, 2).unwrap();

    assert_eq!(one.patch_count(), 1);
    assert_eq!(two.patch_count(), 2);
    assert_eq!(one.encoded_bytes(), 8 * 64 + 128);
    assert_eq!(two.encoded_bytes(), 2 * (8 * 64 + 128));
    assert_eq!(one_centroids.centroid_count(), 2);
    assert_eq!(two_centroids.centroid_count(), 4);
    assert_eq!(one_centroids.encoded_bytes(), one.encoded_bytes());
    assert_eq!(two_centroids.encoded_bytes(), two.encoded_bytes());
    assert_eq!(two.total_population(), 32);

    for coordinate in [-20.0, 20.0] {
        let mut query = vec![0.0_f64; 64];
        query[0] = coordinate;
        assert!(
            score_v35_leaf_patch_arm(&one, &query, 0.0)
                .unwrap()
                .is_finite()
        );
        assert!(
            score_v35_leaf_patch_arm(&two, &query, 0.0)
                .unwrap()
                .is_finite()
        );
        assert!(score_v35_equal_byte_centroid_control(&two_centroids, &query).unwrap() >= 0.0);
    }
}

#[test]
fn v35_patch_control_split_is_deterministic_and_rejects_budget_drift() {
    // Break caught: row arrival order changes the variance split, or controls
    // compare different admitted bytes/dimensions.
    let rows = (0_usize..16)
        .map(|ordinal| {
            let mut row = vec![0.0_f32; 64];
            row[0] = if ordinal.is_multiple_of(2) { -4.0 } else { 4.0 };
            row[1] = if ordinal < 8 { -4.0 } else { 4.0 };
            row
        })
        .collect::<Vec<_>>();
    let forward = request(rows.clone(), vec![0.0; rows.len()]);
    let mut reversed_rows = rows;
    reversed_rows.reverse();
    let reverse = request(reversed_rows, vec![0.0; 16]);
    let left = build_v35_equal_byte_centroid_control(&forward, 2).unwrap();
    let right = build_v35_equal_byte_centroid_control(&reverse, 2).unwrap();
    assert_eq!(left.centers(), right.centers());

    assert!(build_v35_leaf_patch_arm(&forward, 0).is_err());
    assert!(build_v35_leaf_patch_arm(&forward, 3).is_err());
    assert!(build_v35_equal_byte_centroid_control(&forward, 0).is_err());
    assert!(build_v35_equal_byte_centroid_control(&forward, 3).is_err());
}
