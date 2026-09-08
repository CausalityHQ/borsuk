//! Fast-fail scalar contracts for the V36 sequential prefix oracle.

use borsuk::{
    Result, V35ProjectionBackend, V36CenteredProjectionBlockVisitor, V36CenteredProjectionSource,
    V36CenteredProjectionTrainingSpec, V36CenteredSampleRole, V36GeometryStop, admit_v36_geometry,
    allocate_v36_hamilton_postings, build_v36_srht192_control, project_v35_query_scalar,
    project_v35_query_simd, select_v36_closure_owners, train_v36_centered_subspace,
};

struct TestProjectionSource {
    corpus_rows: Vec<Vec<f32>>,
    reservoir_rows: Vec<Vec<f32>>,
    block_rows: usize,
    mutate_reservoir: bool,
}

impl V36CenteredProjectionSource for TestProjectionSource {
    fn scan(
        &mut self,
        role: V36CenteredSampleRole,
        visitor: &mut V36CenteredProjectionBlockVisitor<'_>,
    ) -> Result<()> {
        let rows = match role {
            V36CenteredSampleRole::CorpusMean => &self.corpus_rows,
            V36CenteredSampleRole::GeometryReservoir => &self.reservoir_rows,
        };
        for (block_index, block) in rows.chunks(self.block_rows).enumerate() {
            let start = block_index * self.block_rows;
            let ordinals = (start..start + block.len())
                .map(|ordinal| ordinal as u64)
                .collect::<Vec<_>>();
            let mut values = block.iter().flatten().copied().collect::<Vec<_>>();
            if self.mutate_reservoir
                && role == V36CenteredSampleRole::GeometryReservoir
                && start + block.len() == rows.len()
            {
                *values.last_mut().unwrap() = 31.0;
            }
            visitor(&ordinals, &values)?;
        }
        Ok(())
    }
}

fn centered_training_spec(
    source_dimensions: usize,
    retained_dimensions: usize,
    corpus_sha256: &str,
    reservoir_sha256: &str,
    energy_dimensions: Vec<usize>,
) -> V36CenteredProjectionTrainingSpec {
    V36CenteredProjectionTrainingSpec {
        source_dimensions,
        retained_dimensions,
        corpus_rows: 4,
        corpus_sha256: corpus_sha256.to_owned(),
        reservoir_rows: 4,
        reservoir_sha256: reservoir_sha256.to_owned(),
        maximum_block_rows: 3,
        energy_dimensions,
    }
}

#[test]
fn v36_hamilton_postings_preserve_every_nonempty_run() {
    // Break caught: proportional allocation rounds tiny non-empty runs to zero,
    // making their rows unreachable before any scorer can evaluate them.
    let allocation = allocate_v36_hamilton_postings(&[999_997, 1, 1, 1], 123).unwrap();

    assert_eq!(allocation, [120, 1, 1, 1]);
    assert_eq!(allocation.iter().sum::<u32>(), 123);
}

#[test]
fn v36_closure_owners_reject_redundant_centroids_without_losing_distinct_regions() {
    // Break caught: closure either retains a redundant near-duplicate centroid
    // or drops a geometrically distinct owner that covers a boundary row.
    let centroids = vec![
        vec![1.0, 0.0],
        vec![-1.0, 0.0],
        vec![1.0, 0.1],
        vec![0.0, 1.02],
    ];

    let owners = select_v36_closure_owners(&[0.0, 0.0], &centroids, 0.05, 8).unwrap();

    assert_eq!(owners, [0, 1, 3]);
}

#[test]
fn v36_geometry_admission_names_the_first_construction_gate() {
    // Break caught: an over-replicated or badly skewed geometry reaches query
    // evaluation instead of failing at its first registered construction gate.
    let admitted = admit_v36_geometry(&[100; 100], &[300; 100], 100).unwrap();
    assert_eq!(admitted.mean_replication_ppm, 3_000_000);
    assert_eq!(admitted.primary_p99, 100);
    assert_eq!(admitted.stored_p99, 300);
    assert_eq!(admitted.stop, None);

    let cases = [
        (
            vec![100; 100],
            [vec![300; 99], vec![301]].concat(),
            V36GeometryStop::MeanReplication,
        ),
        (
            [vec![100; 98], vec![201, 201]].concat(),
            [vec![300; 98], vec![301, 301]].concat(),
            V36GeometryStop::PrimaryP99,
        ),
        (
            [vec![100; 99], vec![401]].concat(),
            [vec![300; 99], vec![401]].concat(),
            V36GeometryStop::PrimaryMaximum,
        ),
        (
            vec![100; 100],
            [vec![290; 98], vec![601, 601]].concat(),
            V36GeometryStop::StoredP99,
        ),
        (
            vec![100; 100],
            [vec![291; 99], vec![801]].concat(),
            V36GeometryStop::StoredMaximum,
        ),
    ];
    for (primary, stored, expected) in cases {
        assert_eq!(
            admit_v36_geometry(&primary, &stored, 100).unwrap().stop,
            Some(expected)
        );
    }
}

#[test]
fn v36_geometry_admission_rejects_threshold_overflow() {
    // Break caught: saturating threshold arithmetic silently turns malformed
    // oversized authority into an effectively unbounded admission gate.
    assert!(admit_v36_geometry(&[1], &[1], u64::MAX).is_err());
}

#[test]
fn v36_srht192_control_reuses_the_fused_generic_projection() {
    // Break caught: V36 silently changes dimensions/seed or grows a separate
    // scalar projection whose coordinates drift from the fused query path.
    let projection = build_v36_srht192_control().unwrap();
    assert_eq!(projection.dimensions().source, 768);
    assert_eq!(projection.dimensions().routing, 192);
    assert_eq!(projection.seed(), 36);
    assert_eq!(projection.algorithm(), "srht-prefix-orthonormalized-v1");

    let query = (0..768)
        .map(|dimension| ((dimension % 31) as f32 - 15.0) / 32.0)
        .collect::<Vec<_>>();
    let scalar = project_v35_query_scalar(&projection, &query).unwrap();
    let simd = project_v35_query_simd(&projection, &query).unwrap();
    assert_eq!(scalar.coordinates(), simd.coordinates());
    assert_eq!(scalar.complement_energy(), simd.complement_energy());
    assert_ne!(simd.backend(), V35ProjectionBackend::ScalarControl);
}

#[test]
fn v36_centered_covariance_streams_translation_invariant_full_spectrum_evidence() {
    // Break caught: block-local reductions, an uncentered second moment, or an
    // unauthenticated second pass changes projection identity or energy.
    let rows = vec![
        vec![11.0, 22.0, 30.0],
        vec![9.0, 18.0, 30.0],
        vec![11.0, 18.0, 30.0],
        vec![9.0, 22.0, 30.0],
    ];
    let spec = centered_training_spec(
        3,
        2,
        "bb2dc686dcdbe09129f2fc2eb285e3391410d5f4c7a2f530213cb11a99706396",
        "ccbd798f5fb1cecb3409b14e5f87c86310cf597078129f3fbf4ca46b595550f4",
        vec![1, 2, 3],
    );
    let one_row = train_v36_centered_subspace(
        &spec,
        &mut TestProjectionSource {
            corpus_rows: rows.clone(),
            reservoir_rows: rows.clone(),
            block_rows: 1,
            mutate_reservoir: false,
        },
    )
    .unwrap();
    let three_rows = train_v36_centered_subspace(
        &spec,
        &mut TestProjectionSource {
            corpus_rows: rows.clone(),
            reservoir_rows: rows.clone(),
            block_rows: 3,
            mutate_reservoir: false,
        },
    )
    .unwrap();

    assert_eq!(one_row, three_rows);
    assert_eq!(one_row.mean(), [10.0, 20.0, 30.0]);
    assert_eq!(
        one_row.mean_sha256(),
        "f321334d8b936bd1e31bb89f100c8acdf6f246c9b2e94ddc01a451c4cee48295"
    );
    assert_eq!(
        one_row.covariance_sha256(),
        "adc8b1865b02a45137632026311b2cd7133f3eea3b6e863734ff4b57577e72c1"
    );
    assert_eq!(one_row.eigenvalues(), [4.0, 1.0, 0.0]);
    assert_eq!(
        one_row.retained_energy_ppm(),
        [800_000, 1_000_000, 1_000_000]
    );
    assert_eq!(one_row.basis_source_major(), [0.0, 1.0, 1.0, 0.0, 0.0, 0.0]);
    assert!(one_row.max_eigenpair_relative_residual() <= 1e-10);
    assert!(one_row.reconstruction_relative_error() <= 1e-10);

    let changed = train_v36_centered_subspace(
        &spec,
        &mut TestProjectionSource {
            corpus_rows: rows.clone(),
            reservoir_rows: rows,
            block_rows: 3,
            mutate_reservoir: true,
        },
    );
    assert!(changed.is_err());
}

#[test]
fn v36_centered_covariance_canonicalizes_repeated_eigenspace_before_truncation() {
    // Break caught: sign fixing alone cannot make an arbitrary solver rotation
    // deterministic when an equal-eigenvalue cluster crosses retained M.
    let rows = vec![
        vec![1.0, 0.0, 0.0],
        vec![-1.0, 0.0, 0.0],
        vec![0.0, 1.0, 0.0],
        vec![0.0, -1.0, 0.0],
    ];
    let spec = centered_training_spec(
        3,
        1,
        "212cc8493b13f629db89c6ae99b19a0ca35c2f24110619ba913ddb5e85b6f96d",
        "3a7447a4d88cf24e38a8c31774d1e4806710230af1421cabcd1b3c219d026d1b",
        vec![1, 2],
    );

    let projection = train_v36_centered_subspace(
        &spec,
        &mut TestProjectionSource {
            corpus_rows: rows.clone(),
            reservoir_rows: rows,
            block_rows: 2,
            mutate_reservoir: false,
        },
    )
    .unwrap();

    assert_eq!(projection.eigenvalues(), [0.5, 0.5, 0.0]);
    assert_eq!(projection.basis_source_major(), [1.0, 0.0, 0.0]);
    assert_eq!(projection.retained_energy_ppm(), [500_000, 1_000_000]);
}

#[test]
fn v36_centered_covariance_uses_corpus_mean_for_off_diagonal_reservoir_moments() {
    // Break caught: silently recentering on the reservoir changes both the
    // off-diagonal covariance and the registered retained spectrum.
    let corpus_rows = vec![vec![10.0, 20.0, 30.0]; 4];
    let reservoir_rows = vec![
        vec![11.0, 22.0, 30.0],
        vec![11.0, 22.0, 30.0],
        vec![13.0, 26.0, 30.0],
        vec![13.0, 26.0, 30.0],
    ];
    let spec = centered_training_spec(
        3,
        1,
        "d2272823d9d7d32a6eb95601229f084f90fa1c6b5e7a9707cc114bbd337c5cc7",
        "45279d95a26c02e856d1ee6883c042c705d333bff928908dd3576381de4b0fcf",
        vec![1, 2, 3],
    );

    let projection = train_v36_centered_subspace(
        &spec,
        &mut TestProjectionSource {
            corpus_rows,
            reservoir_rows,
            block_rows: 2,
            mutate_reservoir: false,
        },
    )
    .unwrap();

    assert_eq!(projection.mean(), [10.0, 20.0, 30.0]);
    assert_eq!(projection.eigenvalues(), [25.0, 0.0, 0.0]);
    let expected = [1.0_f32 / 5.0_f32.sqrt(), 2.0_f32 / 5.0_f32.sqrt(), 0.0];
    for (actual, expected) in projection.basis_source_major().iter().zip(expected) {
        assert!((*actual - expected).abs() <= f32::EPSILON);
    }
    assert_eq!(
        projection.retained_energy_ppm(),
        [1_000_000, 1_000_000, 1_000_000]
    );
}

#[test]
fn v36_centered_covariance_canonicalizes_non_axis_aligned_repeated_plane() {
    // Break caught: a repeated non-coordinate eigenspace retains nalgebra's
    // arbitrary rotated columns rather than projected-coordinate axes.
    let rows = vec![
        vec![1.0, 1.0, 0.0, 0.0],
        vec![-1.0, -1.0, 0.0, 0.0],
        vec![0.0, 0.0, 1.0, 1.0],
        vec![0.0, 0.0, -1.0, -1.0],
    ];
    let spec = centered_training_spec(
        4,
        1,
        "41a58172c4bb4b9db89eaa1dd535405828d3e5246bcc8a744f3322d78e2516b8",
        "30b6128aab5cb66541a75e8729ed87fa59265c43023a3916f3a0ddf4fc48c9d6",
        vec![1, 2, 4],
    );

    let projection = train_v36_centered_subspace(
        &spec,
        &mut TestProjectionSource {
            corpus_rows: rows.clone(),
            reservoir_rows: rows,
            block_rows: 2,
            mutate_reservoir: false,
        },
    )
    .unwrap();

    assert_eq!(projection.eigenvalues(), [1.0, 1.0, 0.0, 0.0]);
    let inverse_sqrt_two = 1.0_f32 / 2.0_f32.sqrt();
    let expected = [inverse_sqrt_two, inverse_sqrt_two, 0.0, 0.0];
    for (actual, expected) in projection.basis_source_major().iter().zip(expected) {
        assert!((*actual - expected).abs() <= f32::EPSILON);
    }
    assert_eq!(
        projection.retained_energy_ppm(),
        [500_000, 1_000_000, 1_000_000]
    );
}

#[test]
fn v36_centered_covariance_rejects_zero_signal_numerically() {
    // Break caught: an all-constant reservoir publishes an arbitrary nullspace
    // as if it were an admitted principal projection.
    let rows = vec![vec![1.0, 2.0, 3.0]; 4];
    let spec = centered_training_spec(
        3,
        1,
        "1bd398622bda1155d329595b3d25989f7fec098520e24255de924b0fd9f3ac2a",
        "1a523488674ff0f8c55ca80b75aa593f41dfca79b423da3083c808b7212688c4",
        vec![1],
    );

    assert!(
        train_v36_centered_subspace(
            &spec,
            &mut TestProjectionSource {
                corpus_rows: rows.clone(),
                reservoir_rows: rows,
                block_rows: 2,
                mutate_reservoir: false,
            },
        )
        .is_err()
    );
}

#[test]
fn v36_centered_covariance_rejects_unbounded_workspace_before_scanning() {
    // Break caught: malformed dimensions allocate an attacker-sized dense
    // covariance before the registered training workspace gate runs.
    struct PanicSource;
    impl V36CenteredProjectionSource for PanicSource {
        fn scan(
            &mut self,
            _role: V36CenteredSampleRole,
            _visitor: &mut V36CenteredProjectionBlockVisitor<'_>,
        ) -> Result<()> {
            panic!("workspace authority must reject before source access")
        }
    }
    let spec = V36CenteredProjectionTrainingSpec {
        source_dimensions: 2_000,
        retained_dimensions: 1,
        corpus_rows: 1,
        corpus_sha256: "0".repeat(64),
        reservoir_rows: 1,
        reservoir_sha256: "1".repeat(64),
        maximum_block_rows: 1,
        energy_dimensions: vec![1],
    };

    assert!(train_v36_centered_subspace(&spec, &mut PanicSource).is_err());
}
