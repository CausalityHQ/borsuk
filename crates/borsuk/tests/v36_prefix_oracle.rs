//! Fast-fail scalar contracts for the V36 sequential prefix oracle.

use borsuk::{
    V36GeometryStop, admit_v36_geometry, allocate_v36_hamilton_postings, select_v36_closure_owners,
};

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
