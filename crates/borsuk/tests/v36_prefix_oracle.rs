//! Fast-fail scalar contracts for the V36 sequential prefix oracle.

use borsuk::{allocate_v36_hamilton_postings, select_v36_closure_owners};

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
