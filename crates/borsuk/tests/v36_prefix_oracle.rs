//! Fast-fail scalar contracts for the V36 sequential prefix oracle.

use borsuk::allocate_v36_hamilton_postings;

#[test]
fn v36_hamilton_postings_preserve_every_nonempty_run() {
    // Break caught: proportional allocation rounds tiny non-empty runs to zero,
    // making their rows unreachable before any scorer can evaluate them.
    let allocation = allocate_v36_hamilton_postings(&[999_997, 1, 1, 1], 123).unwrap();

    assert_eq!(allocation, [120, 1, 1, 1]);
    assert_eq!(allocation.iter().sum::<u32>(), 123);
}
