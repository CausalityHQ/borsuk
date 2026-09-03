use borsuk::{Pq4Match, merge_pq4_shard_matches};

#[test]
fn pq4_public_api_preserves_deterministic_cross_shard_identity() {
    // Break caught: the production crate stops exposing the exact-row API or loses the shard
    // ordinal needed for deterministic global ties.
    let match_for = |shard_ordinal, id: u8| Pq4Match {
        id: vec![id],
        squared_distance: 0.25,
        source_ordinal: 9,
        shard_ordinal,
    };
    assert_eq!(
        merge_pq4_shard_matches(vec![vec![match_for(8, 2)], vec![match_for(3, 1)]], 2).unwrap(),
        vec![match_for(3, 1), match_for(8, 2)]
    );
}
