//! V35 exact complete-prefix routing contracts.

use borsuk::{
    V35Dimensions, V35GroupStorage, V35LeafPatchBuildRequest, V35RouteBudget, V35RoutePrefix,
    V35RoutingGenerationLimits, bound_v35_node, build_v35_leaf_patch_arm, build_v35_route_tree,
    build_v35_routing_generation, build_v35_srht, decode_v35_generation_arrow,
    encode_v35_generation_arrow, exhaustive_v35_route, hierarchical_v35_route,
    project_v35_route_tree_bytes, score_v35_leaf_patch_arm,
};

const MIB: u64 = 1_048_576;

fn digest(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

fn fixture(
    routing: u16,
    means: &[(u32, f32)],
) -> (borsuk::V35RoutingGeneration, Vec<V35GroupStorage>, Vec<f64>) {
    let dimensions = V35Dimensions {
        source: u32::from(routing),
        routing,
    };
    let projection = build_v35_srht(dimensions, 91).unwrap();
    let mut logical_start = 0_u64;
    let mut expected_scores = Vec::with_capacity(means.len());
    let mut arms = Vec::with_capacity(means.len());
    for (leaf, (group, mean)) in means.iter().copied().enumerate() {
        let mut rows = vec![vec![mean; usize::from(routing)]; 2];
        rows[1][0] += 0.25;
        let request = V35LeafPatchBuildRequest {
            assignment_max: logical_start + 1,
            assignment_min: logical_start,
            dimensions,
            group_ordinal: group,
            leaf_ordinal: u32::try_from(leaf).unwrap(),
            logical_start,
            omitted_energies: vec![0.0; 2],
            projected_rows: rows,
        };
        let arm = build_v35_leaf_patch_arm(&request, 1).unwrap();
        expected_scores
            .push(score_v35_leaf_patch_arm(&arm, &vec![0.0; routing.into()], 0.0).unwrap());
        arms.push(arm);
        logical_start += 2;
    }
    let generation =
        build_v35_routing_generation(&projection, "deep-image", &digest(0x31), arms).unwrap();
    let group_count = means.last().map_or(0, |(group, _)| group + 1);
    let groups = (0..group_count)
        .map(|group| {
            let rows = 2 * means.iter().filter(|(value, _)| *value == group).count() as u64;
            V35GroupStorage::new(group, rows, 100 + 10 * u64::from(group)).unwrap()
        })
        .collect();
    (generation, groups, expected_scores)
}

type RouteEvidence = (Vec<(u32, u32, u64)>, Option<(u32, u32)>, u64, u64);

fn route_evidence(route: &V35RoutePrefix) -> RouteEvidence {
    (
        route
            .selected_groups()
            .iter()
            .map(|group| {
                (
                    group.group_ordinal(),
                    group.leaf_ordinal(),
                    group.score().to_bits(),
                )
            })
            .collect(),
        route
            .overflow()
            .map(|group| (group.group_ordinal(), group.leaf_ordinal())),
        route.selected_rows(),
        route.selected_code_bytes(),
    )
}

#[test]
fn v35_route_exhaustive_uses_group_minima_stable_order_and_complete_prefix() {
    // Break caught: duplicate leaves overwrite rather than minimize, ties use
    // discovery order, or a group is partially admitted.
    let (generation, groups, scores) = fixture(64, &[(0, 3.0), (1, 1.0), (1, 0.5), (2, 2.0)]);
    let route = exhaustive_v35_route(
        &generation,
        &[0.0; 64],
        0.0,
        &groups,
        V35RouteBudget::new(3, 8, 330).unwrap(),
    )
    .unwrap();
    let expected_group_one = scores[1].min(scores[2]);
    let mut expected = [(0, scores[0]), (1, expected_group_one), (2, scores[3])];
    expected.sort_by(|left, right| left.1.total_cmp(&right.1).then(left.0.cmp(&right.0)));
    assert_eq!(
        route
            .selected_groups()
            .iter()
            .map(|group| (group.group_ordinal(), group.score().to_bits()))
            .collect::<Vec<_>>(),
        expected
            .iter()
            .map(|(group, score)| (*group, score.to_bits()))
            .collect::<Vec<_>>()
    );
    assert_eq!(route.selected_rows(), 8);
    assert_eq!(route.selected_code_bytes(), 330);
    assert!(route.overflow().is_none());
}

#[test]
fn v35_route_exhaustive_stops_at_first_overflow_without_group_skipping() {
    // Break caught: an over-budget group is skipped so a later cheaper group
    // can be admitted, changing the exact complete score prefix.
    let (generation, groups, _) = fixture(64, &[(0, 0.0), (1, 1.0), (2, 2.0)]);
    for budget in [
        V35RouteBudget::new(1, 6, 1_000).unwrap(),
        V35RouteBudget::new(3, 2, 1_000).unwrap(),
        V35RouteBudget::new(3, 6, 150).unwrap(),
    ] {
        let route = exhaustive_v35_route(&generation, &[0.0; 64], 0.0, &groups, budget).unwrap();
        assert_eq!(route.selected_groups().len(), 1);
        assert_eq!(route.selected_groups()[0].group_ordinal(), 0);
        assert_eq!(route.overflow().unwrap().group_ordinal(), 1);
        assert_eq!(route.selected_rows(), 2);
        assert_eq!(route.selected_code_bytes(), 100);
    }
    assert!(V35RouteBudget::new(65, 262_144, 8 * MIB).is_err());
    assert!(V35RouteBudget::new(64, 262_145, 8 * MIB).is_err());
    assert!(V35RouteBudget::new(64, 262_144, 8 * MIB + 1).is_err());
}

#[test]
fn v35_route_hierarchy_matches_exhaustive_for_every_routing_width() {
    // Break caught: a width-specific bound prunes a better leaf or changes
    // selected groups, scores, rows, bytes, or first overflow.
    for routing in [64, 128, 192] {
        let (generation, groups, _) = fixture(routing, &[(0, -1.0), (1, 1.0), (2, 0.5), (3, 2.0)]);
        let query = vec![0.0; usize::from(routing)];
        let budget = V35RouteBudget::new(3, 6, 1_000).unwrap();
        let exhaustive = exhaustive_v35_route(&generation, &query, 0.0, &groups, budget).unwrap();
        let tree = build_v35_route_tree(&generation).unwrap();
        let hierarchical =
            hierarchical_v35_route(&generation, &tree, &query, 0.0, &groups, budget).unwrap();
        assert_eq!(route_evidence(&hierarchical), route_evidence(&exhaustive));
        assert!(hierarchical.exact_leaf_evaluations() <= generation.leaf_count());
        assert!(hierarchical.bound_evaluations() > 0);
    }
}

fn tree_fixture(
    routing: u16,
    count: u32,
    patch_count: u8,
) -> (borsuk::V35RoutingGeneration, Vec<borsuk::V35LeafPatchArm>) {
    let dimensions = V35Dimensions {
        source: u32::from(routing),
        routing,
    };
    let projection = build_v35_srht(dimensions, 191).unwrap();
    let mut arms = Vec::new();
    let mut logical_start = 0_u64;
    for leaf in 0..count {
        let population = u32::from(patch_count) + leaf % 3;
        let mut rows = vec![vec![0.0; usize::from(routing)]; population as usize];
        for (row, values) in rows.iter_mut().enumerate() {
            values[0] = leaf as f32 / 7.0;
            values[1] = (leaf % 5) as f32 - 2.0;
            values[2] = row as f32 * 0.125;
        }
        let request = V35LeafPatchBuildRequest {
            assignment_max: logical_start + u64::from(population) - 1,
            assignment_min: logical_start,
            dimensions,
            group_ordinal: leaf / 3,
            leaf_ordinal: leaf,
            logical_start,
            omitted_energies: (0..population).map(|row| f64::from(row) * 0.25).collect(),
            projected_rows: rows,
        };
        arms.push(build_v35_leaf_patch_arm(&request, patch_count).unwrap());
        logical_start += u64::from(population);
    }
    let generation =
        build_v35_routing_generation(&projection, "deep-image", &digest(0x41), arms.clone())
            .unwrap();
    (generation, arms)
}

#[test]
fn v35_route_tree_is_bounded_deterministic_and_outward_for_every_descendant() {
    // Break caught: the provisional exhaustive facade has no real tree, or a
    // quantized node bound can exceed one descendant's exact decoded score.
    let (generation, arms) = tree_fixture(64, 33, 1);
    let first = build_v35_route_tree(&generation).unwrap();
    let second = build_v35_route_tree(&generation).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.format(), "borsuk-v35-route-tree-v1");
    assert_eq!(first.authority_digest(), second.authority_digest());
    assert_ne!(first.authority_digest(), [0; 32]);
    assert_eq!(first.root(), 0);
    assert_eq!(first.leaf_count(), 33);
    assert!(first.nodes().len() > 1);
    assert_eq!(
        project_v35_route_tree_bytes(69_905, 192).unwrap(),
        22_369_600
    );

    let mut covered = Vec::new();
    let query = vec![0.375; 64];
    let omitted = 0.5;
    for (node_ordinal, node) in first.nodes().iter().enumerate() {
        assert!(node.encoded_numeric_bytes() <= 64 + 128);
        let descendants = first
            .descendant_leaf_ordinals(u32::try_from(node_ordinal).unwrap())
            .unwrap();
        assert!(!descendants.is_empty());
        if node.is_terminal() {
            assert!(descendants.len() <= 16);
            covered.extend_from_slice(descendants);
        }
        let bound = bound_v35_node(
            &first,
            u32::try_from(node_ordinal).unwrap(),
            &query,
            omitted,
        )
        .unwrap();
        for leaf in descendants {
            let exact =
                score_v35_leaf_patch_arm(&arms[usize::try_from(*leaf).unwrap()], &query, omitted)
                    .unwrap();
            assert!(
                bound <= exact,
                "node={node_ordinal} leaf={leaf} bound={bound} exact={exact}"
            );
        }
    }
    covered.sort_unstable();
    assert_eq!(covered, (0..33).collect::<Vec<_>>());
}

#[test]
fn v35_route_hierarchy_certifies_a_narrow_prefix_without_scanning_every_leaf() {
    // Break caught: a nominal tree still evaluates every leaf before returning
    // the first exact complete storage group.
    let means = (0..64_u32)
        .map(|leaf| (leaf, leaf as f32 * 100.0))
        .collect::<Vec<_>>();
    let (generation, groups, _) = fixture(64, &means);
    let budget = V35RouteBudget::new(1, 2, 8 * MIB).unwrap();
    let exact = exhaustive_v35_route(&generation, &[0.0; 64], 0.0, &groups, budget).unwrap();
    let tree = build_v35_route_tree(&generation).unwrap();
    let routed =
        hierarchical_v35_route(&generation, &tree, &[0.0; 64], 0.0, &groups, budget).unwrap();
    assert_eq!(route_evidence(&routed), route_evidence(&exact));
    assert!(routed.exact_leaf_evaluations() < generation.leaf_count());
}

#[test]
fn v35_route_uses_the_authenticated_columnar_generation_without_row_owners() {
    // Break caught: routing works only on the construction Vec and requires a
    // second per-leaf owner after strict Arrow decode.
    let (generation, groups, _) = fixture(64, &[(0, 0.0), (1, 1.0), (2, 2.0)]);
    let projection = build_v35_srht(generation.dimensions(), 91).unwrap();
    let (bytes, identity) =
        encode_v35_generation_arrow(&generation, "active-routing", "s3://v35/routing.arrow")
            .unwrap();
    let decoded = decode_v35_generation_arrow(
        &bytes,
        &identity,
        &projection,
        "deep-image",
        &digest(0x31),
        V35RoutingGenerationLimits {
            maximum_encoded_bytes: bytes.len() as u64,
            maximum_leaf_count: 3,
            maximum_numeric_bytes: 3 * (8 * 64 + 128),
            maximum_peak_codec_bytes: 2 * bytes.len() as u64,
        },
    )
    .unwrap();
    assert!(decoded.is_columnar_serving());
    let budget = V35RouteBudget::new(2, 4, 1_000).unwrap();
    let construction_route =
        exhaustive_v35_route(&generation, &[0.0; 64], 0.0, &groups, budget).unwrap();
    let decoded_tree = build_v35_route_tree(&decoded).unwrap();
    let decoded_route =
        hierarchical_v35_route(&decoded, &decoded_tree, &[0.0; 64], 0.0, &groups, budget).unwrap();
    assert_eq!(
        route_evidence(&decoded_route),
        route_evidence(&construction_route)
    );
}

#[test]
fn v35_route_tree_rejects_a_different_generation_with_matching_outer_metadata() {
    // Break caught: tree authority binds only dimensions/source/projection and
    // accepts different leaf payloads under the same outer metadata.
    let (first, groups, _) = fixture(64, &[(0, 0.0), (1, 1.0), (2, 2.0)]);
    let (different, _, _) = fixture(64, &[(0, 0.5), (1, 1.5), (2, 2.5)]);
    let tree = build_v35_route_tree(&first).unwrap();
    assert!(
        hierarchical_v35_route(
            &different,
            &tree,
            &[0.0; 64],
            0.0,
            &groups,
            V35RouteBudget::new(2, 4, 1_000).unwrap(),
        )
        .is_err()
    );
}

#[test]
fn v35_route_tree_bound_survives_two_patch_and_numeric_adversaries() {
    // Break caught: the lower bound assumes one patch, drops complement-energy
    // safety, or rounds inward for subnormal or huge-finite queries.
    for patch_count in [1, 2] {
        let (generation, arms) = tree_fixture(64, 17, patch_count);
        let tree = build_v35_route_tree(&generation).unwrap();
        for (query, omitted) in [
            (vec![0.0; 64], 0.0),
            (vec![f64::from_bits(1); 64], 0.5),
            (vec![0.375; 64], 1.0),
            (vec![1.0e150; 64], 0.25),
        ] {
            for node_ordinal in 0..tree.nodes().len() {
                let bound =
                    bound_v35_node(&tree, u32::try_from(node_ordinal).unwrap(), &query, omitted)
                        .unwrap();
                for leaf in tree
                    .descendant_leaf_ordinals(u32::try_from(node_ordinal).unwrap())
                    .unwrap()
                {
                    let exact = score_v35_leaf_patch_arm(
                        &arms[usize::try_from(*leaf).unwrap()],
                        &query,
                        omitted,
                    )
                    .unwrap();
                    assert!(
                        bound <= exact,
                        "patches={patch_count} query0={} omitted={omitted} node={node_ordinal} leaf={leaf} bound={bound} exact={exact}",
                        query[0],
                    );
                }
            }
        }
    }
}

#[test]
fn v35_route_tree_bound_uses_identical_quantized_center_arithmetic() {
    // Break caught: construction computes code*scale in f32 while traversal
    // widens first and computes it in f64, leaving the stored radius inward.
    let (generation, arms) = tree_fixture(64, 33, 1);
    let tree = build_v35_route_tree(&generation).unwrap();
    for seed in 0..128_u64 {
        let query = (0..64_u64)
            .map(|dimension| {
                let bits = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(dimension.wrapping_mul(1442695040888963407));
                (i32::try_from(bits % 2001).unwrap() - 1000) as f64 * 0.0137
            })
            .collect::<Vec<_>>();
        for node_ordinal in 0..tree.nodes().len() {
            let bound =
                bound_v35_node(&tree, u32::try_from(node_ordinal).unwrap(), &query, 0.75).unwrap();
            for leaf in tree
                .descendant_leaf_ordinals(u32::try_from(node_ordinal).unwrap())
                .unwrap()
            {
                let exact =
                    score_v35_leaf_patch_arm(&arms[usize::try_from(*leaf).unwrap()], &query, 0.75)
                        .unwrap();
                assert!(
                    bound <= exact,
                    "seed={seed} node={node_ordinal} leaf={leaf}"
                );
            }
        }
    }
}

#[test]
fn v35_route_tree_bound_uses_outward_omitted_energy_interval() {
    // Break caught: dropping the complement-energy interval is safe for order
    // but destroys pruning for high-dimensional energy outside the projection.
    let dimensions = V35Dimensions {
        source: 384,
        routing: 64,
    };
    let projection = build_v35_srht(dimensions, 291).unwrap();
    let arms = (0..17_u32)
        .map(|leaf| {
            build_v35_leaf_patch_arm(
                &V35LeafPatchBuildRequest {
                    assignment_max: u64::from(leaf) * 2 + 1,
                    assignment_min: u64::from(leaf) * 2,
                    dimensions,
                    group_ordinal: leaf,
                    leaf_ordinal: leaf,
                    logical_start: u64::from(leaf) * 2,
                    omitted_energies: vec![4.0, 4.0],
                    projected_rows: vec![vec![0.0; 64]; 2],
                },
                1,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let generation =
        build_v35_routing_generation(&projection, "deep-image", &digest(0x51), arms.clone())
            .unwrap();
    let tree = build_v35_route_tree(&generation).unwrap();
    let bound = bound_v35_node(&tree, tree.root(), &[0.0; 64], 0.0).unwrap();
    let exact = score_v35_leaf_patch_arm(&arms[0], &[0.0; 64], 0.0).unwrap();
    assert!(bound > 3.9, "bound={bound}");
    assert!(bound <= exact);
}

#[test]
fn v35_route_hierarchy_expands_ambiguous_ties_and_returns_after_all_groups() {
    // Break caught: ambiguous equal bounds are certified early, or an all-fit
    // query continues traversal after every complete group is established.
    let (generation, groups, _) = fixture(64, &[(0, 1.0); 32]);
    let budget = V35RouteBudget::new(1, 64, 8 * MIB).unwrap();
    let exact = exhaustive_v35_route(&generation, &[0.0; 64], 0.0, &groups, budget).unwrap();
    let tree = build_v35_route_tree(&generation).unwrap();
    let routed =
        hierarchical_v35_route(&generation, &tree, &[0.0; 64], 0.0, &groups, budget).unwrap();
    assert_eq!(route_evidence(&routed), route_evidence(&exact));
    assert_eq!(routed.exact_leaf_evaluations(), generation.leaf_count());
    assert!(routed.overflow().is_none());
}
