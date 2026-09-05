"""Contracts for the metadata-only V33 group-prototype falsifier."""

import math
import unittest


class GroupProxyTests(unittest.TestCase):
    def test_three_prototypes_are_deterministic_and_population_weighted(self):
        from scripts.v33_group_proxy import ParentSummary, build_group_prototypes

        parents = (
            ParentSummary(0, 10, (0.0, 0.0)),
            ParentSummary(1, 10, (0.0, 2.0)),
            ParentSummary(2, 40, (8.0, 0.0)),
            ParentSummary(3, 40, (8.0, 2.0)),
        )
        first = build_group_prototypes(parents, prototype_count=3, iterations=10)
        second = build_group_prototypes(parents, prototype_count=3, iterations=10)

        self.assertEqual(first, second)
        self.assertEqual(len(first), 3)
        self.assertEqual(first[0], (8.0, 1.0))
        self.assertEqual(first[1:], ((0.0, 2.0), (0.0, 0.0)))
        self.assertEqual(
            build_group_prototypes(parents[:1], prototype_count=3, iterations=10),
            ((0.0, 0.0),),
        )

    def test_rank_and_prefix_use_total_order_and_complete_group_rows(self):
        from scripts.v33_group_proxy import GroupProxy, rank_groups, select_group_prefix

        groups = (
            GroupProxy(0, 70, ((0.0, 0.0),)),
            GroupProxy(1, 40, ((1.0, 0.0),)),
            GroupProxy(2, 50, ((1.0, 0.0),)),
            GroupProxy(3, 20, ((4.0, 0.0),)),
        )
        ranked = rank_groups(groups, (1.0, 0.0))
        self.assertEqual(ranked, (1, 2, 0, 3))
        self.assertEqual(
            select_group_prefix(groups, ranked, row_limit=100, group_limit=3),
            (1, 2),
        )
        self.assertEqual(
            select_group_prefix(groups, ranked, row_limit=200, group_limit=2),
            (1, 2),
        )

    def test_owner_evaluation_rejects_any_miss_or_invalid_authority(self):
        from scripts.v33_group_proxy import GroupProxy, evaluate_owner_inclusion

        groups = (
            GroupProxy(0, 60, ((0.0, 0.0),)),
            GroupProxy(1, 60, ((1.0, 0.0),)),
            GroupProxy(2, 60, ((2.0, 0.0),)),
        )
        queries = (
            ((0.0, 0.0), (0, 1)),
            ((2.0, 0.0), (2,)),
        )
        result = evaluate_owner_inclusion(
            groups,
            queries,
            row_limit=120,
            group_limit=2,
        )
        self.assertTrue(result.passed)
        self.assertEqual(result.included_owners, 3)
        self.assertEqual(result.total_owners, 3)
        self.assertEqual(result.perfect_queries, 2)
        self.assertEqual(result.maximum_rows, 120)

        missed = evaluate_owner_inclusion(
            groups,
            (((0.0, 0.0), (0, 2)),),
            row_limit=120,
            group_limit=2,
        )
        self.assertFalse(missed.passed)
        self.assertEqual(missed.included_owners, 1)
        with self.assertRaises(ValueError):
            evaluate_owner_inclusion(
                groups,
                (((math.nan, 0.0), (0,)),),
                row_limit=120,
                group_limit=2,
            )
        with self.assertRaises(ValueError):
            evaluate_owner_inclusion(
                groups,
                (((0.0, 0.0), (3,)),),
                row_limit=120,
                group_limit=2,
            )


if __name__ == "__main__":
    unittest.main()
