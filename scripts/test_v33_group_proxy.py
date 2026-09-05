"""Contracts for the metadata-only V33 group-prototype falsifier."""

import math
import subprocess
import tempfile
import unittest
from pathlib import Path


class GroupProxyTests(unittest.TestCase):
    def test_shape_scores_bind_scalar_diagonal_and_split_controls(self):
        from scripts.v33_group_proxy import (
            LeafShape,
            rank_shape_groups,
            score_leaf_shape,
        )

        zero = (0.0,) * 96
        mean = (2.0,) + zero[1:]
        diagonal = (1.0,) + zero[1:]
        leaf = LeafShape(
            ordinal=0,
            group_ordinal=0,
            population=2,
            mean=mean,
            diagonal_variance=diagonal,
            scalar_moment=1.0,
            split_centers=((1.0,) + zero[1:], (3.0,) + zero[1:]),
            scalar_split_selected=True,
        )
        query = (4.0,) + zero[1:]
        factor = math.sqrt(2.0 * math.log(2.0))
        self.assertEqual(score_leaf_shape(leaf, query, "centroid"), 4.0)
        self.assertEqual(score_leaf_shape(leaf, query, "split-centroid"), 1.0)
        self.assertEqual(
            score_leaf_shape(leaf, query, "scalar-moment"),
            5.0 - factor * math.sqrt(18.0 / 96.0),
        )
        self.assertEqual(
            score_leaf_shape(leaf, query, "diagonal-moment"),
            5.0 - factor * math.sqrt(18.0),
        )
        other = LeafShape(
            ordinal=1,
            group_ordinal=1,
            population=1,
            mean=(5.0,) + zero[1:],
            diagonal_variance=zero,
            scalar_moment=0.0,
            split_centers=((5.0,) + zero[1:], (5.0,) + zero[1:]),
            scalar_split_selected=False,
        )
        self.assertEqual(rank_shape_groups((other, leaf), query, "diagonal-moment"), (0, 1))

    def test_driver_supports_direct_script_execution(self):
        repository = Path(__file__).resolve().parents[1]
        driver = repository / "scripts/run_v33_group_proxy.py"
        with tempfile.TemporaryDirectory() as temporary:
            missing = str(Path(temporary) / "missing")
            Path(missing).write_bytes(b"")
            command = [
                "uv",
                "run",
                "--offline",
                "--python",
                "3.12",
                "--with-requirements",
                "scripts/requirements-format-bench.txt",
                "python",
                str(driver),
            ]
            from scripts.run_v33_group_proxy import EXPECTED_DIGESTS

            for role in EXPECTED_DIGESTS:
                command.extend(("--" + role.replace("_", "-"), missing))
            command.extend(
                (
                    "--output",
                    str(Path(temporary) / "result.json"),
                    "--execute-group-proxy",
                )
            )
            completed = subprocess.run(
                command,
                cwd=repository,
                capture_output=True,
                check=False,
                text=True,
            )
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("V33 leaves byte authority differs", completed.stderr)
            self.assertNotIn("ModuleNotFoundError", completed.stderr)

    def test_driver_cli_requires_each_local_role_once_and_explicit_execution(self):
        from scripts.run_v33_group_proxy import EXPECTED_DIGESTS, parse_args

        with tempfile.TemporaryDirectory() as temporary:
            output = str(Path(temporary) / "result.json")
            values = []
            for role in EXPECTED_DIGESTS:
                values.extend(("--" + role.replace("_", "-"), role + ".bin"))
            values.extend(("--output", output, "--execute-group-proxy"))
            parsed = parse_args(values)
            self.assertEqual(parsed.output, output)
            with self.assertRaises(SystemExit):
                parse_args(values[:-1])
            with self.assertRaises(SystemExit):
                parse_args(values + (values[:2]))
            with self.assertRaises(SystemExit):
                parse_args(values + ["--bucket", "forbidden"])

    def test_materializer_binds_dense_groups_to_each_parent_once(self):
        from scripts.v33_group_proxy import ParentSummary, materialize_group_proxies

        parents = (
            ParentSummary(0, 10, (0.0, 0.0)),
            ParentSummary(1, 20, (0.0, 2.0)),
            ParentSummary(2, 30, (8.0, 0.0)),
            ParentSummary(3, 40, (8.0, 2.0)),
        )
        groups = materialize_group_proxies(
            ((0, 30, (0, 1)), (1, 70, (2, 3))),
            parents,
            prototype_count=3,
            iterations=10,
        )
        self.assertEqual(tuple(group.rows for group in groups), (30, 70))
        self.assertEqual(tuple(len(group.prototypes) for group in groups), (2, 2))
        with self.assertRaises(ValueError):
            materialize_group_proxies(
                ((0, 30, (0, 1)), (1, 80, (2, 3))),
                parents,
                prototype_count=3,
                iterations=10,
            )
        with self.assertRaises(ValueError):
            materialize_group_proxies(
                ((0, 30, (0, 1)), (1, 70, (1, 3))),
                parents,
                prototype_count=3,
                iterations=10,
            )

    def test_materializer_preserves_sparse_populated_parent_ordinals(self):
        from scripts.v33_group_proxy import ParentSummary, materialize_group_proxies

        parents = (
            ParentSummary(1, 20, (0.0, 2.0)),
            ParentSummary(3, 40, (8.0, 2.0)),
        )
        groups = materialize_group_proxies(
            ((0, 60, (1, 3)),),
            parents,
            prototype_count=3,
            iterations=10,
        )
        self.assertEqual(groups[0].rows, 60)
        self.assertEqual(len(groups[0].prototypes), 2)

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
