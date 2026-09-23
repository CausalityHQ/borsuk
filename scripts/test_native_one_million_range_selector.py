"""Fixed adjacent group range planner and independent small cohort."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.launch_native_geometric_layout_spot import SourceArchiveIdentity
from scripts.launch_native_one_million_selector_spot import (
    build_launch_specs,
    build_plan,
    worker_script,
)
from scripts.native_one_million_group_selector import Group
from scripts.native_one_million_page_selector import build_page_selector
from scripts.native_one_million_range_selector_cell import (
    run_construct,
    run_evaluate,
    run_validate,
)
from scripts.native_one_million_range_selector_evaluation import (
    evaluate_range_selector,
    plan_group_ranges,
)
from scripts.test_native_one_million_group_selector import _fixture as source_fixture
from scripts.test_native_one_million_selector_evaluation import (
    _fixture as development_fixture,
)
from scripts.validate_native_one_million_range_selector import validate_range_selector


class OneMillionRangeSelectorTests(unittest.TestCase):
    def test_admits_neighbor_and_bridge_after_get_cap(self) -> None:
        groups = tuple(Group("base", i, i, i + 1, 1, 204) for i in range(8))
        chosen, intervals, gets, bytes_used = plan_group_ranges(
            groups, (0, 2, 4, 6, 1, 3, 5, 7), maximum_gets=4, maximum_bytes=204 * 7,
        )
        self.assertEqual(chosen, (0, 2, 4, 6, 1, 3, 5))
        self.assertEqual(intervals, (("base", 0, 7),))
        self.assertEqual(gets, 1)
        self.assertEqual(bytes_used, 204 * 7)

    def test_role_boundary_never_merges(self) -> None:
        groups = (Group("base", 0, 0, 1, 1, 204), Group("delta", 0, 0, 1, 1, 204))
        chosen, intervals, gets, bytes_used = plan_group_ranges(groups, (0, 1), maximum_gets=2, maximum_bytes=408)
        self.assertEqual(chosen, (0, 1))
        self.assertEqual(intervals, (("base", 0, 1), ("delta", 0, 1)))
        self.assertEqual((gets, bytes_used), (2, 408))

    def test_byte_only_plan_ignores_intermediate_get_count(self) -> None:
        groups = tuple(Group("base", i, i, i + 1, 1, 204) for i in range(70))
        ranked = tuple(range(0, 70, 2)) + tuple(range(1, 70, 2))
        chosen, intervals, gets, bytes_used = plan_group_ranges(
            groups, ranked, maximum_gets=len(groups), maximum_bytes=70 * 204,
        )
        self.assertEqual(chosen, ranked)
        self.assertEqual(intervals, (("base", 0, 70),))
        self.assertEqual((gets, bytes_used), (1, 70 * 204))

    def test_byte_only_diagnostic_replays_without_get_gate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identities = source_fixture(root, base_rows=256)
            artifact = build_page_selector(root, root, identities)
            _, development = development_fixture(root)
            result = evaluate_range_selector(
                artifact, root / "queries.parquet", root / "truth.parquet", root,
                development, query_count=1, byte_only=True,
            )
            validation = validate_range_selector(
                root, root, root, root, identities, development,
                query_count=1, byte_only=True,
            )
            self.assertEqual(result["metrics"], validation["metrics"])
            self.assertEqual(result["decision"], "score-ranked-byte-ceiling-feasible")

    def test_pq96_projection_replays_exact_width(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identities = source_fixture(root, base_rows=256)
            artifact = build_page_selector(root, root, identities)
            _, development = development_fixture(root)
            result = evaluate_range_selector(
                artifact, root / "queries.parquet", root / "truth.parquet", root,
                development, query_count=1, pq96=True,
            )
            validation = validate_range_selector(
                root, root, root, root, identities, development,
                query_count=1, pq96=True,
            )
            self.assertEqual(result["metrics"], validation["metrics"])
            self.assertEqual(result["metrics"]["row_bytes"], 96)
            self.assertEqual(result["decision"], "pq96-locality-projection-feasible")

    def test_fixed_source_seal_matches_previous_screen(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identities = source_fixture(root, base_rows=256)
            _, development = development_fixture(root)
            self.assertEqual(set(development), {"queries", "truth"})
            self.assertEqual(set(identities), {"source", "generation", "base", "delta", "router"})

    def test_small_cell_and_range_spot_script(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identities = source_fixture(root, base_rows=256)
            with (
                patch("scripts.native_one_million_range_selector_cell.SOURCE_IDENTITIES", identities),
                patch("scripts.native_one_million_range_selector_cell.check_prior_page_artifact"),
            ):
                run_construct(root)
                self.assertFalse((root / "queries.parquet").exists())
                _, development = development_fixture(root)
                with patch("scripts.native_one_million_range_selector_cell.DEVELOPMENT_IDENTITIES", development):
                    run_evaluate(root, root, query_count=1)
                    run_validate(root, root, query_count=1)
        commit = "12" * 20
        plan = build_plan(
            source_commit=commit,
            source_archive=SourceArchiveIdentity("s3://frozen/source.tar.gz", "34" * 32, 1234),
            requirements_sha256="56" * 32,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/native-one-million-range-selector/"
                + commit + "/runs/relaion-1m-dev1000-a0001"
            ),
            selector_kind="range",
        )
        script = worker_script(plan)
        syntax = subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True, check=False)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        self.assertLess(len(script.encode()), 16_384)
        self.assertIn("scripts.native_one_million_range_selector_cell construct", script)
        self.assertIn("borsuk-one-million-range-selector-terminal-v1", script)
        self.assertTrue(all(len(spec["ClientToken"]) <= 64 for spec in build_launch_specs(plan)))


if __name__ == "__main__":
    unittest.main()
