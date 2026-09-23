"""Focused geometric group-order construction, replay and Spot wiring."""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

from scripts.launch_native_geometric_layout_spot import SourceArchiveIdentity
from scripts.launch_native_one_million_selector_spot import (
    artifact_names,
    build_plan,
    worker_script,
)
from scripts.native_one_million_geometric_group_order import (
    build_geometric_group_order,
    geometric_group_order,
    read_geometric_group_order,
)
from scripts.native_one_million_geometric_group_order_evaluation import (
    evaluate_geometric_group_order,
)
from scripts.native_one_million_group_selector import Group
from scripts.native_one_million_page_selector import (
    PageSelectorArtifact,
    build_page_selector,
    read_page_selector,
)
from scripts.native_one_million_range_selector_evaluation import evaluate_range_selector
from scripts.test_native_one_million_group_selector import _fixture as source_fixture
from scripts.test_native_one_million_selector_evaluation import (
    _fixture as development_fixture,
)
from scripts.validate_native_one_million_geometric_group_order import (
    independently_order_groups,
    validate_geometric_group_order,
)


class GeometricGroupOrderTests(unittest.TestCase):
    def test_role_local_geometry_reorders_whole_groups(self) -> None:
        groups = tuple(
            Group(role, ordinal, ordinal, ordinal + 1, 1, 208)
            for role, values in (("base", (0, 10, 1)), ("delta", (20, 10)))
            for ordinal, _ in enumerate(values)
        )
        centers = np.zeros((5, 768), dtype="<f2")
        centers[:, 0] = [0, 10, 1, 20, 10]
        artifact = PageSelectorArtifact(
            groups, centers, np.arange(5, dtype="<u4"),
            np.arange(5, dtype="<i8"), np.arange(5, dtype="<u4"), {},
        )
        expected = (0, 2, 1, 3, 4)
        self.assertEqual(geometric_group_order(artifact), expected)
        self.assertEqual(independently_order_groups(centers, groups), expected)

    def test_small_source_boundary_and_independent_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = source_fixture(root, base_rows=256)
            build_page_selector(root, root, source)
            artifact = read_page_selector(root, source)
            build_geometric_group_order(artifact, root)
            _, development = development_fixture(root)
            order = read_geometric_group_order(artifact, root / "layout.json")
            self.assertEqual(set(order), set(range(len(artifact.groups))))
            control = root / "control"
            evaluate_range_selector(
                artifact, root / "queries.parquet", root / "truth.parquet",
                control, development, query_count=1, row_bytes=96,
            )
            baseline = json.loads((control / "evidence.json").read_bytes())
            b = baseline["metrics"]
            fixed = (
                b["mean_recall_at_100_ppm"], b["p05_recall_at_100_ppm"],
                b["mean_recall_at_10_ppm"],
                sum(item["hits_at_100"] < 90 for item in baseline["samples"]),
            )
            with (
                patch("scripts.native_one_million_geometric_group_order_evaluation.BASELINE", fixed),
                patch("scripts.validate_native_one_million_geometric_group_order.BASELINE", fixed),
            ):
                result = evaluate_geometric_group_order(
                    artifact, root / "layout.json", root / "queries.parquet",
                    root / "truth.parquet", root, development, query_count=1,
                )
                validation = validate_geometric_group_order(
                    root, root, root, source, development, query_count=1,
                )
            self.assertEqual(result["metrics"], validation["metrics"])
            self.assertEqual(result["baseline_metrics"], validation["baseline_metrics"])
            tampered = json.loads((root / "layout.json").read_bytes())
            tampered["physical_to_logical"] = list(reversed(tampered["physical_to_logical"]))
            (root / "layout.json").write_text(json.dumps(tampered))
            with self.assertRaises(ValueError):
                read_geometric_group_order(artifact, root / "layout.json")

    def test_layout_spot_script_seals_tenth_artifact(self) -> None:
        commit = "12" * 20
        plan = build_plan(
            source_commit=commit,
            source_archive=SourceArchiveIdentity("s3://frozen/source.tar.gz", "34" * 32, 1234),
            requirements_sha256="56" * 32,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/native-one-million-layout-selector/"
                + commit + "/runs/relaion-1m-dev1000-a0001"
            ),
            selector_kind="layout",
        )
        script = worker_script(plan)
        syntax = subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True, check=False)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        self.assertLess(len(script.encode()), 16_384)
        self.assertIn("scripts.native_one_million_geometric_group_order_cell construct", script)
        self.assertIn("publish_artifact layout.json", script)
        self.assertIn("MAXIMUM_RSS_BYTES - 67108864", script)
        self.assertIn("Maximum sampled process-tree RSS (bytes)", script)
        self.assertIn("--body terminal.json --if-none-match '*'", script)
        self.assertIn('cmp -s terminal.json readback/terminal.json', script)
        self.assertEqual(len(artifact_names("layout")), 10)


if __name__ == "__main__":
    unittest.main()
