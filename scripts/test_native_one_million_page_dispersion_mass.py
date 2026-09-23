"""Focused source moments, fixed mass score and Spot wiring."""

from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.launch_native_geometric_layout_spot import SourceArchiveIdentity
from scripts.launch_native_one_million_selector_spot import (
    artifact_names,
    build_plan,
    worker_script,
)
from scripts.native_one_million_group_selector import Group, _source_schema
from scripts.native_one_million_page_dispersion_mass import (
    PageMoments,
    build_page_moments,
    rank_mass_groups,
    read_page_moments,
)
from scripts.native_one_million_page_dispersion_mass_evaluation import (
    evaluate_page_dispersion_mass,
)
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
from scripts.v97_row_width_screen import _canonical_json_bytes
from scripts.validate_native_one_million_page_dispersion_mass import (
    independently_rebuild_moments,
    validate_page_dispersion_mass,
)


class PageDispersionMassTests(unittest.TestCase):
    def test_two_pass_source_variance_has_nonzero_and_zero_pages(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            groups = (
                Group("base", 0, 0, 1, 2, 408),
                Group("delta", 0, 0, 1, 2, 408),
            )
            page_groups = np.asarray([0, 1], dtype="<u4")
            rows = np.zeros((4, 768), dtype=np.float32)
            rows[:, 0] = [-1, 1, 3, 3]
            table = pa.Table.from_arrays([
                pa.array([0, 1, 2, 3], type=pa.uint64()),
                pa.FixedSizeListArray.from_arrays(
                    pa.array(rows.reshape(-1), type=pa.float32()),
                    type=pa.list_(pa.field("item", pa.float32(), nullable=False), 768),
                ),
            ], schema=_source_schema())
            pq.write_table(table, root / "source.parquet")
            artifact = PageSelectorArtifact(
                groups, np.zeros((2, 768), dtype="<f2"), page_groups,
                np.arange(4, dtype="<i8"), np.asarray([0, 0, 1, 1], dtype="<u4"),
                {"page_order_sha256": "synthetic"},
            )
            membership = (groups, {0: 0, 1: 0, 2: 1, 3: 1}, page_groups, "synthetic")
            with patch("scripts.native_one_million_page_dispersion_mass._page_membership", return_value=membership):
                built = build_page_moments(root, artifact, {})
            self.assertEqual(built.counts.tolist(), [2, 2])
            self.assertEqual(built.means[:, 0].tolist(), [0.0, 3.0])
            self.assertEqual(built.variances[:, 0].tolist(), [1.0, 0.0])
            self.assertTrue(np.all(built.variances[:, 1:] == 0))
            with patch("scripts.validate_native_one_million_page_dispersion_mass._page_membership", return_value=membership):
                counts, means, variances = independently_rebuild_moments(root, artifact, {})
            self.assertTrue(np.array_equal(counts, built.counts))
            self.assertTrue(np.array_equal(means, built.means))
            self.assertTrue(np.array_equal(variances, built.variances))

    def test_positive_dispersion_changes_mass_rank_with_mixed_zero_scales(self) -> None:
        groups = tuple(Group("base", i, i, i + 1, 100, 20_008) for i in range(3))
        centers = np.zeros((3, 768), dtype="<f2")
        centers[:, 0] = [0, 1, 2]
        artifact = PageSelectorArtifact(
            groups, centers, np.arange(3, dtype="<u4"),
            np.arange(300, dtype="<i8"), np.repeat(np.arange(3, dtype="<u4"), 100), {},
        )
        means = centers.astype(np.float64)
        variances = np.zeros((3, 768), dtype=np.float64)
        variances[2, 0] = 1.0
        counts = np.asarray([100, 100, 100], dtype="<u4")
        query = np.zeros(768, dtype=np.float32)
        self.assertEqual(
            rank_mass_groups(query, artifact, PageMoments(counts, means, variances)),
            (0, 2, 1),
        )
        self.assertEqual(
            rank_mass_groups(query, artifact, PageMoments(counts, means, np.zeros_like(variances))),
            (0, 1, 2),
        )

    def test_small_moments_and_independent_paired_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = source_fixture(root, base_rows=256)
            build_page_selector(root, root, source)
            artifact = read_page_selector(root, source)
            build_page_moments(root, artifact, source)
            moments = read_page_moments(root / "moments.bin", artifact)
            self.assertEqual(int(np.sum(moments.counts)), len(artifact.membership_ids))
            self.assertTrue(np.isfinite(moments.variances).all())
            _, development = development_fixture(root)
            control = root / "control"
            evaluate_range_selector(
                artifact, root / "queries.parquet", root / "truth.parquet", control,
                development, query_count=1, row_bytes=96,
            )
            baseline = json.loads((control / "evidence.json").read_bytes())
            b = baseline["metrics"]
            fixed = (
                b["mean_recall_at_100_ppm"], b["p05_recall_at_100_ppm"],
                b["mean_recall_at_10_ppm"],
                sum(item["hits_at_100"] < 90 for item in baseline["samples"]),
            )
            baseline_samples_hash = hashlib.sha256(
                _canonical_json_bytes(baseline["samples"]),
            ).hexdigest()
            with (
                patch("scripts.native_one_million_page_dispersion_mass_evaluation.BASELINE", fixed),
                patch("scripts.native_one_million_page_dispersion_mass_evaluation.BASELINE_SAMPLES_SHA256", baseline_samples_hash),
                patch("scripts.validate_native_one_million_page_dispersion_mass.BASELINE", fixed),
                patch("scripts.validate_native_one_million_page_dispersion_mass.BASELINE_SAMPLES_SHA256", baseline_samples_hash),
            ):
                result = evaluate_page_dispersion_mass(
                    artifact, root / "moments.bin", root / "queries.parquet",
                    root / "truth.parquet", root, development, query_count=1,
                )
                validation = validate_page_dispersion_mass(
                    root, root, root, source, development, query_count=1,
                )
            self.assertEqual(result["metrics"], validation["metrics"])
            self.assertEqual(result["baseline_metrics"], validation["baseline_metrics"])
            query = np.zeros(768, dtype=np.float32)
            rank = rank_mass_groups(query, artifact, moments)
            self.assertEqual(set(rank), set(range(len(artifact.groups))))
            with (root / "moments.bin").open("ab") as stream:
                stream.write(b"x")
            with self.assertRaises(ValueError):
                read_page_moments(root / "moments.bin", artifact)

    def test_mass_spot_script_seals_tenth_artifact(self) -> None:
        commit = "12" * 20
        plan = build_plan(
            source_commit=commit,
            source_archive=SourceArchiveIdentity("s3://frozen/source.tar.gz", "34" * 32, 1234),
            requirements_sha256="56" * 32,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/native-one-million-mass-selector/"
                + commit + "/runs/relaion-1m-dev1000-a0001"
            ),
            selector_kind="mass",
        )
        script = worker_script(plan)
        syntax = subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True, check=False)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        self.assertLess(len(script.encode()), 16_384)
        self.assertIn("scripts.native_one_million_page_dispersion_mass_cell construct", script)
        self.assertIn("publish_artifact moments.bin", script)
        self.assertEqual(len(artifact_names("mass")), 10)


if __name__ == "__main__":
    unittest.main()
