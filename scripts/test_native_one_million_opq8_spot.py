"""Immutable Causality Spot plan for the 1M OPQ8 source-only gate."""

from __future__ import annotations

import subprocess
import unittest

from scripts.launch_native_geometric_layout_spot import SourceArchiveIdentity
from scripts.launch_native_one_million_selector_spot import (
    artifact_names,
    build_launch_specs,
    build_plan,
    parse_args,
    worker_script,
)


class OneMillionOpq8SpotTests(unittest.TestCase):
    def test_worker_seals_source_and_plan_before_truth(self) -> None:
        commit = "ab" * 20
        plan = build_plan(
            source_commit=commit,
            source_archive=SourceArchiveIdentity("s3://frozen/source.tar.gz", "34" * 32, 1234),
            requirements_sha256="56" * 32,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/native-one-million-opq8-selector/"
                + commit + "/runs/relaion-1m-dev1000-a0001"
            ),
            selector_kind="opq8_1m",
            wall_seconds=14_400,
        )
        script = worker_script(plan)
        syntax = subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True, check=False)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        self.assertLess(len(script.encode()), 16_384)
        self.assertIn("borsuk-one-million-opq8-terminal-v1", script)
        self.assertEqual(len(artifact_names("opq8_1m")), 13)
        self.assertLess(script.index("publish_artifact seal.json"), script.index("queries.parquet --only-show-errors"))
        self.assertLess(script.index("publish_artifact plan-seal.json"), script.index("truth.parquet --only-show-errors"))
        self.assertIn("control/centroids.bin", script)
        self.assertIn("scripts.native_one_million_opq8_cell", script)
        self.assertIn("--body terminal.json --if-none-match '*'", script)
        self.assertEqual(script.count("timeout --foreground"), 4)
        self.assertTrue(all(len(spec["ClientToken"]) <= 64 for spec in build_launch_specs(plan)))
        parsed = parse_args([
            "--source-commit", commit, "--source-archive-uri", plan.source_archive.uri,
            "--source-archive-sha256", plan.source_archive.sha256,
            "--source-archive-bytes", str(plan.source_archive.encoded_bytes),
            "--requirements-sha256", plan.requirements_sha256,
            "--output-prefix", plan.output_prefix, "--selector-kind", "opq8_1m",
        ])
        self.assertEqual(parsed.wall_seconds, 14_400)


if __name__ == "__main__":
    unittest.main()
