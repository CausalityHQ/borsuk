"""Frozen paired OPQ8/two-bit Spot authority checks."""

from __future__ import annotations

import subprocess
import unittest

from scripts.launch_native_geometric_layout_spot import SourceArchiveIdentity
from scripts.launch_native_one_million_selector_spot import (
    artifact_names,
    build_plan,
    worker_script,
)
from scripts.native_hundred_thousand_opq8_paired_cell import (
    OPQ_SOURCE,
    TWO_BIT_SOURCE,
    verify_recorded_outcomes,
)


class PairedSpotTests(unittest.TestCase):
    def test_recorded_primary_hits_are_bound_to_pages_and_truth(self) -> None:
        truth = tuple(i.to_bytes(2, "little") for i in range(100))
        owners = {item: index // 50 for index, item in enumerate(truth)}
        arm = {
            "grouped_pages": [0, 1], "grouped_hits_at_10": 10,
            "grouped_hits_at_100": 100,
        }
        for name in ("exact", "primary", "diagnostic"):
            arm[name + "_pages"] = [0]
            arm[name + "_data_bytes"] = 100
            arm[name + "_hits_at_10"] = 10
            arm[name + "_hits_at_100"] = 50
        verify_recorded_outcomes(arm, truth, owners, (100, 100))
        arm["primary_hits_at_100"] = 51
        with self.assertRaisesRegex(ValueError, "primary outcome"):
            verify_recorded_outcomes(arm, truth, owners, (100, 100))

    def test_worker_seals_source_before_development_and_reads_real_ranges(self) -> None:
        commit = "ab" * 20
        plan = build_plan(
            source_commit=commit,
            source_archive=SourceArchiveIdentity("s3://frozen/archive.tar.gz", "cd" * 32, 1234),
            requirements_sha256="ef" * 32,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/native-hundred-thousand-opq8-paired/"
                + commit + "/runs/relaion-100k-dev1000-a0001"
            ),
            selector_kind="paired",
        )
        script = worker_script(plan)
        syntax = subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True, check=False)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        self.assertLess(len(script.encode()), 16_384)
        self.assertEqual(len(artifact_names("paired")), 10)
        self.assertLess(script.index("publish_artifact source-seal.json"), script.index("opq/plans.json"))
        self.assertLess(script.index("publish_artifact plan-seal.json"), script.index("queries.parquet"))
        self.assertIn("setpriv --reuid=nobody", script)
        self.assertIn("unshare --net --fork setpriv", script)
        self.assertIn("scripts.native_hundred_thousand_opq8_paired_broker", script)
        self.assertIn("timeout --foreground", script)
        self.assertIn("--body terminal.json --if-none-match '*'", script)
        self.assertIn("borsuk-hundred-thousand-opq8-paired-terminal-v1", script)
        self.assertIn(
            "80ddf40533aefd3c24b7d3ea4539887aa94f91bb/runs/relaion-100k-dev1000-a0001/artifacts/seal.json seal.json",
            script,
        )
        self.assertEqual(OPQ_SOURCE["model"].encoded_bytes, 3_148_820)
        self.assertEqual(TWO_BIT_SOURCE["groups"].encoded_bytes, 20_000_832)


if __name__ == "__main__":
    unittest.main()
