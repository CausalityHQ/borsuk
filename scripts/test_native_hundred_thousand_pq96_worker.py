"""Spot user-data contract for the truth-separated pq96 gate."""

from __future__ import annotations

import subprocess
import unittest

from scripts.launch_native_geometric_layout_spot import (
    DEFAULT_TARGETS,
    FROZEN_QUERIES,
    FROZEN_SOURCE,
    FROZEN_TRUTH,
    SourceArchiveIdentity,
    SpotLayoutPlan,
)
from scripts.native_hundred_thousand_pq96_worker import (
    ARTIFACT_FILES,
    pq96_worker_script,
)


class Pq96WorkerTest(unittest.TestCase):
    def test_user_data_is_parseable_bounded_and_truth_separated(self) -> None:
        commit = "a" * 40
        plan = SpotLayoutPlan(
            profile="causality",
            source_commit=commit,
            source_archive=SourceArchiveIdentity("s3://bucket/source.tar.gz", "b" * 64, 123),
            source=FROZEN_SOURCE,
            queries=FROZEN_QUERIES,
            truth=FROZEN_TRUTH,
            requirements_sha256="c" * 64,
            output_prefix=(
                "s3://bucket/research/native-pq96/"
                f"{commit}/runs/relaion-100k-dev1000-a0001"
            ),
            image_id="ami-1",
            security_group_id="sg-1",
            instance_profile_arn="arn:aws:iam::123:instance-profile/x",
            targets=DEFAULT_TARGETS,
        )
        script = pq96_worker_script(plan)
        self.assertLessEqual(len(script.encode()), 16_384)
        self.assertNotRegex(script, r"@[A-Z_]+@")
        subprocess.run(["bash", "-n"], input=script, text=True, check=True)
        self.assertLess(
            script.index("-m scripts.native_hundred_thousand_pq96_cell construct"),
            script.index("aws s3 cp " + FROZEN_QUERIES.uri),
        )
        self.assertLess(
            script.index("-m scripts.native_hundred_thousand_pq96_cell plan"),
            script.index("aws s3 cp " + FROZEN_TRUTH.uri),
        )
        self.assertEqual(len(ARTIFACT_FILES), 18)
        self.assertIn("--if-none-match '*'", script)
        self.assertIn("failure_reason=resource_cap", script)
        self.assertIn("failure_reason=phase_timeout", script)
        self.assertIn("$prefix/diagnostics/$diagnostic", script)
        self.assertIn("sudo shutdown -h now", script)


if __name__ == "__main__":
    unittest.main()
