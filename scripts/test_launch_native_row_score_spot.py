"""One immutable Spot cell for the row-score nomination falsifier."""

from __future__ import annotations

import json
import subprocess
import unittest

from scripts.launch_native_geometric_layout_spot import (
    DEFAULT_TARGETS,
    FROZEN_QUERIES,
    FROZEN_SOURCE,
    FROZEN_TRUTH,
    SourceArchiveIdentity,
)
from scripts.launch_native_row_score_spot import (
    _validate_terminal_bytes,
    build_launch_specs,
    build_plan,
    worker_script,
)


class RowScoreSpotTests(unittest.TestCase):
    @staticmethod
    def plan():
        return build_plan(
            profile="causality",
            source_commit="12" * 20,
            source_archive=SourceArchiveIdentity(
                uri="s3://frozen/source.tar.gz", sha256="34" * 32,
                encoded_bytes=1234,
            ),
            source=FROZEN_SOURCE,
            queries=FROZEN_QUERIES,
            truth=FROZEN_TRUTH,
            requirements_sha256="56" * 32,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/native-row-score/"
                + "12" * 20 + "/runs/relaion-100k-dev1000-a0001"
            ),
            image_id="ami-06121aa3085b6f918",
            security_group_id="sg-0b1fd3e4fbde4af0d",
            instance_profile_arn=(
                "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
            ),
            targets=DEFAULT_TARGETS,
        )

    def test_worker_seals_code_plane_before_query_access(self) -> None:
        script = worker_script(self.plan())
        syntax = subprocess.run(
            ["bash", "-n"], input=script, text=True, capture_output=True, check=False
        )
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        before_queries = script[: script.index("phase=evaluate")]
        self.assertIn("unshare --net", before_queries)
        self.assertIn("artifacts/codes.bin", before_queries)
        self.assertNotIn(FROZEN_QUERIES.uri, before_queries)
        self.assertNotIn(FROZEN_TRUTH.uri, before_queries)
        self.assertIn("tree.parquet", script[script.index("phase=evaluate") :])
        self.assertIn("scripts.native_row_score_cell validate", script)
        self.assertIn('env PYTHONPATH="$root/repo"', script[script.index("phase=validate") :])
        terminal_body = script[script.index("terminal() {") : script.index("trap terminal EXIT")]
        terminal_python = terminal_body.split("python3 - <<'PY'\n", 1)[1].split("\nPY", 1)[0]
        compile(terminal_python, "<Spot terminal>", "exec")
        self.assertIn("set +e", terminal_body)
        self.assertLess(len(script.encode()), 16_384)

    def test_complete_terminal_needs_exact_artifact_roster(self) -> None:
        plan = self.plan()
        body = (json.dumps({
            "schema": "borsuk-row-score-terminal-v1", "artifacts": {},
            "attempt": 1, "claim_eligible": False, "elapsed_seconds": 12,
            "exit_code": 0, "instance_id": "i-0123456789abcdef0",
            "phase": "complete", "source_commit": plan.source_commit,
            "status": "complete",
        }, sort_keys=True, separators=(",", ":")) + "\n").encode()
        with self.assertRaisesRegex(ValueError, "artifact roster"):
            _validate_terminal_bytes(body, plan, "i-0123456789abcdef0")

    def test_launch_specs_use_one_time_spot_client_tokens(self) -> None:
        specs = build_launch_specs(self.plan())
        self.assertEqual(len(specs), 3)
        for spec in specs:
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertIn("native-row-score", spec["ClientToken"])
            self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")


if __name__ == "__main__":
    unittest.main()
