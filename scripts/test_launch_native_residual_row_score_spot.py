"""One immutable Spot cell for the row-score nomination falsifier."""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.launch_native_geometric_layout_spot import (
    DEFAULT_TARGETS,
    FROZEN_QUERIES,
    FROZEN_SOURCE,
    FROZEN_TRUTH,
    SourceArchiveIdentity,
)
from scripts.launch_native_residual_row_score_spot import (
    PRIOR_PAGES,
    PRIOR_TREE,
    _validate_terminal_bytes,
    build_launch_specs,
    build_plan,
    worker_script,
)
from scripts.native_residual_row_score_cell import PRIOR_PAGES as CELL_PAGES
from scripts.native_residual_row_score_cell import PRIOR_TREE as CELL_TREE


class ResidualRowScoreSpotTests(unittest.TestCase):
    def test_controller_import_needs_no_science_dependencies(self) -> None:
        result = subprocess.run(
            ["/usr/bin/python3", "-c", "import scripts.launch_native_residual_row_score_spot"],
            capture_output=True, text=True, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_worker_and_cell_bind_the_same_frozen_router_artifacts(self) -> None:
        for worker, cell in ((PRIOR_TREE, CELL_TREE), (PRIOR_PAGES, CELL_PAGES)):
            self.assertEqual(worker.uri, cell.uri)
            self.assertEqual(worker.sha256, cell.sha256)
            self.assertEqual(worker.encoded_bytes, cell.encoded_bytes)

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
                "s3://borsuk-bench-453182569524-euc1/research/native-residual-row-score/"
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
        self.assertIn("scripts.native_residual_row_score_cell validate", script)
        self.assertIn('env PYTHONPATH="$root/repo"', script[script.index("phase=validate") :])
        terminal_body = script[script.index("terminal() {") : script.index("trap terminal EXIT")]
        terminal_python = terminal_body.split("python3 - <<'PY'\n", 1)[1].split("\nPY", 1)[0]
        compile(terminal_python, "<Spot terminal>", "exec")
        self.assertIn("set +e", terminal_body)
        self.assertLess(len(script.encode()), 16_384)

    def test_complete_terminal_needs_exact_artifact_roster(self) -> None:
        plan = self.plan()
        body = (json.dumps({
            "schema": "borsuk-residual-row-score-terminal-v1", "artifacts": {},
            "attempt": 1, "claim_eligible": False, "elapsed_seconds": 12,
            "exit_code": 0, "instance_id": "i-0123456789abcdef0",
            "phase": "complete", "source_commit": plan.source_commit,
            "source_archive": {
                "uri": plan.source_archive.uri,
                "sha256": plan.source_archive.sha256,
                "encoded_bytes": plan.source_archive.encoded_bytes,
            },
            "requirements_sha256": plan.requirements_sha256,
            "status": "complete",
        }, sort_keys=True, separators=(",", ":")) + "\n").encode()
        with self.assertRaisesRegex(ValueError, "artifact roster"):
            _validate_terminal_bytes(body, plan, "i-0123456789abcdef0")

    def test_failed_terminal_binds_the_executed_source_archive(self) -> None:
        plan = self.plan()
        value = {
            "schema": "borsuk-residual-row-score-terminal-v1", "artifacts": {},
            "attempt": 1, "claim_eligible": False, "elapsed_seconds": 12,
            "exit_code": 1, "instance_id": "i-0123456789abcdef0",
            "phase": "source", "source_commit": plan.source_commit,
            "source_archive": {
                "uri": plan.source_archive.uri,
                "sha256": "00" * 32,
                "encoded_bytes": plan.source_archive.encoded_bytes,
            },
            "requirements_sha256": plan.requirements_sha256,
            "status": "failed",
        }
        body = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
        with self.assertRaisesRegex(ValueError, "source archive"):
            _validate_terminal_bytes(body, plan, "i-0123456789abcdef0")

    def test_launch_specs_use_one_time_spot_client_tokens(self) -> None:
        specs = build_launch_specs(self.plan())
        self.assertEqual(len(specs), 3)
        for spec in specs:
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertIn("native-residual-row-score", spec["ClientToken"])
            self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")

    def test_worker_rejects_resource_peak_above_rss_cap(self) -> None:
        script = worker_script(self.plan())
        function = script.split("check_peak_rss() {", 1)[1].split("terminal() {", 1)[0]
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "resources.txt"
            path.write_text("Maximum resident set size (kbytes): 3145729\n")
            result = subprocess.run(
                ["bash", "-c", "MAXIMUM_RSS_BYTES=3221225472\ncheck_peak_rss() {" + function + "\ncheck_peak_rss \"$1\"", "bash", str(path)],
                capture_output=True, text=True, check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            path.write_text("Maximum resident set size (kbytes): 3145728\n")
            accepted = subprocess.run(
                ["bash", "-c", "MAXIMUM_RSS_BYTES=3221225472\ncheck_peak_rss() {" + function + "\ncheck_peak_rss \"$1\"", "bash", str(path)],
                capture_output=True, text=True, check=False,
            )
            self.assertEqual(accepted.returncode, 0, accepted.stderr)


if __name__ == "__main__":
    unittest.main()
