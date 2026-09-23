"""Selector phase boundary, Spot one-shot and terminal checks."""

from __future__ import annotations

import dataclasses
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts.launch_native_geometric_layout_spot import SourceArchiveIdentity
from scripts.launch_native_one_million_selector_spot import (
    _validate_terminal_bytes,
    build_launch_specs,
    build_plan,
    launch_and_monitor,
    main,
    worker_script,
)
from scripts.native_one_million_selector_cell import (
    run_construct,
    run_evaluate,
    run_validate,
)
from scripts.test_native_one_million_group_selector import _fixture as source_fixture
from scripts.test_native_one_million_selector_evaluation import (
    _fixture as development_fixture,
)


class OneMillionSelectorSpotTests(unittest.TestCase):
    @staticmethod
    def plan():
        commit = "12" * 20
        return build_plan(
            source_commit=commit,
            source_archive=SourceArchiveIdentity("s3://frozen/source.tar.gz", "34" * 32, 1234),
            requirements_sha256="56" * 32,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/native-one-million-group-selector/"
                + commit + "/runs/relaion-1m-dev1000-a0001"
            ),
        )

    def test_source_query_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "queries.parquet").write_bytes(b"forbidden")
            with self.assertRaisesRegex(ValueError, "query boundary"):
                run_construct(root)

    def test_small_phase_separated_cell_replays_end_to_end(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source_identities = source_fixture(root, base_rows=256)
            with patch("scripts.native_one_million_selector_cell.SOURCE_IDENTITIES", source_identities):
                run_construct(root)
                self.assertFalse((root / "queries.parquet").exists())
                _, development_identities = development_fixture(root)
                with patch("scripts.native_one_million_selector_cell.DEVELOPMENT_IDENTITIES", development_identities):
                    run_evaluate(root, root, query_count=1)
                    run_validate(root, root, query_count=1)
                    self.assertEqual(json.loads((root / "validation.json").read_bytes())["metrics"]["query_count"], 1)

    def test_worker_shell_boundary_and_spot_specs(self) -> None:
        script = worker_script(self.plan())
        syntax = subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True, check=False)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        self.assertLess(len(script.encode()), 16_384)
        self.assertNotRegex(script, r"@[A-Z_]+@")
        before = script[: script.index("phase=evaluate")]
        self.assertIn("unshare --net --fork", before)
        self.assertNotIn("development-query.parquet", before)
        self.assertNotIn("development-gt100.parquet", before)
        self.assertIn("centroids.bin", before)
        self.assertIn("membership.bin", before)
        self.assertIn("swapoff -a", script)
        self.assertIn("check_resources", script)
        self.assertIn("worker-stderr.log", script)
        self.assertIn("--if-none-match '*'", script)
        self.assertIn('cmp -s "$name" "readback/$name"', script)
        self.assertIn("unshare --net --fork setpriv --reuid=nobody", script)
        terminal_body = script[script.index("terminal() {") : script.index("trap terminal EXIT")]
        terminal_python = terminal_body.split("python3 - <<'PY'\n", 1)[1].split("\nPY", 1)[0]
        compile(terminal_python, "<selector terminal>", "exec")
        specs = build_launch_specs(self.plan())
        self.assertTrue(all(spec["InstanceMarketOptions"]["MarketType"] == "spot" for spec in specs))
        self.assertEqual(len({spec["ClientToken"] for spec in specs}), len(specs))

    def test_terminal_roster_and_failed_controller_exit(self) -> None:
        plan = self.plan()
        terminal = {
            "schema": "borsuk-one-million-selector-terminal-v1", "artifacts": {},
            "attempt": 1, "claim_eligible": False, "elapsed_seconds": 12,
            "exit_code": 1, "instance_id": "i-0123456789abcdef0", "phase": "construct",
            "source_commit": plan.source_commit, "source_archive": dataclasses.asdict(plan.source_archive),
            "requirements_sha256": plan.requirements_sha256, "status": "failed",
        }
        body = (json.dumps(terminal, sort_keys=True, separators=(",", ":")) + "\n").encode()
        self.assertEqual(_validate_terminal_bytes(body, plan, "i-0123456789abcdef0"), terminal)
        with (
            patch("scripts.launch_native_one_million_selector_spot.parse_args", return_value=plan),
            patch("scripts.launch_native_one_million_selector_spot.launch_and_monitor", return_value=terminal),
            self.assertRaises(SystemExit) as error,
        ):
            main([])
        self.assertEqual(error.exception.code, 1)

    def test_existing_prefix_blocks_launch(self) -> None:
        class S3:
            def list_objects_v2(self, **kwargs):
                return {"KeyCount": 1, "Contents": [{"Key": kwargs["Prefix"] + "terminal.json"}]}

            def put_object(self, **kwargs):
                raise AssertionError("must not reserve existing attempt")

        session = SimpleNamespace(client=lambda name: S3() if name == "s3" else object())
        with patch.dict(sys.modules, {"boto3": SimpleNamespace(Session=lambda **kwargs: session)}):
            with self.assertRaisesRegex(ValueError, "immutable attempt"):
                launch_and_monitor(self.plan())


if __name__ == "__main__":
    unittest.main()
