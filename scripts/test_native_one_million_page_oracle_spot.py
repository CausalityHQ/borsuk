"""Focused source authority and Spot worker boundary checks."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np

from scripts.launch_native_geometric_layout_spot import SourceArchiveIdentity
from scripts.launch_native_one_million_selector_spot import (
    artifact_names,
    build_launch_specs,
    build_plan,
    launch_and_monitor,
    worker_script,
)
from scripts.native_one_million_page_feasibility import oracle_page_selection
from scripts.native_one_million_page_oracle_cell import _prior_authority


class PageOracleSpotTest(unittest.TestCase):
    def test_numpy_page_group_plane_is_admitted(self) -> None:
        page_groups = np.asarray([0, 0, 1, 1], dtype="<u4")
        self.assertEqual(
            oracle_page_selection((0, 1, 2, 3), (0, 1), page_groups, maximum_pages=2),
            (0, 1),
        )

    def test_frozen_prior_artifacts_authenticate(self) -> None:
        source = Path("/tmp/borsuk-opq8-one-million-closeout")
        if not (source / "terminal.json").exists():
            self.skipTest("terminal-closed prior artifacts are not staged here")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for target, old in (
                ("prior-terminal.json", "terminal.json"),
                ("prior-source-seal.json", "seal.artifact"),
                ("prior-plans.json", "plans.artifact"),
                ("prior-plan-seal.json", "plan-seal.artifact"),
            ):
                (root / target).symlink_to(source / old)
            seal, plans = _prior_authority(root)
            self.assertEqual(
                seal["schema"], "borsuk-one-million-opq8-physical-codes-v1"
            )
            self.assertEqual(len(plans["samples"]), 1000)
            (root / "prior-plan-seal.json").unlink()
            (root / "prior-plan-seal.json").write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "identity differs"):
                _prior_authority(root)

    def test_worker_is_one_spot_cell_with_truth_after_map_seal(self) -> None:
        commit = "12" * 20
        plan = build_plan(
            source_commit=commit,
            source_archive=SourceArchiveIdentity(
                "s3://frozen/source.tar.gz", "34" * 32, 1234
            ),
            requirements_sha256="56" * 32,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/"
                f"native-one-million-page-oracle-selector/{commit}/runs/relaion-1m-dev1000-a0001"
            ),
            selector_kind="page_oracle",
        )
        script = worker_script(plan)
        result = subprocess.run(
            ["bash", "-n"], input=script, text=True, capture_output=True
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertLess(len(script.encode()), 16_384)
        self.assertNotRegex(script, r"@[A-Z_]+@")
        before = script[: script.index("phase=evaluate")]
        self.assertIn("prior-plans.json", before)
        self.assertNotIn("development-gt100.parquet", before)
        self.assertNotIn("truth.parquet", before)
        self.assertIn('publish_artifact "$name"', before)
        self.assertIn("unshare --net --fork setpriv --reuid=nobody", script)
        self.assertIn("--if-none-match '*'", script)
        self.assertIn("swapoff -a", script)
        self.assertEqual(len(artifact_names("page_oracle")), 10)
        self.assertTrue(
            all(
                item["InstanceMarketOptions"]["MarketType"] == "spot"
                for item in build_launch_specs(plan)
            )
        )
        terminal = script[
            script.index("terminal() {") : script.index("trap terminal EXIT")
        ]
        embedded = terminal.split("python3 - <<'PY'\n", 1)[1].split("\nPY", 1)[0]
        compile(embedded, "<page oracle terminal>", "exec")

    def test_reservation_names_frozen_prior_artifacts(self) -> None:
        commit = "12" * 20
        plan = build_plan(
            source_commit=commit,
            source_archive=SourceArchiveIdentity(
                "s3://frozen/source.tar.gz", "34" * 32, 1234
            ),
            requirements_sha256="56" * 32,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/"
                f"native-one-million-page-oracle-selector/{commit}/runs/relaion-1m-dev1000-a0001"
            ),
            selector_kind="page_oracle",
        )

        class S3:
            def list_objects_v2(self, **_kwargs):
                return {"KeyCount": 0}

            def get_object(self, **_kwargs):
                raise RuntimeError("poll unavailable")

        class EC2:
            def run_instances(self, **_kwargs):
                return {"Instances": [{"InstanceId": "i-0123456789abcdef0"}]}

        writes = []
        session = SimpleNamespace(client=lambda name: S3() if name == "s3" else EC2())
        with (
            patch.dict(
                sys.modules,
                {"boto3": SimpleNamespace(Session=lambda **_kwargs: session)},
            ),
            patch(
                "scripts.launch_native_one_million_selector_spot._atomic_put",
                side_effect=lambda *_args, **kwargs: writes.append(kwargs),
            ),
            patch(
                "scripts.launch_native_one_million_selector_spot._terminate_and_wait"
            ),
            self.assertRaisesRegex(RuntimeError, "poll unavailable"),
        ):
            launch_and_monitor(plan)
        reservation = json.loads(writes[0]["body"])
        self.assertEqual(
            set(reservation["source_inputs"]),
            {
                "generation",
                "base",
                "delta",
                "prior-terminal",
                "prior-source-seal",
                "prior-plans",
                "prior-plan-seal",
            },
        )
        self.assertEqual(set(reservation["development_inputs"]), {"queries", "truth"})


if __name__ == "__main__":
    unittest.main()
