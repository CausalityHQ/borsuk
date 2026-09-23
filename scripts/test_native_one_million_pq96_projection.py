"""PQ96 row-width projection phase boundary and Spot wiring."""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.launch_native_geometric_layout_spot import SourceArchiveIdentity
from scripts.launch_native_one_million_selector_spot import build_plan, worker_script
from scripts.native_one_million_page_selector import build_page_selector
from scripts.native_one_million_pq80_projection_cell import (
    run_evaluate as run_pq80_evaluate,
)
from scripts.native_one_million_pq80_projection_cell import (
    run_validate as run_pq80_validate,
)
from scripts.native_one_million_pq96_projection_cell import run_evaluate, run_validate
from scripts.test_native_one_million_group_selector import _fixture as source_fixture
from scripts.test_native_one_million_selector_evaluation import (
    _fixture as development_fixture,
)


class OneMillionPq96ProjectionTests(unittest.TestCase):
    def test_small_replay_and_spot_script(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = source_fixture(root, base_rows=256)
            build_page_selector(root, root, source)
            _, development = development_fixture(root)
            with (
                patch("scripts.native_one_million_pq96_projection_cell.SOURCE_IDENTITIES", source),
                patch("scripts.native_one_million_pq96_projection_cell.DEVELOPMENT_IDENTITIES", development),
                patch("scripts.native_one_million_pq96_projection_cell.check_prior_page_artifact"),
            ):
                run_evaluate(root, root, query_count=1)
                run_validate(root, root, query_count=1)
            result = json.loads((root / "result.json").read_bytes())
            validation = json.loads((root / "validation.json").read_bytes())
            self.assertEqual(result["metrics"], validation["metrics"])
            self.assertEqual(result["metrics"]["row_bytes"], 96)
            self.assertEqual(result["schema"], "borsuk-one-million-pq96-locality-result-v1")
        commit = "12" * 20
        plan = build_plan(
            source_commit=commit,
            source_archive=SourceArchiveIdentity("s3://frozen/source.tar.gz", "34" * 32, 1234),
            requirements_sha256="56" * 32,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/native-one-million-pq96-selector/"
                + commit + "/runs/relaion-1m-dev1000-a0001"
            ),
            selector_kind="pq96",
        )
        script = worker_script(plan)
        syntax = subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True, check=False)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        self.assertLess(len(script.encode()), 16_384)
        self.assertIn("scripts.native_one_million_pq96_projection_cell construct", script)
        self.assertIn("borsuk-one-million-pq96-selector-terminal-v1", script)

    def test_pq80_small_replay_and_spot_script(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = source_fixture(root, base_rows=256)
            build_page_selector(root, root, source)
            _, development = development_fixture(root)
            with (
                patch("scripts.native_one_million_pq80_projection_cell.SOURCE_IDENTITIES", source),
                patch("scripts.native_one_million_pq80_projection_cell.DEVELOPMENT_IDENTITIES", development),
                patch("scripts.native_one_million_pq80_projection_cell.check_prior_page_artifact"),
            ):
                run_pq80_evaluate(root, root, query_count=1)
                run_pq80_validate(root, root, query_count=1)
            result = json.loads((root / "result.json").read_bytes())
            validation = json.loads((root / "validation.json").read_bytes())
            self.assertEqual(result["metrics"], validation["metrics"])
            self.assertEqual(result["metrics"]["row_bytes"], 80)
            self.assertEqual(result["schema"], "borsuk-one-million-pq80-locality-result-v1")
        commit = "12" * 20
        plan = build_plan(
            source_commit=commit,
            source_archive=SourceArchiveIdentity("s3://frozen/source.tar.gz", "34" * 32, 1234),
            requirements_sha256="56" * 32,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/native-one-million-pq80-selector/"
                + commit + "/runs/relaion-1m-dev1000-a0001"
            ),
            selector_kind="pq80",
        )
        script = worker_script(plan)
        syntax = subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True, check=False)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        self.assertLess(len(script.encode()), 16_384)
        self.assertIn("scripts.native_one_million_pq80_projection_cell construct", script)
        self.assertIn("borsuk-one-million-pq80-selector-terminal-v1", script)


if __name__ == "__main__":
    unittest.main()
