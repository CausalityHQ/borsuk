"""Source-only page centroids and independent small-cohort replay."""

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
    _controller_terminal,
    _validate_terminal_bytes,
    build_launch_specs,
    build_plan,
    worker_script,
)
from scripts.native_one_million_page_selector import (
    build_page_selector,
    read_page_selector,
)
from scripts.native_one_million_page_selector_cell import (
    run_construct,
    run_evaluate,
    run_validate,
)
from scripts.native_one_million_page_selector_evaluation import (
    evaluate_page_selector,
    select_page_groups,
)
from scripts.test_native_one_million_group_selector import _fixture as source_fixture
from scripts.test_native_one_million_selector_evaluation import (
    _fixture as development_fixture,
)
from scripts.validate_native_one_million_page_selector import validate_page_selector


class OneMillionPageSelectorTests(unittest.TestCase):
    def test_source_batches_and_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identities = source_fixture(root, base_rows=256)
            first = build_page_selector(root, root / "one", identities, batch_rows=17)
            build_page_selector(root, root / "two", identities, batch_rows=37)
            self.assertEqual((root / "one/centroids.bin").read_bytes(), (root / "two/centroids.bin").read_bytes())
            self.assertEqual((root / "one/seal.json").read_bytes(), (root / "two/seal.json").read_bytes())
            self.assertEqual(first.page_centroids.shape, (257, 768))
            self.assertEqual(len(first.groups), 33)
            self.assertEqual(first.page_groups[256], 32)
            self.assertEqual(first.groups[0].code_bytes, 1636)
            _, development = development_fixture(root)
            result = evaluate_page_selector(first, root / "queries.parquet", root / "truth.parquet", root / "one", development, query_count=1)
            validation = validate_page_selector(root, root / "one", root / "one", root / "one", identities, development, query_count=1)
            self.assertEqual(result["metrics"], validation["metrics"])
            self.assertEqual(json.loads((root / "one/validation.json").read_bytes())["decision"], result["decision"])

    def test_seal_and_source_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identities = source_fixture(root, base_rows=256)
            artifact = build_page_selector(root, root, identities)
            self.assertEqual(len(select_page_groups(np.zeros(768, dtype=np.float32), artifact)), 32)
            with (root / "centroids.bin").open("r+b") as handle:
                handle.seek(0)
                handle.write(b"X")
            with self.assertRaisesRegex(ValueError, "identity differs"):
                read_page_selector(root, identities)

    def test_duplicate_page_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identities = source_fixture(root, duplicate_delta=True)
            with self.assertRaisesRegex(ValueError, "duplicate|overlap"):
                build_page_selector(root, root, identities)

    def test_phase_cell_and_page_spot_script(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identities = source_fixture(root, base_rows=256)
            with patch("scripts.native_one_million_page_selector_cell.SOURCE_IDENTITIES", identities):
                run_construct(root)
                self.assertFalse((root / "queries.parquet").exists())
                _, development = development_fixture(root)
                with patch("scripts.native_one_million_page_selector_cell.DEVELOPMENT_IDENTITIES", development):
                    run_evaluate(root, root, query_count=1)
                    run_validate(root, root, query_count=1)
        commit = "12" * 20
        plan = build_plan(
            source_commit=commit,
            source_archive=SourceArchiveIdentity("s3://frozen/source.tar.gz", "34" * 32, 1234),
            requirements_sha256="56" * 32,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/native-one-million-page-selector/"
                + commit + "/runs/relaion-1m-dev1000-a0001"
            ),
            selector_kind="page",
        )
        script = worker_script(plan)
        syntax = subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True, check=False)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        self.assertLess(len(script.encode()), 16_384)
        self.assertTrue(all(len(spec["ClientToken"]) <= 64 for spec in build_launch_specs(plan)))
        self.assertIn("scripts.native_one_million_page_selector_cell construct", script)
        self.assertIn("borsuk-one-million-page-selector-terminal-v1", script)
        self.assertIn("--if-none-match '*'", script)
        restarted = build_plan(
            source_commit=commit,
            source_archive=plan.source_archive,
            requirements_sha256=plan.requirements_sha256,
            output_prefix=plan.output_prefix.replace("a0001", "a0002"),
            selector_kind="page",
            attempt=2,
        )
        self.assertIn('"attempt":2', worker_script(restarted))
        self.assertNotEqual(build_launch_specs(plan)[0]["ClientToken"], build_launch_specs(restarted)[0]["ClientToken"])
        interrupted = _controller_terminal(restarted, "i-0123456789abcdef0")
        body = (json.dumps(interrupted, sort_keys=True, separators=(",", ":")) + "\n").encode()
        self.assertEqual(_validate_terminal_bytes(body, restarted, "i-0123456789abcdef0"), interrupted)


if __name__ == "__main__":
    unittest.main()
