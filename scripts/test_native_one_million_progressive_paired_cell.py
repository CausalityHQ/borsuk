"""Truth and prior-identity barriers for the 1M paired score cell."""

from __future__ import annotations

import base64
import gzip
import tempfile
import unittest
from pathlib import Path

from scripts.launch_native_geometric_layout_spot import SourceArchiveIdentity
from scripts.launch_native_one_million_selector_spot import (
    artifact_names,
    build_plan,
    worker_script,
)
from scripts.native_one_million_progressive_paired_cell import (
    _prior_plans,
    run_construct,
    run_plan,
)
from scripts.native_one_million_progressive_paired_worker import expanded_worker_script


class ProgressivePairedCellTest(unittest.TestCase):
    def test_construct_rejects_query_capability(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "queries.parquet").write_bytes(b"must remain sealed")
            with self.assertRaisesRegex(ValueError, "query capability"):
                run_construct(root)

    def test_plan_rejects_truth_capability(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "truth.parquet").write_bytes(b"must remain sealed")
            with self.assertRaisesRegex(ValueError, "truth-free"):
                run_plan(root, root)

    def test_prior_terminal_must_match_pinned_hash(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in (
                "prior-progressive-terminal.json", "prior-progressive-plans.json",
                "prior-progressive-plan-seal.json",
            ):
                (root / name).write_bytes(b"{}\n")
            with self.assertRaisesRegex(ValueError, "identity"):
                _prior_plans(root)

    def test_compressed_spot_worker_keeps_truth_barrier_and_full_roster(self) -> None:
        commit = "0" * 40
        plan = build_plan(
            source_commit=commit,
            source_archive=SourceArchiveIdentity("s3://bucket/source.tar.gz", "0" * 64, 1),
            requirements_sha256="0" * 64,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/"
                f"native-one-million-progressive_paired-selector/{commit}/"
                "runs/relaion-1m-dev1000-a0001"
            ),
            selector_kind="progressive_paired",
        )
        expanded = expanded_worker_script(plan)
        wrapper = worker_script(plan)
        lines = wrapper.splitlines()
        start = next(index for index, line in enumerate(lines) if line.startswith("base64 -d <<")) + 1
        payload = "".join(lines[start:lines.index("BORSUK_WORKER_PAYLOAD")])
        self.assertEqual(gzip.decompress(base64.b64decode(payload)).decode(), expanded)
        self.assertLess(len(wrapper.encode()), 16_384)
        self.assertLess(expanded.index("phase=construct\n"), expanded.index("queries.parquet"))
        self.assertLess(expanded.index("queries.parquet"), expanded.index("phase=plan\n"))
        self.assertLess(expanded.index("phase=plan\n"), expanded.index("phase=evaluate\n"))
        self.assertLess(expanded.index("phase=evaluate\n"), expanded.index("truth.parquet"))
        self.assertIn("progressive-code-seal.json", expanded)
        self.assertIn("prior-progressive-plans.json", expanded)
        self.assertEqual(len(artifact_names(plan.selector_kind)), 20)


if __name__ == "__main__":
    unittest.main()
