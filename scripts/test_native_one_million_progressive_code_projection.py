"""Focused checks for the preregistered mirrored-wave Spot projection."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts.launch_native_geometric_layout_spot import SourceArchiveIdentity
from scripts.launch_native_one_million_selector_spot import (
    artifact_names,
    build_plan,
    worker_script,
)
from scripts.native_one_million_progressive_code_projection_cell import (
    _prior_authority,
    run_plan,
)
from scripts.native_progressive_code_wave import page_lengths, plan_mirrored_wave
from scripts.validate_native_one_million_progressive_code_projection import (
    _replay_cover,
)


class ProgressiveProjectionTest(unittest.TestCase):
    def test_independent_sign_admission_and_mirror(self) -> None:
        rng = np.random.default_rng(20260923)
        for _ in range(20):
            counts = tuple(int(value) for value in rng.integers(1, 5, size=9))
            sign, magnitude = page_lengths(counts, base_pages=5)
            priority = tuple(int(value) for value in rng.permutation(9))
            plan = plan_mirrored_wave(priority, sign, magnitude,
                                      maximum_gets=2, maximum_bytes=2000)
            ranked = tuple(tuple(page) for page in plan["sign"]["priority_pages"])
            self.assertEqual(_replay_cover(ranked, sign, 2, 2000), plan["sign"])
            mirror_bytes = sum(
                sum(magnitude[role][first:end])
                for role, first, end in plan["sign"]["ranges"]
            )
            self.assertEqual(mirror_bytes, plan["magnitude"]["encoded_bytes"])

    def test_plan_phase_rejects_truth_capability(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "truth.parquet").write_bytes(b"truth must remain sealed")
            with self.assertRaisesRegex(ValueError, "truth-free"):
                run_plan(root)

    def test_prior_terminal_requires_pinned_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "prior-range-terminal.json").write_text("{}\n")
            with self.assertRaisesRegex(ValueError, "terminal identity"):
                _prior_authority(root)

    def test_spot_plan_and_truth_barrier(self) -> None:
        commit = "0" * 40
        plan = build_plan(
            source_commit=commit,
            source_archive=SourceArchiveIdentity("s3://bucket/source.tar.gz", "0" * 64, 1),
            requirements_sha256="0" * 64,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/"
                f"native-one-million-progressive_code_wave-selector/{commit}/"
                "runs/relaion-1m-dev1000-a0001"
            ),
            selector_kind="progressive_code_wave",
        )
        self.assertIn("progressive-code-wave-plans", artifact_names(plan.selector_kind))
        worker = worker_script(plan)
        self.assertLess(len(worker.encode()), 16_384)
        self.assertLess(worker.index("phase=plan\n"), worker.index("phase=evaluate\n"))
        self.assertLess(worker.index("phase=evaluate\n"), worker.index("truth.parquet"))
        self.assertIn("borsuk-one-million-progressive-code-wave-terminal-v1", worker)


if __name__ == "__main__":
    unittest.main()
