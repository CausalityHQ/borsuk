"""Focused code-wave geometry tests for the page-local 200-byte format."""

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
from scripts.native_one_million_two_bit_code_projection import (
    code_page_lengths,
    plan_code_wave,
)
from scripts.native_one_million_two_bit_code_projection_cell import (
    _prior_authority,
    run_plan,
)
from scripts.validate_native_one_million_two_bit_code_projection import _replay_cover


class CodeWaveProjectionTest(unittest.TestCase):
    def test_bridged_code_page_consumes_real_bytes(self) -> None:
        lengths = code_page_lengths((1, 1, 1, 1), base_pages=4)
        plan = plan_code_wave((0, 2), lengths, maximum_gets=1, maximum_bytes=600)
        self.assertEqual(plan["target_pages"], [["base", 0], ["base", 2]])
        self.assertEqual(plan["included_pages"], [["base", 0], ["base", 1], ["base", 2]])
        self.assertEqual(plan["ranges"], [["base", 0, 3]])
        self.assertEqual(plan["gets"], 1)
        self.assertEqual(plan["encoded_bytes"], 600)


    def test_code_page_lengths_reject_empty_page(self) -> None:
        with self.assertRaisesRegex(ValueError, "code page rows"):
            code_page_lengths((1, 0, 2), base_pages=2)

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

    def test_independent_cover_replays_bridge_cost_and_admission(self) -> None:
        lengths = {"base": (200, 200, 200, 200)}
        replay = _replay_cover((('base', 0), ('base', 2), ('base', 3)), lengths, 1, 600)
        self.assertEqual(replay["target_pages"], [["base", 0], ["base", 2]])
        self.assertEqual(replay["included_pages"], [["base", 0], ["base", 1], ["base", 2]])
        self.assertEqual(replay["ranges"], [["base", 0, 3]])
        self.assertEqual(replay["encoded_bytes"], 600)

    def test_independent_replay_matches_planner_across_small_layouts(self) -> None:
        rng = np.random.default_rng(20260923)
        for _ in range(30):
            counts = tuple(int(value) for value in rng.integers(1, 5, size=9))
            lengths = code_page_lengths(counts, base_pages=5)
            priority = tuple(int(value) for value in rng.permutation(9))
            expected = plan_code_wave(priority, lengths, maximum_gets=2, maximum_bytes=3200)
            ranked = tuple(tuple(page) for page in expected["priority_pages"])
            self.assertEqual(_replay_cover(ranked, lengths, 2, 3200), expected)

    def test_spot_plan_has_dedicated_projection_roster(self) -> None:
        commit = "0" * 40
        plan = build_plan(
            source_commit=commit,
            source_archive=SourceArchiveIdentity("s3://bucket/source.tar.gz", "0" * 64, 1),
            requirements_sha256="0" * 64,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/"
                f"native-one-million-two_bit_code_wave-selector/{commit}/"
                "runs/relaion-1m-dev1000-a0001"
            ),
            selector_kind="two_bit_code_wave",
        )
        self.assertEqual(plan.selector_kind, "two_bit_code_wave")
        self.assertIn("two-bit-code-wave-plans", artifact_names(plan.selector_kind))
        worker = worker_script(plan)
        self.assertLess(len(worker.encode()), 16_384)
        self.assertLess(worker.index("phase=plan\n"), worker.index("phase=evaluate\n"))
        self.assertLess(worker.index("phase=evaluate\n"), worker.index("truth.parquet"))
        self.assertIn("prior-range-plans.json", worker)
        self.assertIn("borsuk-one-million-two-bit-code-wave-terminal-v1", worker)
