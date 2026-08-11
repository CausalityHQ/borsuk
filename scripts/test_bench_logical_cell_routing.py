#!/usr/bin/env python3
"""Behavioral contract tests for positioned V12 logical-cell qualification."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PLANNER = ROOT / "scripts/bench_logical_cell_routing.py"
MANIFEST = ROOT / "docs/research/logical-cell-routing-positioned-v12-campaign.json"


class BenchLogicalCellRoutingTest(unittest.TestCase):
    def plan(self, *extra: str) -> dict[str, object]:
        result = subprocess.run(
            [sys.executable, str(PLANNER), "--manifest", str(MANIFEST), *extra],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(result.stdout)

    def test_production_plan_is_realistic_positioned_v12_and_exactly_paired(
        self,
    ) -> None:
        plan = self.plan()
        self.assertEqual(plan["campaign_id"], "logical-cell-routing-positioned-v12-v1")
        self.assertEqual(plan["dimensions"], 768)
        self.assertEqual(plan["metric"], "cosine")
        self.assertEqual(plan["mutation_protocol"], "positioned-v12")
        self.assertEqual(plan["purchase_option"], "spot")
        self.assertEqual(plan["cell_counts"], [2_000, 16_000])
        self.assertEqual(plan["writers"], [1, 8, 32])
        self.assertEqual(plan["repetitions"], 5)
        self.assertEqual(plan["arm_count"], 60)
        self.assertEqual(plan["campaign_timeout_seconds"], 21_600)
        self.assertEqual(plan["setup_timeout_seconds"], 1_800)
        self.assertEqual(plan["cell_timeout_seconds"], 240)
        self.assertEqual(plan["clone_timeout_seconds"], 60)
        self.assertLessEqual(plan["worst_case_seconds"], 21_600)
        arms = plan["arms"]
        self.assertEqual(len(arms), 60)
        paired = {
            (arm["cell_count"], arm["writers"], arm["repetition"]): set()
            for arm in arms
        }
        for arm in arms:
            paired[(arm["cell_count"], arm["writers"], arm["repetition"])].add(
                arm["routing_mode"]
            )
        self.assertTrue(paired)
        self.assertTrue(
            all(modes == {"flat", "quantizer"} for modes in paired.values())
        )

    def test_structural_smoke_covers_the_full_factor_shape_without_measurement(
        self,
    ) -> None:
        plan = self.plan(
            "--structural-smoke",
            "--writers",
            "1,8,32",
            "--logical-cells",
            "2000,16000",
        )
        self.assertEqual(plan["claim_eligibility"], "ineligible-structural-smoke")
        self.assertEqual(plan["cell_counts"], [2_000, 16_000])
        self.assertEqual(plan["writers"], [1, 8, 32])
        self.assertEqual(plan["arm_count"], 12)
        self.assertTrue(all(arm["repetition"] == 1 for arm in plan["arms"]))

    def test_structural_smoke_rejects_a_factor_subset(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                str(PLANNER),
                "--manifest",
                str(MANIFEST),
                "--structural-smoke",
                "--writers",
                "1,8",
                "--logical-cells",
                "2000,16000",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("full preregistered factor shape", result.stderr)

    def test_manifest_rejects_a_matrix_that_cannot_fit_its_campaign_deadline(
        self,
    ) -> None:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        manifest["cell_timeout_seconds"] = 3_600
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "manifest.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")
            result = subprocess.run(
                [sys.executable, str(PLANNER), "--manifest", str(path)],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("worst-case matrix duration", result.stderr)

    def test_shell_runner_plan_only_uses_the_same_authoritative_arm_expansion(
        self,
    ) -> None:
        result = subprocess.run(
            ["bash", str(ROOT / "scripts/bench_logical_cell_routing.sh")],
            cwd=ROOT,
            env={**os.environ, "BORSUK_ROUTING_PLAN_ONLY": "1"},
            check=True,
            capture_output=True,
            text=True,
        )
        plan = json.loads(result.stdout)
        self.assertEqual(plan["arm_count"], 60)
        self.assertEqual(len(plan["arms"]), 60)

    def test_shell_runner_smoke_plan_covers_both_catalog_sizes_and_both_modes(
        self,
    ) -> None:
        result = subprocess.run(
            ["bash", str(ROOT / "scripts/bench_logical_cell_routing.sh")],
            cwd=ROOT,
            env={
                **os.environ,
                "BORSUK_ROUTING_PLAN_ONLY": "1",
                "BORSUK_ROUTING_SMOKE": "1",
            },
            check=True,
            capture_output=True,
            text=True,
        )
        plan = json.loads(result.stdout)
        self.assertEqual(plan["cell_counts"], [2_000, 16_000])
        self.assertEqual(plan["arm_count"], 4)
        self.assertEqual(plan["claim_eligibility"], "ineligible-structural-smoke")
        self.assertEqual(
            [(arm["cell_count"], arm["routing_mode"]) for arm in plan["arms"]],
            [
                (2_000, "flat"),
                (2_000, "quantizer"),
                (16_000, "flat"),
                (16_000, "quantizer"),
            ],
        )

    def test_rust_builder_uses_catalog_initialization_without_seed_records(
        self,
    ) -> None:
        source = (
            ROOT / "crates/borsuk/examples/logical_cell_routing_bench.rs"
        ).read_text(encoding="utf-8")
        build_body = source.split("fn build() -> BenchResult<()> {", 1)[1].split(
            "\nfn run_config()", 1
        )[0]
        self.assertIn("initialize_logical_cell_catalog", build_body)
        self.assertNotIn("index.add(", build_body)
        self.assertNotIn("finish_bulk_load", build_body)
        for field in (
            "catalog_checksum",
            "catalog_rows",
            "catalog_dimensions",
            "encoded_bytes",
            "seed_records",
            "physical_segments",
            "flat_distinct_cells",
            "quantizer_distinct_cells",
            "routing_probe_count",
            "routing_agreements",
        ):
            self.assertIn(field, build_body)

    def test_correctness_gates_name_current_positioned_behaviors(self) -> None:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        self.assertEqual(
            manifest["correctness_gates"],
            [
                "multi_writer_reopen",
                "sequential_last_write_wins",
                "delete_reopen",
                "cross_modality_reopen",
                "shard_head_conflict_rebase",
                "lost_head_response_reconciled",
                "publication_failure_invisible",
                "materializer_race",
            ],
        )

    def test_shell_runner_times_and_bounds_every_setup_and_clone_phase(self) -> None:
        source = (ROOT / "scripts/bench_logical_cell_routing.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn('now="$EPOCHREALTIME"', source)
        self.assertIn("local LC_ALL=C", source)
        self.assertIn("'%s%s000'", source)
        self.assertNotIn("'%s%06d000'", source)
        self.assertNotIn("time.monotonic_ns()", source)
        self.assertIn(
            'SETUP_TIMEOUT_SECONDS="$(json_value setup_timeout_seconds)"', source
        )
        self.assertIn(
            'timeout --signal=TERM --kill-after=30s "$setup_remaining_seconds" env',
            source,
        )
        self.assertIn('mv "$clone_evidence" "$cell_output/clone.json"', source)
        self.assertNotIn(
            'mv "$clone_evidence" "$cell_output/clone.json" 2>/dev/null || true',
            source,
        )
        self.assertIn(
            "for artifact in clone.json resources.csv storage-access.csv summary.csv samples.csv",
            source,
        )
        s3_precondition = source.index(
            'python3 "$ROOT_DIR/scripts/benchmark_s3.py" assert-empty'
        )
        s3_timer = source.index("capture_epoch_ns started_ns", s3_precondition)
        s3_copy = source.index("aws s3 sync", s3_timer)
        self.assertLess(s3_precondition, s3_timer)
        self.assertLess(s3_timer, s3_copy)

    def test_startup_failure_is_published_as_terminal_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "output"
            archive = root / "source.tar"
            archive.write_bytes(b"not-the-expected-source")
            result = subprocess.run(
                ["bash", str(ROOT / "scripts/bench_logical_cell_routing.sh")],
                cwd=ROOT,
                env={
                    **os.environ,
                    "BORSUK_ROUTING_SMOKE": "1",
                    "BORSUK_ROUTING_OUTPUT_ROOT": str(output),
                    "BORSUK_ROUTING_INDEX_ROOT": str(root / "indexes"),
                    "BORSUK_SOURCE_ARCHIVE": str(archive),
                    "BORSUK_SOURCE_SHA256": "0" * 64,
                },
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertTrue((output / "LOGICAL_CELL_ROUTING_FAILED").is_file())
            self.assertFalse((output / "LOGICAL_CELL_ROUTING_COMPLETE").exists())


if __name__ == "__main__":
    unittest.main()
