#!/usr/bin/env python3
"""Contract tests for the fresh-index AWS layout qualification runner."""

from __future__ import annotations

import csv
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/bench_storage_layout_qualification_aws.sh"
ASSEMBLER = ROOT / "scripts/assemble_storage_layout_qualification.py"


class StorageLayoutQualificationRunnerTest(unittest.TestCase):
    def test_dry_run_freezes_alternating_two_dataset_schedule_without_aws(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "qualification"
            completed = subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=ROOT,
                env={
                    **os.environ,
                    "BORSUK_LAYOUT_EXECUTE": "0",
                    "BORSUK_LAYOUT_ROOT": str(output),
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            schedule = (output / "schedule.csv").read_text()
            self.assertIn("fashion-mnist-784", schedule)
            self.assertIn("glove-100", schedule)
            self.assertIn("fixed-parquet", schedule)
            self.assertIn("fixed-vortex-full", schedule)
            self.assertIn("mixed-vortex-range", schedule)
            rows = list(csv.DictReader(schedule.splitlines()))
            self.assertEqual(len(rows), 5 * 2 * 2 * 5)
            arms = {
                "fixed-parquet",
                "fixed-vortex-full",
                "fixed-vortex-range",
                "mixed-vortex-full",
                "mixed-vortex-range",
            }
            for repetition in range(1, 6):
                repetition_id = f"r{repetition:02d}"
                selected = [
                    row for row in rows if row["repetition_id"] == repetition_id
                ]
                self.assertEqual(
                    {row["query_seed"] for row in selected},
                    {str(20260726 + repetition)},
                )
                for dataset in ("fashion-mnist-784", "glove-100"):
                    for backend in ("local_disk", "s3"):
                        block = [
                            row
                            for row in selected
                            if row["dataset"] == dataset and row["backend"] == backend
                        ]
                        self.assertEqual({row["arm"] for row in block}, arms)
                        self.assertEqual(
                            {int(row["arm_position"]) for row in block},
                            set(range(5)),
                        )
            for dataset in ("fashion-mnist-784", "glove-100"):
                for backend in ("local_disk", "s3"):
                    for arm in arms:
                        positions = {
                            int(row["arm_position"])
                            for row in rows
                            if row["dataset"] == dataset
                            and row["backend"] == backend
                            and row["arm"] == arm
                        }
                        self.assertEqual(positions, set(range(5)))
            self.assertTrue((output / "environment.txt").is_file())

    def test_paid_mode_requires_an_explicit_gate(self) -> None:
        completed = subprocess.run(
            ["bash", str(SCRIPT)],
            cwd=ROOT,
            env={**os.environ, "BORSUK_LAYOUT_EXECUTE": "1"},
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("BORSUK_RUN_LAYOUT_QUALIFICATION=1", completed.stderr)

    def test_dry_run_rejects_schedule_overrides_that_break_the_protocol(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            completed = subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=ROOT,
                env={
                    **os.environ,
                    "BORSUK_LAYOUT_EXECUTE": "0",
                    "BORSUK_LAYOUT_ROOT": str(Path(temporary) / "qualification"),
                    "BORSUK_LAYOUT_REPETITIONS": "3",
                },
                capture_output=True,
                text=True,
            )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("protocol requires repetitions=5", completed.stderr)

    def test_source_contains_methodology_and_fresh_prefix_guards(self) -> None:
        source = SCRIPT.read_text()
        assembler = ASSEMBLER.read_text()
        self.assertIn("BORSUK_SOURCE_SHA256", source)
        self.assertIn("list-objects-v2", source)
        self.assertIn("refusing to reuse non-empty", source)
        self.assertIn("bench_query_samples.csv", source)
        self.assertIn("resources.csv", source)
        self.assertIn("assemble_storage_layout_qualification.py", source)
        self.assertIn("BORSUK_BENCH_FORCE_SEGMENT_PATH=1", source)
        self.assertIn("BORSUK_BENCH_RECALL_ONLY=1", source)
        self.assertIn("BORSUK_BENCH_SKIP_EXACT_RECALL=1", source)
        self.assertIn("build_ms", assembler)
        self.assertIn("total_active_index_bytes", assembler)
        self.assertIn("peak_rss_bytes", assembler)
        self.assertIn("cpu_core_ms", assembler)
        self.assertIn("analyze_storage_layout_qualification.py", source)
        self.assertIn("BORSUK_VORTEX_RANGE_READS", source)
        self.assertIn("BORSUK_SEGMENT_VORTEX_MIN_ROWS", source)
        self.assertIn("MIXED_VORTEX_MIN_ROWS=4096", source)
        self.assertIn("segment_parquet_objects", source)
        self.assertIn("segment_vortex_objects", source)
        self.assertIn("mixed layout did not emit both Parquet and Vortex", source)
        self.assertIn("protocol campaign_id mismatch", source)
        self.assertIn("freeze_layout_dataset_identity.py", source)
        self.assertIn("dataset_identity_sha256", source)
        self.assertIn("BORSUK_INSTANCE_TYPE", source)
        self.assertIn("BORSUK_LOCAL_DISK_CLASS", source)
        self.assertIn("cpu_model=", source)
        self.assertIn("architecture=", source)
        self.assertIn("aws_region=", source)
        self.assertIn("segment_header_codec=bsh1-little-endian-blake3", source)
        self.assertIn(
            "wal_control_codec=bwh1-bwn1-bwd1-bwc1-bmm1-btm1-bid1-bcn1",
            source,
        )
        self.assertIn(
            "request_guard_source=local-backing-reads-or-s3-network-gets",
            source,
        )
        self.assertIn(
            "uncached_phase=application-payload-cache-cold-kernel-page-cache-not-evicted",
            source,
        )
        self.assertIn("backing_bytes_read", assembler)
        self.assertNotIn("pilot", source)

    def test_disposable_case_data_is_removed_only_after_results_are_synced(
        self,
    ) -> None:
        source = SCRIPT.read_text()
        case_complete = source.index(
            "printf 'complete\\n' > \"$case_root/CASE_COMPLETE\""
        )
        sync = source.index("  sync_results", case_complete)
        cleanup = source.index('rm -rf "$cache_root" "$scratch_root"', sync)
        local_index_cleanup = source.index('rm -rf "$case_root/index"', cleanup)
        self.assertLess(case_complete, sync)
        self.assertLess(sync, cleanup)
        self.assertLess(cleanup, local_index_cleanup)


if __name__ == "__main__":
    unittest.main()
