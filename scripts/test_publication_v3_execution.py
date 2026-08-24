from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.publication_v3_execution import (
    ExecutionJob,
    borsuk_cell,
    build_worker_script,
    qualification_cell,
    runtime_worker_script,
    worker_failure_trap_script,
)
from scripts.publication_v3_protocol import validate_schedule_cell

ROOT = Path(__file__).resolve().parents[1]


def frozen_manifest() -> dict[str, object]:
    value = json.loads(
        (ROOT / "docs/research/publication-v3-manifest.json").read_text()
    )
    value["source"] = {
        "state": "frozen",
        "git_commit": "1" * 40,
        "archive_sha256": "2" * 64,
        "cargo_lock_sha256": "3" * 64,
        "python_lock_sha256": "4" * 64,
        "node_lock_sha256": "5" * 64,
    }
    return value


class PublicationV3ExecutionTests(unittest.TestCase):
    def test_generic_read_cells_and_arms_have_distinct_frozen_authority(self) -> None:
        manifest = frozen_manifest()
        first = borsuk_cell(
            manifest,
            workload_id="standard-ann-read",
            dataset_id="sift-128",
            repetition_id="r01",
            build_attempt=1,
        )
        fifth = borsuk_cell(
            manifest,
            workload_id="standard-ann-read",
            dataset_id="sift-128",
            repetition_id="r05",
            build_attempt=1,
        )

        first_arm = ExecutionJob.runtime(
            first, attempt=1, profile="recall", arm_index=0
        )
        high_recall_arm = ExecutionJob.runtime(
            first, attempt=1, profile="recall", arm_index=2
        )
        fifth_arm = ExecutionJob.runtime(
            fifth, attempt=1, profile="recall", arm_index=2
        )

        self.assertEqual(first["repetition_id"], "r01")
        self.assertEqual(fifth["repetition_id"], "r05")
        self.assertEqual(first["workload"]["id"], "standard-ann-read")
        self.assertNotEqual(first["result_prefix"], fifth["result_prefix"])
        self.assertEqual(first["index_prefix"], fifth["index_prefix"])
        self.assertIn(
            "/runtime-recall/arms/0000/attempts/0001", first_arm.terminal_prefix
        )
        self.assertIn(
            "/runtime-recall/arms/0002/attempts/0001",
            high_recall_arm.terminal_prefix,
        )
        self.assertIn(
            "/runtime-recall/arms/0002/attempts/0001", fifth_arm.terminal_prefix
        )
        self.assertNotEqual(first_arm.terminal_prefix, high_recall_arm.terminal_prefix)
        self.assertNotEqual(high_recall_arm.terminal_prefix, fifth_arm.terminal_prefix)

    def test_generic_cell_rejects_unscheduled_membership(self) -> None:
        manifest = frozen_manifest()
        for workload_id, dataset_id, repetition_id in (
            ("missing", "sift-128", "r01"),
            ("standard-ann-read", "laion-100m-768", "r01"),
            ("standard-ann-read", "sift-128", "r06"),
        ):
            with self.subTest(
                workload_id=workload_id,
                dataset_id=dataset_id,
                repetition_id=repetition_id,
            ):
                with self.assertRaisesRegex(ValueError, "uniquely scheduled"):
                    borsuk_cell(
                        manifest,
                        workload_id=workload_id,
                        dataset_id=dataset_id,
                        repetition_id=repetition_id,
                    )

    def test_worker_signal_trap_delegates_to_preinstalled_reporter(self) -> None:
        with tempfile.TemporaryDirectory(prefix="borsuk-v3-worker-trap-") as directory:
            root = Path(directory)
            reporter = root / "reporter.sh"
            receipt = root / "receipt.txt"
            detail_log = root / "runtime.log"
            detail_log.write_text("runtime failed\n", encoding="utf-8")
            reporter.write_text(
                "#!/usr/bin/env bash\n"
                f'printf \'%s\\n%s\\n%s\\n\' "$1" "$2" "$3" >{receipt!s}\n',
                encoding="utf-8",
            )
            reporter.chmod(0o700)
            script = (
                "complete=0\n"
                "stage=execute-runtime\n"
                f"detail_log={detail_log!s}\n"
                f"{worker_failure_trap_script(reporter)}\n"
                "kill -TERM $$\n"
            )

            result = subprocess.run(
                ["bash", "-c", script], capture_output=True, text=True, check=False
            )

            self.assertEqual(result.returncode, 143)
            self.assertEqual(
                receipt.read_text(encoding="utf-8").splitlines(),
                ["143", "execute-runtime", str(detail_log)],
            )

    def test_build_attempts_use_distinct_index_authority_and_runtime_pins_one(
        self,
    ) -> None:
        manifest = frozen_manifest()
        first_cell = qualification_cell(
            manifest,
            dataset_id="sift-128",
            workload_kind="read-recall",
            build_attempt=1,
        )
        second_cell = qualification_cell(
            manifest,
            dataset_id="sift-128",
            workload_kind="read-recall",
            build_attempt=2,
        )
        first = ExecutionJob.build(first_cell, attempt=1)
        second = ExecutionJob.build(second_cell, attempt=2)
        runtime = ExecutionJob.runtime(second_cell, attempt=3)
        self.assertIn("/build-attempts/0001/index-", first.index_uri)
        self.assertIn("/build-attempts/0002/index-", second.index_uri)
        self.assertEqual(runtime.index_uri, second.index_uri)
        self.assertNotEqual(first.index_uri, second.index_uri)
        self.assertEqual(validate_schedule_cell(first_cell), first_cell)
        self.assertEqual(validate_schedule_cell(second_cell), second_cell)
        self.assertTrue(first.complete_uri.endswith("/BUILD_TERMINAL_COMPLETE.json"))
        self.assertTrue(first.failed_uri.endswith("/BUILD_TERMINAL_FAILED.json"))

    def test_qualification_selects_canonical_borsuk_cells_from_partial_manifest(
        self,
    ) -> None:
        manifest = frozen_manifest()
        read = qualification_cell(
            manifest, dataset_id="sift-128", workload_kind="read-recall"
        )
        lifecycle = qualification_cell(
            manifest,
            dataset_id="synthetic-clustered-1m-768",
            workload_kind="write-update-delete-compact",
        )
        self.assertEqual(read["system"], "borsuk")
        self.assertEqual(read["repetition_id"], "r01")
        self.assertEqual(read["dataset"]["source"]["state"], "staged")
        self.assertEqual(lifecycle["system"], "borsuk")
        self.assertEqual(lifecycle["dataset"]["source"]["state"], "generated")
        self.assertNotEqual(read["index_prefix"], lifecycle["index_prefix"])

    def test_build_worker_emits_binary_index_authority_and_terminal_receipt(
        self,
    ) -> None:
        script = build_worker_script(
            job=ExecutionJob.build(
                qualification_cell(
                    frozen_manifest(),
                    dataset_id="sift-128",
                    workload_kind="read-recall",
                ),
                attempt=1,
            ),
            source_uri="s3://bucket/source/source.tar.gz",
            source_sha256="2" * 64,
            manifest_uri="s3://bucket/manifests/manifest.json",
            manifest_sha256="6" * 64,
            protocol_uri="s3://bucket/protocols/cell.json",
            protocol_sha256="7" * 64,
            attempt_id="build-0001",
            terminal_prefix="s3://bucket/results/cell/build/attempts/0001",
        )
        self.assertIn(
            "cargo build --locked --release --example production_bench", script
        )
        self.assertIn("--example rest_app_bench", script)
        self.assertIn("length(Contents || `[]`)", script)
        self.assertNotIn("length(Contents)'", script)
        self.assertIn("object_count=$(aws s3api list-objects-v2", script)
        self.assertIn('if [[ "$object_count" != 0 ]]; then', script)
        self.assertNotIn("--output text | grep", script)
        self.assertIn('"$work/cell/dataset/"', script)
        self.assertNotIn('"$work/dataset/"', script)
        self.assertIn("refusing nonempty scheduled index prefix", script)
        self.assertIn("seal_publication_v3_index.py", script)
        self.assertIn("--mode seal", script)
        self.assertIn("INDEX_COMPLETE.json", script)
        self.assertIn("INDEX_OBJECTS.json", script)
        self.assertIn("INDEX_INVENTORY.json", script)
        self.assertIn("BINARY_COMPLETE.json", script)
        self.assertIn(
            'binary="$work/source/target/release/examples/production_bench"',
            script,
        )
        self.assertIn(
            'rest_binary="$work/source/target/release/examples/rest_app_bench"',
            script,
        )
        self.assertIn("REST_BINARY_COMPLETE.json", script)
        self.assertIn('"rest_binary_sha256":"%s"', script)
        self.assertIn(
            '--generator "$work/source/target/release/examples/generate_synthetic_dataset"',
            script,
        )
        self.assertIn("BUILD_TERMINAL_COMPLETE.json", script)
        self.assertIn("instance-life-cycle", script)
        self.assertIn('test "$instance_purchase_option" = spot', script)
        self.assertIn('"purchase_option":"%s"', script)
        self.assertIn('exec > >(tee -a "$work/worker.log") 2>&1', script)
        self.assertIn('detail_log="$work/cell/build/step-00.log"', script)
        self.assertNotIn('find "$work"', script)
        self.assertIn("stage=build-index", script)
        self.assertLess(len(script.encode("utf-8")), 16 * 1024)
        self.assertEqual(
            subprocess.run(["bash", "-n"], input=script, text=True).returncode, 0
        )

    def test_runtime_worker_uses_verified_binary_dedicated_cache_and_cgroup(
        self,
    ) -> None:
        script = runtime_worker_script(
            job=ExecutionJob.runtime(
                qualification_cell(
                    frozen_manifest(),
                    dataset_id="sift-128",
                    workload_kind="read-recall",
                ),
                attempt=1,
            ),
            source_uri="s3://bucket/source/source.tar.gz",
            source_sha256="2" * 64,
            manifest_uri="s3://bucket/manifests/manifest.json",
            manifest_sha256="6" * 64,
            protocol_uri="s3://bucket/protocols/cell.json",
            protocol_sha256="7" * 64,
            build_prefix="s3://bucket/results/cell/build/attempts/0001",
            binary_sha256="8" * 64,
            attempt_id="runtime-0001",
            terminal_prefix="s3://bucket/results/cell/runtime/attempts/0001",
            purchase_option="on-demand",
            disk_cache_max_bytes=0,
            exact_read_max_physical_amplification=3,
            max_active_searches=4,
            max_waiting_searches=16,
            leaf_read_width=32,
            max_inflight_leaf_reads=48,
            max_parallel_decode_rank_tasks=1,
            cpu_threads=3,
            io_threads=88,
            s3_get_concurrency=64,
            ram_budget_bytes=2 * 1024 * 1024 * 1024,
        )
        self.assertNotIn("cargo ", script)
        self.assertNotIn("rustup", script)
        self.assertIn('cache_mount="$work/cell/cache"', script)
        self.assertIn('"$work/cell/runtime-dataset/$name"', script)
        self.assertNotIn('"$work/runtime-dataset/$name"', script)
        self.assertIn("MemoryMax=8589934592", script)
        self.assertIn("MemorySwapMax=0", script)
        self.assertIn("--service-type=exec", script)
        self.assertIn("StandardOutput=append:$detail_log", script)
        self.assertIn("StandardError=append:$detail_log", script)
        self.assertNotIn("--scope", script)
        self.assertIn("--mode runtime", script)
        self.assertIn("observe_publication_v3_index.py", script)
        self.assertIn("RESULT_COMPLETE.json", script)
        self.assertIn("RUNTIME_TERMINAL_COMPLETE.json", script)
        self.assertIn("instance-life-cycle", script)
        self.assertIn('test "$instance_purchase_option" = on-demand', script)
        self.assertIn('"purchase_option":"%s"', script)
        self.assertIn('--purchase-option "$instance_purchase_option"', script)
        self.assertIn("--max-active-searches 4", script)
        self.assertIn("--max-waiting-searches 16", script)
        self.assertIn("--leaf-read-width 32", script)
        self.assertIn("--max-inflight-leaf-reads 48", script)
        self.assertIn("--max-parallel-decode-rank-tasks 1", script)
        self.assertIn("--cpu-threads 3", script)
        self.assertIn("--io-threads 88", script)
        self.assertIn("--s3-get-concurrency 64", script)
        self.assertIn("--ram-budget-bytes 2147483648", script)
        self.assertIn("--disk-cache-max-bytes 0", script)
        self.assertIn("--exact-read-max-physical-amplification 3", script)
        self.assertIn('test "$actual_max_parallel_decode_rank_tasks" = 1', script)
        self.assertLess(
            script.index("stage=attest-purchase"), script.index("stage=provision")
        )
        self.assertIn("stage=mount-cache", script)
        self.assertIn("stage=verify-index", script)
        self.assertIn("stage=execute-runtime", script)
        self.assertIn('detail_log="$work/cell/runtime/step-00.log"', script)
        self.assertIn('mkdir -p "$(dirname "$detail_log")"', script)
        self.assertLess(
            script.index('mkdir -p "$(dirname "$detail_log")"'),
            script.index("systemd-run --quiet"),
        )
        self.assertIn("stage=publish-receipts", script)
        self.assertLess(len(script.encode("utf-8")), 16 * 1024)
        self.assertEqual(
            subprocess.run(["bash", "-n"], input=script, text=True).returncode, 0
        )

    def test_concurrency_runtime_worker_publishes_checked_sidecars(self) -> None:
        script = runtime_worker_script(
            job=ExecutionJob.runtime(
                qualification_cell(
                    frozen_manifest(),
                    dataset_id="sift-128",
                    workload_kind="read-recall",
                ),
                attempt=2,
                profile="concurrency",
            ),
            source_uri="s3://bucket/source/source.tar.gz",
            source_sha256="2" * 64,
            manifest_uri="s3://bucket/manifests/manifest.json",
            manifest_sha256="6" * 64,
            protocol_uri="s3://bucket/protocols/cell.json",
            protocol_sha256="7" * 64,
            build_prefix="s3://bucket/results/cell/build/attempts/0001",
            binary_sha256="8" * 64,
            attempt_id="runtime-0002",
            terminal_prefix="s3://bucket/results/cell/runtime/attempts/0002",
            runtime_profile="concurrency",
            arm_index=1,
            disk_cache_max_bytes=1024 * 1024 * 1024,
            exact_read_max_physical_amplification=3,
            max_active_searches=16,
            max_waiting_searches=64,
            leaf_read_width=32,
            max_inflight_leaf_reads=96,
            max_parallel_decode_rank_tasks=1,
            cpu_threads=3,
            io_threads=160,
            s3_get_concurrency=128,
            ram_budget_bytes=2 * 1024 * 1024 * 1024,
        )
        self.assertIn("--runtime-profile concurrency", script)
        self.assertIn("--arm-index 1", script)
        self.assertIn("bench_concurrency.csv", script)
        self.assertIn("bench_concurrency_samples.csv", script)
        self.assertIn('test "$actual_runtime_profile" = concurrency', script)
        self.assertIn('"concurrency_summary_sha256"', script)
        self.assertIn('"concurrency_samples_sha256"', script)
        self.assertIn('test "$actual_max_active" = 16', script)
        self.assertIn('test "$actual_max_waiting" = 64', script)
        self.assertIn('test "$actual_leaf_width" = 32', script)
        self.assertIn('test "$actual_max_leaf_reads" = 96', script)
        self.assertIn('test "$actual_cpu_threads" = 3', script)
        self.assertIn('test "$actual_io_threads" = 160', script)
        self.assertIn('test "$actual_s3_gets" = 128', script)
        self.assertIn('test "$actual_ram_budget" = 2147483648', script)
        self.assertIn("RUNTIME_EXECUTION_CONTRACT.json", script)
        self.assertEqual(
            subprocess.run(["bash", "-n"], input=script, text=True).returncode, 0
        )


if __name__ == "__main__":
    unittest.main()
