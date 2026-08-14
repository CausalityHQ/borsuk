from __future__ import annotations

import json
import subprocess
import unittest
from pathlib import Path

from scripts.publication_v3_execution import (
    ExecutionJob,
    build_worker_script,
    qualification_cell,
    runtime_worker_script,
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
    def test_build_attempts_use_distinct_index_authority_and_runtime_pins_one(self) -> None:
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
            '--generator "$work/source/target/release/examples/generate_synthetic_dataset"',
            script,
        )
        self.assertIn("BUILD_TERMINAL_COMPLETE.json", script)
        self.assertIn("BUILD_TERMINAL_FAILED.json", script)
        self.assertIn('"stage":"%s"', script)
        self.assertIn('exec > >(tee -a "$work/worker.log") 2>&1', script)
        self.assertIn('detail_log="$work/cell/build/step-00.log"', script)
        self.assertIn('failure_source="$detail_log"', script)
        self.assertIn('tail -c 65536 "$failure_source"', script)
        self.assertIn("FAILURE.log", script)
        self.assertNotIn('find "$work"', script)
        self.assertLess(
            script.index('put_immutable "$work/failed.json"'),
            script.index('tail -c 65536 "$failure_source"'),
        )
        self.assertIn(
            'tail -c 65536 "$failure_source" >"$work/FAILURE.log" || true',
            script,
        )
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
        )
        self.assertNotIn("cargo ", script)
        self.assertNotIn("rustup", script)
        self.assertIn('cache_mount="$work/cell/cache"', script)
        self.assertIn('"$work/cell/runtime-dataset/$name"', script)
        self.assertNotIn('"$work/runtime-dataset/$name"', script)
        self.assertIn("MemoryMax=8589934592", script)
        self.assertIn("MemorySwapMax=0", script)
        self.assertIn("--service-type=exec", script)
        self.assertIn('StandardOutput=append:$detail_log', script)
        self.assertIn('StandardError=append:$detail_log', script)
        self.assertNotIn("--scope", script)
        self.assertIn("--mode runtime", script)
        self.assertIn("observe_publication_v3_index.py", script)
        self.assertIn("RESULT_COMPLETE.json", script)
        self.assertIn("RUNTIME_TERMINAL_COMPLETE.json", script)
        self.assertIn("RUNTIME_TERMINAL_FAILED.json", script)
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


if __name__ == "__main__":
    unittest.main()
