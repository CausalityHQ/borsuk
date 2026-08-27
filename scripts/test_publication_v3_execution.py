from __future__ import annotations

import base64
import json
import os
import subprocess
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

from scripts.publication_v3_aws import build_launch_request
from scripts.publication_v3_execution import (
    ExecutionJob,
    borsuk_cell,
    build_worker_script,
    qualification_cell,
    runtime_worker_script,
    worker_failure_trap_script,
    worker_immutable_upload_function,
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


def v21_base_authority(
    cell: dict[str, object], *, build_prefix: str
) -> dict[str, object]:
    index_uri = str(cell["index_prefix"])
    return {
        "manifest_uri": "s3://bucket/manifests/base.json",
        "manifest_sha256": "b" * 64,
        "protocol_uri": "s3://bucket/protocols/base.json",
        "protocol_sha256": "c" * 64,
        "build_terminal_uri": f"{build_prefix}/BUILD_TERMINAL_COMPLETE.json",
        "build_terminal_sha256": "d" * 64,
        "build_prefix": build_prefix,
        "source_archive_sha256": str(cell["source"]["archive_sha256"]),
        "cell": cell,
        "index_id": index_uri.rstrip("/").rsplit("/", 1)[-1],
        "index_uri": index_uri,
        "index_receipt_sha256": "e" * 64,
        "object_roster_sha256": "f" * 64,
        "inventory_sha256": "0" * 64,
    }


class PublicationV3ExecutionTests(unittest.TestCase):
    def test_worker_immutable_upload_retries_reconciles_and_preserves_failure(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(prefix="borsuk-v3-upload-") as directory:
            root = Path(directory)
            binaries = root / "bin"
            captured = root / "captured"
            work = root / "work"
            binaries.mkdir()
            captured.mkdir()
            work.mkdir()
            fake_aws = binaries / "aws"
            fake_aws.write_text(
                "#!/usr/bin/env bash\n"
                "set -euo pipefail\n"
                "operation=${2:-}\n"
                "body= key= query= declared_checksum= checksum_mode=\n"
                'original_args="$*"\n'
                "while [[ $# -gt 0 ]]; do\n"
                "  case $1 in\n"
                "    --body) body=$2; shift 2;;\n"
                "    --key) key=$2; shift 2;;\n"
                "    --query) query=$2; shift 2;;\n"
                "    --checksum-sha256) declared_checksum=$2; shift 2;;\n"
                "    --checksum-mode) checksum_mode=$2; shift 2;;\n"
                "    *) shift;;\n"
                "  esac\n"
                "done\n"
                'count_file="$CAPTURE_DIR/$operation-count"\n'
                'count=0; [[ ! -f "$count_file" ]] || count=$(cat "$count_file")\n'
                'count=$((count + 1)); printf "%s" "$count" >"$count_file"\n'
                'printf "%s" "$original_args" >"$CAPTURE_DIR/$operation-args-$count"\n'
                'target="$CAPTURE_DIR/${key##*/}"\n'
                "if [[ $operation = head-object ]]; then\n"
                "  [[ $query = ChecksumSHA256 && $checksum_mode = ENABLED ]]\n"
                '  openssl dgst -sha256 -binary "$target" | base64 -w0\n'
                "  exit 0\n"
                "fi\n"
                "case $AWS_FAKE_MODE in\n"
                "  transient)\n"
                "    if (( count < 3 )); then echo RequestTimeout >&2; exit 255; fi\n"
                '    cp "$body" "$target";;\n'
                "  ambiguous)\n"
                '    cp "$body" "$target"\n'
                "    echo 'PreconditionFailed (412)' >&2\n"
                "    exit 255;;\n"
                "  ambiguous-mismatch)\n"
                '    printf foreign >"$target"\n'
                "    echo 'PreconditionFailed (412)' >&2\n"
                "    exit 255;;\n"
                "  permanent) echo 'network down: exact upload detail' >&2; exit 255;;\n"
                "  mixed)\n"
                "    if (( count == 1 )); then echo 'AccessDenied first attempt' >&2; exit 255; fi\n"
                "    exit 42;;\n"
                "  silent) exit 42;;\n"
                "esac\n",
                encoding="utf-8",
            )
            fake_aws.chmod(0o700)
            payload = root / "payload.json"
            payload.write_text('{"complete":true}\n', encoding="utf-8")
            runner = root / "run.sh"
            runner.write_text(
                "#!/usr/bin/env bash\n"
                "set -euo pipefail\n"
                f"work={work!s}\n"
                'detail_log="$work/prior.log"\n'
                "immutable_upload_deadline=0\n"
                "immutable_upload_reconciliations=0\n"
                "if [[ ${EXPIRE_UPLOAD_DEADLINE:-0} = 1 ]]; then\n"
                "  immutable_upload_deadline=1\n"
                "  SECONDS=2\n"
                "fi\n" + worker_immutable_upload_function() + "\n"
                "finish() {\n"
                "  status=$1\n"
                "  trap - EXIT\n"
                '  printf "%s" "$detail_log" >"$work/observed-detail-path"\n'
                '  printf "%s" "$immutable_upload_reconciliations" >"$work/observed-reconciliations"\n'
                "  exit $status\n"
                "}\n"
                "trap 'finish $?' EXIT\n"
                f"put_immutable {payload!s} s3://bucket/result.json\n"
                "finish 0\n",
                encoding="utf-8",
            )
            runner.chmod(0o700)
            environment = {
                **dict(os.environ),
                "PATH": f"{binaries}:{os.environ['PATH']}",
                "CAPTURE_DIR": str(captured),
            }

            transient = subprocess.run(
                [str(runner)],
                text=True,
                capture_output=True,
                env={**environment, "AWS_FAKE_MODE": "transient"},
                check=False,
            )
            self.assertEqual(transient.returncode, 0, transient.stderr)
            self.assertEqual((captured / "put-object-count").read_text(), "3")
            self.assertEqual(
                (captured / "result.json").read_bytes(), payload.read_bytes()
            )
            put_args = (captured / "put-object-args-3").read_text()
            for required in (
                "--expected-bucket-owner 453182569524",
                "--if-none-match *",
                "--checksum-sha256",
                "--metadata borsuk-sha256=",
            ):
                self.assertIn(required, put_args)

            for path in captured.iterdir():
                path.unlink()
            ambiguous = subprocess.run(
                [str(runner)],
                text=True,
                capture_output=True,
                env={**environment, "AWS_FAKE_MODE": "ambiguous"},
                check=False,
            )
            self.assertEqual(ambiguous.returncode, 0, ambiguous.stderr)
            self.assertEqual((captured / "put-object-count").read_text(), "1")
            self.assertEqual((captured / "head-object-count").read_text(), "1")
            self.assertEqual((work / "observed-reconciliations").read_text(), "1")
            self.assertIn("reconciled-412", ambiguous.stderr)

            for path in captured.iterdir():
                path.unlink()
            mismatch = subprocess.run(
                [str(runner)],
                text=True,
                capture_output=True,
                env={**environment, "AWS_FAKE_MODE": "ambiguous-mismatch"},
                check=False,
            )
            self.assertNotEqual(mismatch.returncode, 0)
            self.assertEqual((captured / "put-object-count").read_text(), "3")
            self.assertEqual((captured / "head-object-count").read_text(), "3")
            self.assertEqual((work / "observed-reconciliations").read_text(), "0")

            for path in captured.iterdir():
                path.unlink()
            permanent = subprocess.run(
                [str(runner)],
                text=True,
                capture_output=True,
                env={**environment, "AWS_FAKE_MODE": "permanent"},
                check=False,
            )
            self.assertNotEqual(permanent.returncode, 0)
            self.assertEqual((captured / "put-object-count").read_text(), "3")
            detail_path = Path((work / "observed-detail-path").read_text())
            self.assertEqual(detail_path, work / "failure-detail.log")
            self.assertIn("network down: exact upload detail", detail_path.read_text())

            for path in captured.iterdir():
                path.unlink()
            mixed = subprocess.run(
                [str(runner)],
                text=True,
                capture_output=True,
                env={**environment, "AWS_FAKE_MODE": "mixed"},
                check=False,
            )
            self.assertNotEqual(mixed.returncode, 0)
            mixed_detail = Path((work / "observed-detail-path").read_text()).read_text()
            self.assertIn("AccessDenied first attempt", mixed_detail)
            self.assertIn("attempt=3 status=42", mixed_detail)

            for path in captured.iterdir():
                path.unlink()
            silent = subprocess.run(
                [str(runner)],
                text=True,
                capture_output=True,
                env={**environment, "AWS_FAKE_MODE": "silent"},
                check=False,
            )
            self.assertNotEqual(silent.returncode, 0)
            detail_path = Path((work / "observed-detail-path").read_text())
            self.assertIn("status=42", detail_path.read_text())

            for path in captured.iterdir():
                path.unlink()
            expired = subprocess.run(
                [str(runner)],
                text=True,
                capture_output=True,
                env={
                    **environment,
                    "AWS_FAKE_MODE": "transient",
                    "EXPIRE_UPLOAD_DEADLINE": "1",
                },
                check=False,
            )
            self.assertNotEqual(expired.returncode, 0)
            self.assertFalse((captured / "put-object-count").exists())
            expired_detail = Path(
                (work / "observed-detail-path").read_text()
            ).read_text()
            self.assertIn("publish-budget-exhausted", expired_detail)

    def test_generated_build_consumes_staged_roster_without_regeneration(self) -> None:
        manifest = frozen_manifest()
        dataset = next(
            item
            for item in manifest["datasets"]
            if item["id"] == "synthetic-uniform-10m-768"
        )
        recipe = dataset["source"]
        attempt_root = (
            f"{manifest['prefixes']['dataset']}/{dataset['id']}/attempts/0001"
        )
        dataset["source"] = {
            "state": "staged-generated",
            "generator": recipe["generator"],
            "seed": recipe["seed"],
            "generator_source_archive_sha256": "a" * 64,
            "url": f"{attempt_root}/materialized",
            "sha256": "b" * 64,
            "receipt_uri": f"{attempt_root}/STAGING_COMPLETE.json",
            "receipt_sha256": "c" * 64,
        }
        cell = borsuk_cell(
            manifest,
            workload_id="synthetic-dense-read",
            dataset_id=dataset["id"],
            repetition_id="r01",
            build_attempt=1,
        )
        script = build_worker_script(
            job=ExecutionJob.build(cell, attempt=1),
            source_uri="s3://bucket/source/archive.tar.gz",
            source_sha256="2" * 64,
            manifest_uri="s3://bucket/manifest.json",
            manifest_sha256="6" * 64,
            protocol_uri="s3://bucket/protocol.json",
            protocol_sha256="7" * 64,
            attempt_id="build-0001",
            terminal_prefix="s3://bucket/results/build/attempts/0001",
        )
        self.assertIn(dataset["source"]["receipt_uri"], script)
        self.assertIn(dataset["source"]["receipt_sha256"], script)
        self.assertIn("fetch_publication_v3_dataset.py", script)
        self.assertNotIn("BORSUK_SYNTHETIC_GENERATOR", script)
        self.assertIn(f"--dataset-materialization-sha256 {'b' * 64}", script)

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
        self.assertEqual(lifecycle["dataset"]["source"]["state"], "staged-generated")
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
        self.assertIn('"schema_version":2', script)
        self.assertIn('"artifact_upload_reconciliations":%s', script)
        self.assertIn("immutable_upload_deadline=$((SECONDS + 600))", script)
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
        self.assertIn('"schema_version":5', script)
        self.assertIn('"artifact_upload_reconciliations":%s', script)
        self.assertIn("immutable_upload_deadline=$((SECONDS + 600))", script)
        self.assertLess(len(script.encode("utf-8")), 16 * 1024)
        self.assertEqual(
            subprocess.run(["bash", "-n"], input=script, text=True).returncode, 0
        )

    def test_read_diagnostic_is_claim_ineligible_namespaced_and_binds_raw_artifacts(
        self,
    ) -> None:
        cell = qualification_cell(
            frozen_manifest(), dataset_id="sift-128", workload_kind="read-recall"
        )
        job = ExecutionJob.runtime(
            cell,
            attempt=2,
            profile="recall",
            arm_index=0,
            diagnostic=True,
        )
        self.assertIn(
            "/runtime-read-diagnostic/arms/0000/attempts/0002",
            job.terminal_prefix,
        )

        script = runtime_worker_script(
            job=job,
            source_uri="s3://bucket/source/source.tar.gz",
            source_sha256="2" * 64,
            manifest_uri="s3://bucket/manifests/manifest.json",
            manifest_sha256="6" * 64,
            protocol_uri="s3://bucket/protocols/cell.json",
            protocol_sha256="7" * 64,
            build_prefix="s3://bucket/results/cell/build/attempts/0001",
            binary_sha256="8" * 64,
            attempt_id="read-diagnostic-0002",
            terminal_prefix=job.terminal_prefix,
            disk_cache_max_bytes=0,
            exact_read_max_physical_amplification=2,
            max_active_searches=4,
            max_waiting_searches=16,
            leaf_read_width=32,
            max_inflight_leaf_reads=48,
            max_parallel_decode_rank_tasks=1,
            cpu_threads=3,
            io_threads=88,
            s3_get_concurrency=64,
            ram_budget_bytes=2 * 1024 * 1024 * 1024,
            diagnostic_read_nprobes=(32, 64),
            diagnostic_read_candidates=(512, 1024, 2048, 4096),
        )

        self.assertIn("--diagnostic-read-nprobes 32,64", script)
        self.assertIn("--diagnostic-read-candidates 512,1024,2048,4096", script)
        self.assertIn('"claim_eligible":false', script)
        for name in (
            "RESULT_COMPLETE.json",
            "bench_query_samples.csv",
            "bench_recall_latency.csv",
        ):
            self.assertIn(name, script)
        for field in (
            "diagnostic_result_sha256",
            "diagnostic_samples_sha256",
            "diagnostic_summary_sha256",
        ):
            self.assertIn(field, script)
        self.assertEqual(
            script.count('put_immutable "$work/cell/RUNTIME_ATTESTATION.json"'),
            1,
        )
        request = build_launch_request(
            frozen_manifest(),
            role="runtime",
            system="borsuk",
            image_id="ami-0123456789abcdef0",
            subnet_id="subnet-0123456789abcdef0",
            security_group_id="sg-0123456789abcdef0",
            instance_profile_arn=(
                "arn:aws:iam::453182569524:instance-profile/borsuk-test"
            ),
            image_architecture="aarch64",
            subnet_region="eu-central-1",
            campaign_id="publication-v3-20260812",
            cell_id=job.cell_tag,
            attempt=2,
            worker_script=script,
            terminal_failure_uri=job.failed_uri,
            terminal_detail_log_path="/var/lib/borsuk-publication/worker.log",
            max_seconds=7_200,
        )
        self.assertLess(len(base64.b64decode(request["UserData"])), 16 * 1024)
        self.assertEqual(
            subprocess.run(["bash", "-n"], input=script, text=True).returncode, 0
        )

    def test_v21_feasibility_worker_uploads_raw_evidence_before_result_and_receipt(
        self,
    ) -> None:
        cell = qualification_cell(
            frozen_manifest(), dataset_id="deep-image-96", workload_kind="read-recall"
        )
        job = ExecutionJob.runtime(
            cell,
            attempt=3,
            profile="recall",
            arm_index=0,
            v21_feasibility=True,
        )
        self.assertIn(
            "/runtime-v21-feasibility/arms/0000/attempts/0003",
            job.terminal_prefix,
        )
        base_authority = v21_base_authority(
            cell,
            build_prefix="s3://bucket/results/cell/build/attempts/0001",
        )
        script = runtime_worker_script(
            job=job,
            source_uri="s3://bucket/source/source.tar.gz",
            source_sha256="2" * 64,
            manifest_uri="s3://bucket/manifests/manifest.json",
            manifest_sha256="6" * 64,
            protocol_uri="s3://bucket/protocols/cell.json",
            protocol_sha256="7" * 64,
            build_prefix="s3://bucket/results/cell/build/attempts/0001",
            binary_sha256=None,
            attempt_id="v21-feasibility-0003",
            terminal_prefix=job.terminal_prefix,
            disk_cache_max_bytes=0,
            exact_read_max_physical_amplification=2,
            max_active_searches=4,
            max_waiting_searches=16,
            leaf_read_width=32,
            max_inflight_leaf_reads=48,
            max_parallel_decode_rank_tasks=1,
            cpu_threads=3,
            io_threads=88,
            s3_get_concurrency=64,
            ram_budget_bytes=3 * 1024 * 1024 * 1024,
            v21_feasibility=True,
            v21_base_authority=base_authority,
        )
        self.assertIn("--v21-feasibility", script)
        self.assertIn('"claim_eligible":false', script)
        raw_names = (
            "bench_v21_feasibility_arms.csv",
            "bench_v21_feasibility_samples.csv",
            "bench_v21_feasibility_summary.json",
        )
        for name in raw_names:
            self.assertIn(name, script)
            self.assertLess(
                script.index(f'put_immutable "$work/cell/runtime-output/{name}"'),
                script.index('put_immutable "$work/cell/RESULT_COMPLETE.json"'),
            )
        for field in (
            "v21_result_sha256",
            "v21_arms_sha256",
            "v21_samples_sha256",
            "v21_summary_sha256",
        ):
            self.assertIn(field, script)
        self.assertIn("-p MemoryMax=34359738368 -p MemorySwapMax=0", script)
        self.assertIn("--remain-after-exit", script)
        self.assertNotIn("systemd-run --quiet --wait --unit=", script)
        self.assertIn("unit_sub_state", script)
        self.assertIn("--property=ExecMainStatus --value", script)
        self.assertIn("systemctl stop borsuk-v21-0003.service", script)
        self.assertIn("--property=MemoryMax --value", script)
        self.assertIn("--property=MemorySwapMax --value", script)
        self.assertIn("--property=MemoryPeak --value", script)
        self.assertNotIn("requested-systemd-enforced", script)
        self.assertIn("stage=disable-cache", script)
        self.assertNotIn('cache_device=$(lsblk', script)
        self.assertLess(
            script.index("stage=compile-diagnostic"),
            script.index("stage=verify-index"),
        )
        for field in (
            "base_build_terminal_sha256",
            "base_manifest_sha256",
            "base_protocol_sha256",
            "base_source_archive_sha256",
            "base_index_receipt_sha256",
            "base_object_roster_sha256",
            "base_inventory_sha256",
            "base_index_id",
            "base_index_uri",
            "diagnostic_source_archive_sha256",
            "memory_max_bytes",
            "memory_swap_max_bytes",
            "memory_peak_bytes",
        ):
            self.assertIn(field, script)
        self.assertEqual(
            subprocess.run(["bash", "-n"], input=script, text=True).returncode, 0
        )

        receipt_fragment = script.split("        diagnostic_fields=''", 1)[1].split(
            "        concurrency_fields=''", 1
        )[0]
        receipt_probe = f"""set -euo pipefail
v21_result_sha={'1' * 64}
v21_arms_sha={'2' * 64}
v21_samples_sha={'3' * 64}
v21_summary_sha={'4' * 64}
actual_memory_max=34359738368
actual_memory_swap_max=0
actual_memory_peak=123456789
diagnostic_fields=''
{receipt_fragment}
printf '{{\"schema_version\":5%s}}\n' "$diagnostic_fields"
"""
        completed = subprocess.run(
            ["bash"], input=receipt_probe, text=True, capture_output=True
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        receipt = json.loads(completed.stdout)
        self.assertEqual(receipt["base_index_id"], base_authority["index_id"])
        self.assertEqual(receipt["memory_peak_bytes"], 123456789)

    def test_v21_worker_compiles_current_source_but_reads_historical_index_authority(
        self,
    ) -> None:
        current_cell = qualification_cell(
            frozen_manifest(),
            dataset_id="deep-image-96",
            workload_kind="read-recall",
        )
        job = ExecutionJob.runtime(
            current_cell,
            attempt=3,
            profile="recall",
            arm_index=0,
            v21_feasibility=True,
        )
        base_cell = deepcopy(current_cell)
        base_cell["source"]["git_commit"] = "1" * 40
        base_cell["source"]["archive_sha256"] = "a" * 64
        base_cell["cell_id"] = "r01-historical"
        base_cell["index_prefix"] = (
            "s3://bucket/indexes/build-attempts/0001/index-historical"
        )
        base_authority = {
            "manifest_uri": "s3://bucket/manifests/base.json",
            "manifest_sha256": "b" * 64,
            "protocol_uri": "s3://bucket/protocols/base.json",
            "protocol_sha256": "c" * 64,
            "build_terminal_uri": "s3://bucket/results/base/BUILD_TERMINAL_COMPLETE.json",
            "build_terminal_sha256": "d" * 64,
            "build_prefix": "s3://bucket/results/base",
            "source_archive_sha256": "a" * 64,
            "cell": base_cell,
            "index_id": "index-historical",
            "index_uri": base_cell["index_prefix"],
            "index_receipt_sha256": "e" * 64,
            "object_roster_sha256": "f" * 64,
            "inventory_sha256": "0" * 64,
        }
        script = runtime_worker_script(
            job=job,
            source_uri="s3://bucket/source/current.tar.gz",
            source_sha256="2" * 64,
            manifest_uri="s3://bucket/manifests/current.json",
            manifest_sha256="6" * 64,
            protocol_uri="s3://bucket/protocols/current.json",
            protocol_sha256="7" * 64,
            build_prefix=base_authority["build_prefix"],
            binary_sha256=None,
            attempt_id="v21-feasibility-0003",
            terminal_prefix=job.terminal_prefix,
            disk_cache_max_bytes=0,
            exact_read_max_physical_amplification=2,
            max_active_searches=4,
            max_waiting_searches=16,
            leaf_read_width=32,
            max_inflight_leaf_reads=48,
            max_parallel_decode_rank_tasks=1,
            cpu_threads=3,
            io_threads=88,
            s3_get_concurrency=64,
            ram_budget_bytes=3 * 1024 * 1024 * 1024,
            v21_feasibility=True,
            v21_base_authority=base_authority,
        )
        self.assertIn(
            "cargo build --locked --release --example production_bench", script
        )
        self.assertNotIn(
            f"aws s3 cp {base_authority['build_prefix']}/production_bench", script
        )
        self.assertIn(str(base_authority["index_uri"]), script)
        self.assertIn(str(base_authority["source_archive_sha256"]), script)
        self.assertIn("s3://bucket/source/current.tar.gz", script)
        self.assertEqual(
            subprocess.run(["bash", "-n"], input=script, text=True).returncode, 0
        )

    def test_generated_runtime_fetches_only_authenticated_query_roles(self) -> None:
        manifest = frozen_manifest()
        dataset = next(
            item
            for item in manifest["datasets"]
            if item["id"] == "synthetic-clustered-1m-768"
        )
        recipe = dataset["source"]
        attempt_root = (
            f"{manifest['prefixes']['dataset']}/{dataset['id']}/attempts/0001"
        )
        dataset["source"] = {
            "state": "staged-generated",
            "generator": recipe["generator"],
            "seed": recipe["seed"],
            "generator_source_archive_sha256": "a" * 64,
            "url": f"{attempt_root}/materialized",
            "sha256": "b" * 64,
            "receipt_uri": f"{attempt_root}/STAGING_COMPLETE.json",
            "receipt_sha256": "c" * 64,
        }
        cell = borsuk_cell(
            manifest,
            workload_id="synthetic-dense-read",
            dataset_id=dataset["id"],
            repetition_id="r01",
            build_attempt=1,
        )
        script = runtime_worker_script(
            job=ExecutionJob.runtime(cell, attempt=1),
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
            disk_cache_max_bytes=0,
            exact_read_max_physical_amplification=2,
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
        self.assertIn(dataset["source"]["receipt_uri"], script)
        self.assertIn(dataset["source"]["receipt_sha256"], script)
        self.assertIn("fetch_publication_v3_dataset.py", script)
        self.assertIn("--roles query,ground-truth,metadata", script)
        self.assertNotIn("--roles train,", script)

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
