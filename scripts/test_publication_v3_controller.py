from __future__ import annotations

import base64
import copy
import gzip
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.publication_v3_aws import build_staging_receipt, staging_jobs
from scripts.publication_v3_controller import (
    AwsCli,
    BaseIndexAuthority,
    LaunchEnvironment,
    _minimum_lifecycle_write_ops,
    authenticate_v21_base_index_authority,
    completed_build_authority,
    prepare_qualification_execution,
    run_execution_job,
    select_execution_attempt,
    stage_dataset,
)
from scripts.publication_v3_execution import (
    ExecutionJob,
    borsuk_cell,
    qualification_cell,
)
from scripts.publication_v3_protocol import canonical_json_bytes, index_id
from scripts.publication_v3_receipts import build_index_receipt

MANIFEST = (
    Path(__file__).resolve().parents[1] / "docs/research/publication-v3-manifest.json"
)


def unstaged_sift_manifest() -> dict[str, object]:
    manifest = json.loads(MANIFEST.read_text())
    sift = next(item for item in manifest["datasets"] if item["id"] == "sift-128")
    sift["source"] = {
        "state": "unstaged",
        "expected_source": "https://ann-benchmarks.com/sift-128-euclidean.hdf5",
        "license": "upstream-dataset-license",
    }
    return manifest


def receipt_for(manifest: dict[str, object], attempt: int) -> dict[str, object]:
    job = next(
        j for j in staging_jobs(manifest, attempt=attempt) if j.dataset_id == "sift-128"
    )
    objects = [
        {
            "role": role,
            "format": "json" if role == "metadata" else "parquet",
            "uri": f"{job.output_uri}/{name}",
            "sha256": f"{ordinal:064x}",
            "bytes": 1024,
            "rows": rows,
        }
        for ordinal, (role, name, rows) in enumerate(
            (
                ("train", "train.parquet", 10),
                ("query", "test.parquet", 2),
                ("ground-truth", "neighbors.parquet", 2),
                ("metadata", "meta.json", 1),
            ),
            1,
        )
    ]
    identity = [
        {
            **{k: item[k] for k in ("role", "format", "sha256", "bytes", "rows")},
            "path": str(item["uri"]).removeprefix(job.output_uri + "/"),
        }
        for item in sorted(objects, key=lambda item: str(item["uri"]))
    ]
    content_sha = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
    return build_staging_receipt(
        manifest,
        job,
        source_archive_sha256="a" * 64,
        source_provenance={
            "schema_version": 1,
            "dataset": "sift-128",
            "source": "https://ann-benchmarks.com/sift-128-euclidean.hdf5",
            "source_sha256": "b" * 64,
            "materialization_sha256": content_sha,
        },
        provenance_sha256="c" * 64,
        objects=objects,
        instance_id="i-0123456789abcdef0",
        instance_type="r7g.8xlarge",
        availability_zone="eu-central-1a",
        purchase_option="spot",
    )


class FakeAws:
    def __init__(self, receipt: dict[str, object]) -> None:
        self.receipt = receipt
        self.launched: list[int] = []
        self.terminated: list[str] = []
        self.fresh_observations = 0

    def terminal_markers(self, job: object) -> tuple[str, ...]:
        if job.attempt in (1, 2):
            return ("STAGING_FAILED.json",)
        if job.attempt == 3:
            return ("STAGING_COMPLETE.json",)
        self.fresh_observations += 1
        return () if self.fresh_observations == 1 else ("STAGING_COMPLETE.json",)

    def read_receipt(self, job: object) -> dict[str, object]:
        if job.attempt == 3:
            return {
                **self.receipt,
                "attempt": 3,
                "manifest_sha256": "d" * 64,
                "source_archive_sha256": "e" * 64,
            }
        return self.receipt

    def find_instance(self, _job: object) -> tuple[str, str] | None:
        return None

    def launch(self, job: object, _request: dict[str, object]) -> str:
        self.launched.append(job.attempt)
        return "i-0123456789abcdef0"

    def instance_state(self, _instance_id: str) -> str:
        return "running"

    def terminate(self, instance_id: str) -> None:
        self.terminated.append(instance_id)

    def wait(self, _seconds: float) -> None:
        pass


class PublicationV3ControllerTests(unittest.TestCase):
    def _v21_base_authority_fixture(self):
        current = json.loads(MANIFEST.read_text())
        historical = copy.deepcopy(current)
        historical["source"] = {
            **historical["source"],
            "git_commit": "1" * 40,
            "archive_sha256": "2" * 64,
        }
        historical_sha256 = hashlib.sha256(canonical_json_bytes(historical)).hexdigest()
        cell = borsuk_cell(
            historical,
            workload_id="standard-ann-read",
            dataset_id="deep-image-96",
            repetition_id="r01",
            build_attempt=1,
        )
        protocol = canonical_json_bytes(cell) + b"\n"
        protocol_sha256 = hashlib.sha256(protocol).hexdigest()
        job = ExecutionJob.build(cell, attempt=1)
        rows = cell["dataset"]["scale"]["rows"]
        roster = [
            {
                "role": "data-bundle",
                "path": "segments/00000000.parquet",
                "format": "parquet",
                "bytes": 64 * 1024 * 1024,
                "rows": rows,
                "checksum": "3" * 64,
                "etag": '"33333333333333333333333333333333"',
            },
            {
                "role": "directory",
                "path": "directory/00000000.arrow",
                "format": "arrow-ipc",
                "bytes": 4096,
                "rows": 0,
                "checksum": "4" * 64,
                "etag": '"44444444444444444444444444444444"',
            },
        ]
        metrics = {
            "cpu_ns": 10,
            "peak_rss_bytes": 20,
            "disk_read_bytes": 30,
            "disk_write_bytes": 40,
            "storage_gets": 50,
            "storage_puts": 60,
            "storage_deletes": 0,
            "storage_heads": 2,
            "storage_lists": 1,
            "storage_bytes_read": 70,
            "storage_bytes_written": 80,
            "build_elapsed_ns": 90,
        }
        receipt = build_index_receipt(
            cell=cell,
            source_archive_sha256="2" * 64,
            dataset_materialization_sha256=cell["dataset"]["source"]["sha256"],
            build_attempt_id=f"{job.cell_tag}-a0001",
            builder_instance_identity="i-0123456789abcdef0",
            builder_instance_type="r7g.8xlarge",
            build_artifact={
                "index_stats": {
                    "logical_cells": cell["index_profile"]["logical_cells"],
                    "records": rows,
                    "total_active_index_bytes": 64 * 1024 * 1024,
                    "logical_cell_catalog_checksum": "5" * 64,
                },
                "storage_metrics": {
                    key: metrics[key]
                    for key in (
                        "storage_gets",
                        "storage_puts",
                        "storage_deletes",
                        "storage_heads",
                        "storage_lists",
                        "storage_bytes_read",
                        "storage_bytes_written",
                    )
                },
            },
            object_roster=roster,
            build_metrics=metrics,
        )
        inventory = [
            {key: item[key] for key in ("path", "bytes", "checksum", "etag")}
            for item in roster
        ]
        terminal = {
            "schema_version": 2,
            "status": "complete",
            "role": "build",
            "attempt": 1,
            "attempt_id": f"{job.cell_tag}-a0001",
            "instance_id": "i-0123456789abcdef0",
            "source_archive_sha256": "2" * 64,
            "manifest_sha256": historical_sha256,
            "protocol_sha256": protocol_sha256,
            "index_uri": job.index_uri,
            "binary_sha256": "6" * 64,
            "rest_binary_sha256": "7" * 64,
            "purchase_option": "spot",
            "artifact_upload_reconciliations": 0,
        }
        root = str(historical["prefixes"]["dataset"]).removesuffix("/datasets")
        objects = {
            job.complete_uri: canonical_json_bytes(terminal) + b"\n",
            f"{root}/manifests/{historical_sha256}.json": canonical_json_bytes(
                historical
            ),
            f"{root}/protocols/{protocol_sha256}.json": protocol,
            f"{job.terminal_prefix}/INDEX_COMPLETE.json": canonical_json_bytes(receipt)
            + b"\n",
            f"{job.terminal_prefix}/INDEX_OBJECTS.json": canonical_json_bytes(roster)
            + b"\n",
            f"{job.terminal_prefix}/INDEX_INVENTORY.json": canonical_json_bytes(
                inventory
            )
            + b"\n",
        }
        return current, historical, cell, job, objects

    def test_v21_base_authority_authenticates_distinct_historical_generation_before_writes(
        self,
    ) -> None:
        current, historical, cell, job, objects = self._v21_base_authority_fixture()

        class AuthorityAws:
            mutating_calls: list[str] = []

            def read_immutable_bytes(self, uri: str, sha256: str | None) -> bytes:
                payload = objects[uri]
                if sha256 is not None and hashlib.sha256(payload).hexdigest() != sha256:
                    raise ValueError("test authority digest differs")
                return payload

        terminal_sha256 = hashlib.sha256(objects[job.complete_uri]).hexdigest()
        authority = authenticate_v21_base_index_authority(
            current_manifest=current,
            terminal_uri=job.complete_uri,
            terminal_sha256=terminal_sha256,
            aws=AuthorityAws(),
            git_is_ancestor=lambda old, new: (
                old == historical["source"]["git_commit"]
                and new == current["source"]["git_commit"]
            ),
        )
        self.assertEqual(authority.build_cell_id, cell["cell_id"])
        self.assertEqual(authority.index_id, index_id(cell))
        self.assertEqual(authority.index_uri, cell["index_prefix"])
        self.assertEqual(authority.source_archive_sha256, "2" * 64)
        self.assertNotEqual(
            authority.source_archive_sha256,
            current["source"]["archive_sha256"],
        )
        self.assertEqual(AuthorityAws.mutating_calls, [])

    def test_v21_base_authority_mismatch_fails_before_any_write_or_ec2(
        self,
    ) -> None:
        current, historical, _cell, job, objects = self._v21_base_authority_fixture()
        receipt_uri = f"{job.terminal_prefix}/INDEX_COMPLETE.json"
        receipt = json.loads(objects[receipt_uri])
        receipt["index_uri"] = str(receipt["index_uri"]) + "-mutated"
        objects[receipt_uri] = canonical_json_bytes(receipt) + b"\n"

        class ReadOnlyAuthorityAws:
            mutating_calls: list[str] = []

            def read_immutable_bytes(self, uri: str, sha256: str | None) -> bytes:
                payload = objects[uri]
                if uri == receipt_uri:
                    return payload
                if sha256 is not None and hashlib.sha256(payload).hexdigest() != sha256:
                    raise ValueError("test authority digest differs")
                return payload

            def __getattr__(self, name: str):
                self.mutating_calls.append(name)
                raise AssertionError(
                    f"base preflight reached mutating AWS method {name}"
                )

        terminal_sha256 = hashlib.sha256(objects[job.complete_uri]).hexdigest()
        aws = ReadOnlyAuthorityAws()
        with self.assertRaisesRegex(ValueError, "base-index authority"):
            authenticate_v21_base_index_authority(
                current_manifest=current,
                terminal_uri=job.complete_uri,
                terminal_sha256=terminal_sha256,
                aws=aws,
                git_is_ancestor=lambda old, new: (
                    old == historical["source"]["git_commit"]
                    and new == current["source"]["git_commit"]
                ),
            )
        self.assertEqual(aws.mutating_calls, [])

    def test_v21_execution_uses_build_class_spot_and_binds_dual_authority(
        self,
    ) -> None:
        current, historical, _base_cell, base_job, objects = (
            self._v21_base_authority_fixture()
        )

        class AuthorityAws:
            def read_immutable_bytes(self, uri: str, sha256: str | None) -> bytes:
                payload = objects[uri]
                if sha256 is not None and hashlib.sha256(payload).hexdigest() != sha256:
                    raise ValueError("test authority digest differs")
                return payload

        base_authority = authenticate_v21_base_index_authority(
            current_manifest=current,
            terminal_uri=base_job.complete_uri,
            terminal_sha256=hashlib.sha256(objects[base_job.complete_uri]).hexdigest(),
            aws=AuthorityAws(),
            git_is_ancestor=lambda old, new: (
                old == historical["source"]["git_commit"]
                and new == current["source"]["git_commit"]
            ),
        )
        current_sha256 = hashlib.sha256(canonical_json_bytes(current)).hexdigest()
        current_cell = borsuk_cell(
            current,
            workload_id="standard-ann-read",
            dataset_id="deep-image-96",
            repetition_id="r01",
            build_attempt=1,
        )
        current_protocol_sha256 = hashlib.sha256(
            canonical_json_bytes(current_cell) + b"\n"
        ).hexdigest()

        class NoAws:
            def __getattr__(self, name: str):
                raise AssertionError(
                    f"prepared V21 execution reached AWS through {name}"
                )

        prepared = prepare_qualification_execution(
            current,
            operation="diagnose-v21-selector",
            workload_id="standard-ann-read",
            dataset_id="deep-image-96",
            repetition_id="r01",
            source_uri="s3://bucket/source/current.tar.gz",
            source_sha256=str(current["source"]["archive_sha256"]),
            manifest_uri="s3://bucket/manifests/current.json",
            manifest_sha256=current_sha256,
            protocol_uri="s3://bucket/protocols/current.json",
            protocol_sha256=current_protocol_sha256,
            launch=LaunchEnvironment(
                "ami-x",
                "subnet-x",
                "sg-x",
                "arn:aws:iam::453182569524:instance-profile/x",
                "aarch64",
                "eu-central-1",
            ),
            aws=NoAws(),
            attempt=2,
            build_attempt=1,
            purchase_option="spot",
            arm_index=0,
            v21_base_authority=base_authority,
        )
        self.assertEqual(prepared.request["InstanceType"], "r7g.8xlarge")
        self.assertEqual(prepared.expected["memory_max_bytes"], 34_359_738_368)
        self.assertEqual(prepared.expected["memory_swap_max_bytes"], 0)
        self.assertEqual(prepared.expected["base_index_id"], base_authority.index_id)
        self.assertEqual(
            prepared.expected["base_source_archive_sha256"],
            base_authority.source_archive_sha256,
        )
        self.assertEqual(
            prepared.expected["diagnostic_source_archive_sha256"],
            current["source"]["archive_sha256"],
        )

    def test_attempt_reconciliation_uses_exact_frozen_job_markers(self) -> None:
        class MarkerAws:
            def __init__(self) -> None:
                self.markers = {1: ("failed",), 3: ("complete",)}
                self.recorded: list[tuple[int, str, str]] = []

            def execution_markers(self, job: object):
                return self.markers.get(job.attempt, ())

            def find_execution_instance(self, job: object, *, purchase_option: str):
                self.purchase_option = purchase_option
                return ("i-stopped", "shutting-down") if job.attempt == 2 else None

            def record_markerless_execution_failure(
                self, job: object, *, instance_id: str, instance_state: str
            ) -> None:
                self.recorded.append((job.attempt, instance_id, instance_state))
                self.markers[job.attempt] = ("controller-failed",)

        selected = MarkerAws()
        self.assertEqual(
            select_execution_attempt(
                lambda attempt: type("Job", (), {"attempt": attempt})(),
                aws=selected,
                max_attempts=4,
            ),
            3,
        )
        self.assertEqual(selected.recorded, [(2, "i-stopped", "shutting-down")])
        skipped = MarkerAws()
        self.assertEqual(
            select_execution_attempt(
                lambda attempt: type("Job", (), {"attempt": attempt})(),
                aws=skipped,
                max_attempts=4,
                purchase_option="on-demand",
                require_complete=True,
            ),
            3,
        )
        self.assertEqual(skipped.purchase_option, "on-demand")
        self.assertEqual(skipped.recorded, [(2, "i-stopped", "shutting-down")])

        class CompletionRaceAws(MarkerAws):
            def record_markerless_execution_failure(
                self, job: object, *, instance_id: str, instance_state: str
            ) -> None:
                self.recorded.append((job.attempt, instance_id, instance_state))
                self.markers[job.attempt] = ("complete", "controller-failed")

        raced = CompletionRaceAws()
        raced.markers = {1: ("failed",)}
        self.assertEqual(
            select_execution_attempt(
                lambda attempt: type("Job", (), {"attempt": attempt})(),
                aws=raced,
                max_attempts=4,
            ),
            2,
        )

        class ConflictAws:
            def execution_markers(self, _job: object):
                return ("complete", "failed")

        with self.assertRaisesRegex(ValueError, "conflict"):
            select_execution_attempt(
                lambda attempt: type("Job", (), {"attempt": attempt})(),
                aws=ConflictAws(),
                max_attempts=4,
            )

    def test_controller_exposes_bounded_generic_read_commands(self) -> None:
        completed = subprocess.run(
            [sys.executable, "scripts/publication_v3_controller.py", "--help"],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("build-read", completed.stdout)
        self.assertIn("run-read", completed.stdout)

    def test_generic_r05_read_reuses_canonical_r01_build_authority(self) -> None:
        manifest = json.loads(MANIFEST.read_text())
        manifest["source"] = {
            "state": "frozen",
            "git_commit": "1" * 40,
            "archive_sha256": "2" * 64,
            "cargo_lock_sha256": "3" * 64,
            "python_lock_sha256": "4" * 64,
            "node_lock_sha256": "5" * 64,
        }
        build_cell = borsuk_cell(
            manifest,
            workload_id="standard-ann-read",
            dataset_id="sift-128",
            repetition_id="r01",
            build_attempt=1,
        )
        build_job = ExecutionJob.build(build_cell, attempt=1)

        class GenericReadAws:
            def execution_markers(self, job: object):
                self.observed_build = job
                return ("complete",)

            def read_receipt(self, _job: object):
                return {
                    "schema_version": 2,
                    "status": "complete",
                    "role": "build",
                    "attempt": 1,
                    "attempt_id": f"{build_job.cell_tag}-a0001",
                    "source_archive_sha256": "2" * 64,
                    "manifest_sha256": "6" * 64,
                    "protocol_sha256": "9" * 64,
                    "index_uri": build_job.index_uri,
                    "binary_sha256": "8" * 64,
                    "purchase_option": "spot",
                    "artifact_upload_reconciliations": 0,
                }

        aws = GenericReadAws()
        prepared = prepare_qualification_execution(
            manifest,
            operation="run-read",
            workload_id="standard-ann-read",
            dataset_id="sift-128",
            repetition_id="r05",
            source_uri="s3://bucket/source.tar.gz",
            source_sha256="2" * 64,
            manifest_uri="s3://bucket/manifest.json",
            manifest_sha256="6" * 64,
            protocol_uri="s3://bucket/protocol-r05.json",
            protocol_sha256="7" * 64,
            build_protocol_sha256="9" * 64,
            launch=LaunchEnvironment(
                "ami-x",
                "subnet-x",
                "sg-x",
                "arn:aws:iam::453182569524:instance-profile/x",
                "aarch64",
                "eu-central-1",
            ),
            aws=aws,
            attempt=3,
            build_attempt=1,
            arm_index=0,
        )

        self.assertEqual(aws.observed_build.cell["repetition_id"], "r01")
        self.assertEqual(prepared.job.cell["repetition_id"], "r05")
        self.assertEqual(prepared.job.index_uri, build_job.index_uri)
        self.assertIn(
            "/runtime-recall/arms/0000/attempts/0003",
            prepared.job.terminal_prefix,
        )
        self.assertEqual(prepared.expected["arm_index"], 0)

    def test_lifecycle_diagnostic_minimum_matches_runtime_integer_ceiling(self) -> None:
        self.assertEqual(
            _minimum_lifecycle_write_ops(
                writers=3,
                batch_size=64,
                update_percent=30,
                delete_percent=60,
            ),
            640,
        )

    def test_read_diagnostic_reuses_one_build_and_binds_the_complete_matrix(
        self,
    ) -> None:
        manifest = json.loads(MANIFEST.read_text())
        manifest["source"] = {
            "state": "frozen",
            "git_commit": "1" * 40,
            "archive_sha256": "2" * 64,
            "cargo_lock_sha256": "3" * 64,
            "python_lock_sha256": "4" * 64,
            "node_lock_sha256": "5" * 64,
        }
        build_cell = borsuk_cell(
            manifest,
            workload_id="standard-ann-read",
            dataset_id="deep-image-96",
            repetition_id="r01",
            build_attempt=1,
        )
        build_job = ExecutionJob.build(build_cell, attempt=1)

        class ReadDiagnosticAws:
            def execution_markers(self, _job: object):
                return ("complete",)

            def read_receipt(self, _job: object):
                return {
                    "schema_version": 2,
                    "status": "complete",
                    "role": "build",
                    "attempt": 1,
                    "attempt_id": f"{build_job.cell_tag}-a0001",
                    "source_archive_sha256": "2" * 64,
                    "manifest_sha256": "6" * 64,
                    "protocol_sha256": "9" * 64,
                    "index_uri": build_job.index_uri,
                    "binary_sha256": "8" * 64,
                    "purchase_option": "spot",
                    "artifact_upload_reconciliations": 0,
                }

        prepared = prepare_qualification_execution(
            manifest,
            operation="diagnose-read",
            workload_id="standard-ann-read",
            dataset_id="deep-image-96",
            repetition_id="r01",
            source_uri="s3://bucket/source.tar.gz",
            source_sha256="2" * 64,
            manifest_uri="s3://bucket/manifest.json",
            manifest_sha256="6" * 64,
            protocol_uri="s3://bucket/protocol.json",
            protocol_sha256="7" * 64,
            build_protocol_sha256="9" * 64,
            launch=LaunchEnvironment(
                "ami-x",
                "subnet-x",
                "sg-x",
                "arn:aws:iam::453182569524:instance-profile/x",
                "aarch64",
                "eu-central-1",
            ),
            aws=ReadDiagnosticAws(),
            attempt=2,
            build_attempt=1,
            diagnostic_read_nprobes=(32, 64),
            diagnostic_read_candidates=(512, 1_024, 2_048, 4_096),
        )

        self.assertTrue(
            prepared.job.terminal_prefix.endswith(
                "/runtime-read-diagnostic/arms/0000/attempts/0002"
            )
        )
        self.assertEqual(prepared.expected["claim_eligible"], False)
        self.assertEqual(prepared.expected["diagnostic_read_nprobes"], [32, 64])
        self.assertEqual(
            prepared.expected["diagnostic_read_candidates"],
            [512, 1_024, 2_048, 4_096],
        )
        user_data = base64.b64decode(prepared.request["UserData"]).decode()
        worker_payload = user_data.split("printf '%s' '", 1)[1].split("'", 1)[0]
        worker = gzip.decompress(base64.b64decode(worker_payload)).decode()
        self.assertIn("--diagnostic-read-nprobes 32,64", worker)
        self.assertIn("--diagnostic-read-candidates 512,1024,2048,4096", worker)
        for artifact in (
            "diagnostic_result_sha256",
            "diagnostic_samples_sha256",
            "diagnostic_summary_sha256",
        ):
            self.assertIn(f'"{artifact}":"%s"', worker)
        self.assertEqual(
            subprocess.run(["bash", "-n"], input=worker, text=True).returncode,
            0,
        )

        class ReadDiagnosticReceiptAws:
            def __init__(self, *, include_summary_digest: bool) -> None:
                self.include_summary_digest = include_summary_digest

            def find_execution_instance(
                self, _job: object, *, purchase_option: str
            ) -> None:
                return None

            def execution_markers(self, _job: object):
                return ("complete",)

            def read_receipt(self, _job: object):
                receipt = {
                    "schema_version": 5,
                    "status": "complete",
                    "role": "runtime",
                    "attempt": 2,
                    **prepared.expected,
                    "execution_contract_sha256": "9" * 64,
                    "diagnostic_result_sha256": "1" * 64,
                    "diagnostic_samples_sha256": "2" * 64,
                    "artifact_upload_reconciliations": 0,
                }
                if self.include_summary_digest:
                    receipt["diagnostic_summary_sha256"] = "3" * 64
                return receipt

            def terminate(self, _instance: str) -> None:
                raise AssertionError("completed observation has no active instance")

        with self.assertRaisesRegex(ValueError, "artifact digest"):
            run_execution_job(
                prepared.job,
                request=prepared.request,
                expected=prepared.expected,
                aws=ReadDiagnosticReceiptAws(include_summary_digest=False),
                timeout_seconds=60,
                poll_seconds=0.01,
                purchase_option="spot",
            )
        completed = run_execution_job(
            prepared.job,
            request=prepared.request,
            expected=prepared.expected,
            aws=ReadDiagnosticReceiptAws(include_summary_digest=True),
            timeout_seconds=60,
            poll_seconds=0.01,
            purchase_option="spot",
        )
        self.assertEqual(completed["diagnostic_summary_sha256"], "3" * 64)

    def test_v21_selector_diagnostic_is_spot_only_and_reuses_exact_deep_image_build(
        self,
    ) -> None:
        manifest = json.loads(MANIFEST.read_text())
        manifest["source"] = {
            "state": "frozen",
            "git_commit": "1" * 40,
            "archive_sha256": "2" * 64,
            "cargo_lock_sha256": "3" * 64,
            "python_lock_sha256": "4" * 64,
            "node_lock_sha256": "5" * 64,
        }
        manifest_sha256 = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
        build_cell = borsuk_cell(
            manifest,
            workload_id="standard-ann-read",
            dataset_id="deep-image-96",
            repetition_id="r01",
            build_attempt=1,
        )
        build_job = ExecutionJob.build(build_cell, attempt=1)

        class V21Aws:
            def execution_markers(self, _job: object):
                return ("complete",)

            def read_receipt(self, _job: object):
                return {
                    "schema_version": 2,
                    "status": "complete",
                    "role": "build",
                    "attempt": 1,
                    "attempt_id": f"{build_job.cell_tag}-a0001",
                    "source_archive_sha256": "2" * 64,
                    "manifest_sha256": manifest_sha256,
                    "protocol_sha256": "9" * 64,
                    "index_uri": build_job.index_uri,
                    "binary_sha256": "8" * 64,
                    "purchase_option": "spot",
                    "artifact_upload_reconciliations": 0,
                }

        launch = LaunchEnvironment(
            "ami-x",
            "subnet-x",
            "sg-x",
            "arn:aws:iam::453182569524:instance-profile/x",
            "aarch64",
            "eu-central-1",
        )
        arguments = {
            "operation": "diagnose-v21-selector",
            "workload_id": "standard-ann-read",
            "dataset_id": "deep-image-96",
            "repetition_id": "r01",
            "source_uri": "s3://bucket/source.tar.gz",
            "source_sha256": "2" * 64,
            "manifest_uri": "s3://bucket/manifest.json",
            "manifest_sha256": manifest_sha256,
            "protocol_uri": "s3://bucket/protocol.json",
            "protocol_sha256": "7" * 64,
            "build_protocol_sha256": "9" * 64,
            "launch": launch,
            "aws": V21Aws(),
            "attempt": 2,
            "build_attempt": 1,
            "arm_index": 0,
            "v21_base_authority": BaseIndexAuthority(
                manifest=manifest,
                manifest_uri="s3://bucket/manifests/base.json",
                manifest_sha256=manifest_sha256,
                protocol_uri="s3://bucket/protocols/base.json",
                protocol_sha256="9" * 64,
                source_uri="s3://bucket/source/base.tar.gz",
                source_archive_sha256="2" * 64,
                source_git_commit="1" * 40,
                build_terminal_uri=build_job.complete_uri,
                build_terminal_sha256="a" * 64,
                build_prefix=build_job.terminal_prefix,
                build_cell_id=str(build_cell["cell_id"]),
                build_attempt=1,
                index_id=index_id(build_cell),
                index_uri=build_job.index_uri,
                index_receipt_sha256="b" * 64,
                object_roster_sha256="c" * 64,
                inventory_sha256="d" * 64,
            ),
        }
        prepared = prepare_qualification_execution(
            manifest, purchase_option="spot", **arguments
        )
        self.assertTrue(
            prepared.job.terminal_prefix.endswith(
                "/runtime-v21-feasibility/arms/0000/attempts/0002"
            )
        )
        self.assertEqual(prepared.expected["claim_eligible"], False)
        self.assertEqual(prepared.expected["v21_feasibility"], True)
        self.assertEqual(prepared.expected["ram_budget_bytes"], 3 * 1024**3)
        self.assertEqual(
            prepared.timeout_seconds,
            manifest["budget_contract"]["max_cell_seconds"],
        )
        user_data = base64.b64decode(prepared.request["UserData"]).decode()
        worker_payload = user_data.split("printf '%s' '", 1)[1].split("'", 1)[0]
        worker = gzip.decompress(base64.b64decode(worker_payload)).decode()
        self.assertIn("--v21-feasibility", worker)
        for artifact in (
            "v21_result_sha256",
            "v21_arms_sha256",
            "v21_samples_sha256",
            "v21_summary_sha256",
        ):
            self.assertIn(f'"{artifact}":"%s"', worker)

        class V21ReceiptAws:
            def __init__(self, *, complete: bool) -> None:
                self.complete = complete

            def find_execution_instance(
                self, _job: object, *, purchase_option: str
            ) -> None:
                return None

            def execution_markers(self, _job: object):
                return ("complete",)

            def read_receipt(self, _job: object):
                receipt = {
                    "schema_version": 5,
                    "status": "complete",
                    "role": "runtime",
                    "attempt": 2,
                    **prepared.expected,
                    "execution_contract_sha256": "9" * 64,
                    "v21_result_sha256": "1" * 64,
                    "v21_arms_sha256": "2" * 64,
                    "v21_samples_sha256": "3" * 64,
                    "artifact_upload_reconciliations": 0,
                    "binary_sha256": "8" * 64,
                    "memory_peak_bytes": 1_024,
                }
                if self.complete:
                    receipt["v21_summary_sha256"] = "4" * 64
                return receipt

            def terminate(self, _instance: str) -> None:
                raise AssertionError("completed observation has no active instance")

        with self.assertRaisesRegex(ValueError, "artifact digest"):
            run_execution_job(
                prepared.job,
                request=prepared.request,
                expected=prepared.expected,
                aws=V21ReceiptAws(complete=False),
                timeout_seconds=60,
                poll_seconds=0.01,
                purchase_option="spot",
            )
        completed = run_execution_job(
            prepared.job,
            request=prepared.request,
            expected=prepared.expected,
            aws=V21ReceiptAws(complete=True),
            timeout_seconds=60,
            poll_seconds=0.01,
            purchase_option="spot",
        )
        self.assertEqual(completed["v21_summary_sha256"], "4" * 64)

        class NoAws:
            def __getattr__(self, name: str):
                raise AssertionError(f"unfrozen authority reached AWS through {name}")

        unfrozen = copy.deepcopy(manifest)
        unfrozen["source"] = {"state": "unfrozen"}
        unfrozen_arguments = {**arguments, "aws": NoAws()}
        with self.assertRaisesRegex(ValueError, "V21 selector diagnostic"):
            prepare_qualification_execution(
                unfrozen, purchase_option="spot", **unfrozen_arguments
            )
        with self.assertRaisesRegex(ValueError, "V21 selector diagnostic"):
            prepare_qualification_execution(
                manifest, purchase_option="on-demand", **arguments
            )

    def test_lifecycle_diagnostic_is_bounded_claim_ineligible_and_namespaced(
        self,
    ) -> None:
        manifest = json.loads(MANIFEST.read_text())
        manifest["source"] = {
            "state": "frozen",
            "git_commit": "1" * 40,
            "archive_sha256": "2" * 64,
            "cargo_lock_sha256": "3" * 64,
            "python_lock_sha256": "4" * 64,
            "node_lock_sha256": "5" * 64,
        }
        cell = qualification_cell(
            manifest,
            dataset_id="sift-128",
            workload_kind="write-update-delete-compact",
            build_attempt=1,
        )
        build_job = ExecutionJob.build(cell, attempt=1)

        class LifecycleDiagnosticAws:
            def execution_markers(self, _job: object):
                return ("complete",)

            def read_receipt(self, _job: object):
                return {
                    "schema_version": 2,
                    "status": "complete",
                    "role": "build",
                    "attempt": 1,
                    "attempt_id": f"{build_job.cell_tag}-a0001",
                    "source_archive_sha256": "2" * 64,
                    "manifest_sha256": "6" * 64,
                    "protocol_sha256": "7" * 64,
                    "index_uri": build_job.index_uri,
                    "binary_sha256": "8" * 64,
                    "purchase_option": "spot",
                    "artifact_upload_reconciliations": 0,
                }

        diagnostic = prepare_qualification_execution(
            manifest,
            operation="diagnose-lifecycle",
            dataset_id="sift-128",
            source_uri="s3://bucket/source.tar.gz",
            source_sha256="2" * 64,
            manifest_uri="s3://bucket/manifest.json",
            manifest_sha256="6" * 64,
            protocol_uri="s3://bucket/protocol.json",
            protocol_sha256="7" * 64,
            launch=LaunchEnvironment(
                "ami-x",
                "subnet-x",
                "sg-x",
                "arn:aws:iam::453182569524:instance-profile/x",
                "aarch64",
                "eu-central-1",
            ),
            aws=LifecycleDiagnosticAws(),
            attempt=2,
            build_attempt=1,
        )

        self.assertTrue(
            diagnostic.job.terminal_prefix.endswith(
                "/runtime-lifecycle-diagnostic/arms/0013/attempts/0002"
            )
        )
        self.assertEqual(diagnostic.timeout_seconds, 1_200)
        self.assertEqual(diagnostic.expected["claim_eligible"], False)
        self.assertEqual(diagnostic.expected["diagnostic_write_ops"], 2_560)
        self.assertEqual(diagnostic.expected["diagnostic_timeout_seconds"], 1_200)
        user_data = base64.b64decode(diagnostic.request["UserData"]).decode()
        worker_payload = user_data.split("printf '%s' '", 1)[1].split("'", 1)[0]
        worker = gzip.decompress(base64.b64decode(worker_payload)).decode()
        self.assertIn("--runtime-profile lifecycle", worker)
        self.assertIn("--diagnostic-write-ops 2560", worker)
        self.assertIn('"claim_eligible":false', worker)
        self.assertIn('"diagnostic_write_ops":2560', worker)
        self.assertIn('"diagnostic_timeout_seconds":1200', worker)
        self.assertIn('["diagnostic_write_ops"]', worker)

        with self.assertRaisesRegex(
            ValueError,
            "lifecycle diagnostic write count must exercise every writer",
        ):
            prepare_qualification_execution(
                manifest,
                operation="diagnose-lifecycle",
                dataset_id="sift-128",
                source_uri="s3://bucket/source.tar.gz",
                source_sha256="2" * 64,
                manifest_uri="s3://bucket/manifest.json",
                manifest_sha256="6" * 64,
                protocol_uri="s3://bucket/protocol.json",
                protocol_sha256="7" * 64,
                launch=LaunchEnvironment(
                    "ami-x",
                    "subnet-x",
                    "sg-x",
                    "arn:aws:iam::453182569524:instance-profile/x",
                    "aarch64",
                    "eu-central-1",
                ),
                aws=LifecycleDiagnosticAws(),
                attempt=2,
                build_attempt=1,
                diagnostic_write_ops=2_559,
            )
        self.assertIn('["claim_eligible"]', worker)
        self.assertIn('"diagnostic_result_sha256":"%s"', worker)
        self.assertEqual(
            subprocess.run(["bash", "-n"], input=worker, text=True).returncode, 0
        )

        class DiagnosticReceiptAws:
            def __init__(self, *, include_result_digest: bool) -> None:
                self.include_result_digest = include_result_digest

            def find_execution_instance(
                self, _job: object, *, purchase_option: str
            ) -> None:
                return None

            def execution_markers(self, _job: object):
                return ("complete",)

            def read_receipt(self, _job: object):
                receipt = {
                    "schema_version": 5,
                    "status": "complete",
                    "role": "runtime",
                    "attempt": 2,
                    **diagnostic.expected,
                    "execution_contract_sha256": "9" * 64,
                    "lifecycle_summary_sha256": "8" * 64,
                    "lifecycle_costs_sha256": "7" * 64,
                    "lifecycle_samples_sha256": "6" * 64,
                    "lifecycle_query_summary_sha256": "5" * 64,
                    "lifecycle_query_samples_sha256": "4" * 64,
                    "lifecycle_storage_trace_sha256": "3" * 64,
                    "artifact_upload_reconciliations": 0,
                }
                if self.include_result_digest:
                    receipt["diagnostic_result_sha256"] = "1" * 64
                return receipt

            def terminate(self, _instance: str) -> None:
                raise AssertionError("completed observation has no active instance")

        with self.assertRaisesRegex(ValueError, "artifact digest"):
            run_execution_job(
                diagnostic.job,
                request=diagnostic.request,
                expected=diagnostic.expected,
                aws=DiagnosticReceiptAws(include_result_digest=False),
                timeout_seconds=60,
                poll_seconds=0.01,
                purchase_option="spot",
            )
        completed = run_execution_job(
            diagnostic.job,
            request=diagnostic.request,
            expected=diagnostic.expected,
            aws=DiagnosticReceiptAws(include_result_digest=True),
            timeout_seconds=60,
            poll_seconds=0.01,
            purchase_option="spot",
        )
        self.assertEqual(completed["diagnostic_result_sha256"], "1" * 64)

    def test_lifecycle_build_and_mutation_runtime_are_immutable_clone_bound(
        self,
    ) -> None:
        manifest = json.loads(MANIFEST.read_text())
        manifest["source"] = {
            "state": "frozen",
            "git_commit": "1" * 40,
            "archive_sha256": "2" * 64,
            "cargo_lock_sha256": "3" * 64,
            "python_lock_sha256": "4" * 64,
            "node_lock_sha256": "5" * 64,
        }
        cell = qualification_cell(
            manifest,
            dataset_id="sift-128",
            workload_kind="write-update-delete-compact",
            build_attempt=1,
        )
        build_job = ExecutionJob.build(cell, attempt=1)

        class LifecycleAws:
            def execution_markers(self, _job: object):
                return ("complete",)

            def read_receipt(self, _job: object):
                return {
                    "schema_version": 2,
                    "status": "complete",
                    "role": "build",
                    "attempt": 1,
                    "attempt_id": f"{build_job.cell_tag}-a0001",
                    "source_archive_sha256": "2" * 64,
                    "manifest_sha256": "6" * 64,
                    "protocol_sha256": "7" * 64,
                    "index_uri": build_job.index_uri,
                    "binary_sha256": "8" * 64,
                    "purchase_option": "spot",
                    "artifact_upload_reconciliations": 0,
                }

        common = {
            "manifest": manifest,
            "source_uri": "s3://bucket/source.tar.gz",
            "source_sha256": "2" * 64,
            "manifest_uri": "s3://bucket/manifest.json",
            "manifest_sha256": "6" * 64,
            "protocol_uri": "s3://bucket/protocol.json",
            "protocol_sha256": "7" * 64,
            "launch": LaunchEnvironment(
                "ami-x",
                "subnet-x",
                "sg-x",
                "arn:aws:iam::453182569524:instance-profile/x",
                "aarch64",
                "eu-central-1",
            ),
            "aws": LifecycleAws(),
            "dataset_id": "sift-128",
        }
        build = prepare_qualification_execution(
            **common, operation="build-lifecycle", attempt=1
        )
        self.assertEqual(build.job.index_uri, build_job.index_uri)
        self.assertEqual(
            build.job.cell["workload"]["kind"], "write-update-delete-compact"
        )
        runtime = prepare_qualification_execution(
            **common,
            operation="run-lifecycle",
            attempt=1,
            build_attempt=1,
            arm_index=4,
        )
        self.assertTrue(
            runtime.job.terminal_prefix.endswith(
                "/runtime-lifecycle/arms/0004/attempts/0001"
            )
        )
        user_data = base64.b64decode(runtime.request["UserData"]).decode()
        worker_payload = user_data.split("printf '%s' '", 1)[1].split("'", 1)[0]
        worker = gzip.decompress(base64.b64decode(worker_payload)).decode()
        self.assertIn("clone_publication_v3_index.py", worker)
        self.assertIn("--clone-receipt", worker)
        self.assertIn("--clone-inventory", worker)
        self.assertIn("--runtime-profile lifecycle", worker)
        self.assertIn("--arm-index 4", worker)
        self.assertIn('"writers":4', worker)
        self.assertIn('"batch_size":64', worker)
        self.assertIn('"insert_mode":"general-upsert"', worker)
        self.assertLess(
            worker.index('put_immutable "$work/CLONE_COMPLETE.json"'),
            worker.index("stage=execute-runtime"),
        )
        self.assertIn("bench_lifecycle.csv", worker)
        self.assertIn("bench_write_costs.csv", worker)
        self.assertIn("bench_write_samples.csv", worker)
        self.assertIn("bench_mutation_queries.csv", worker)
        self.assertIn("bench_mutation_query_samples.csv", worker)
        self.assertIn("storage-access.csv", worker)
        self.assertIn('"lifecycle_summary_sha256"', worker)
        self.assertIn('"lifecycle_costs_sha256"', worker)
        self.assertIn('"lifecycle_samples_sha256"', worker)
        self.assertIn('"lifecycle_query_summary_sha256"', worker)
        self.assertIn('"lifecycle_query_samples_sha256"', worker)
        self.assertIn('"lifecycle_storage_trace_sha256"', worker)
        self.assertEqual(runtime.expected["runtime_profile"], "lifecycle")
        self.assertEqual(runtime.expected["arm_index"], 4)
        self.assertEqual(runtime.expected["purchase_option"], "spot")

        candidate = prepare_qualification_execution(
            **common,
            operation="run-lifecycle",
            attempt=1,
            build_attempt=1,
            arm_index=13,
        )
        self.assertTrue(
            candidate.job.terminal_prefix.endswith(
                "/runtime-lifecycle/arms/0013/attempts/0001"
            )
        )
        candidate_user_data = base64.b64decode(candidate.request["UserData"]).decode()
        candidate_payload = candidate_user_data.split("printf '%s' '", 1)[1].split(
            "'", 1
        )[0]
        candidate_worker = gzip.decompress(base64.b64decode(candidate_payload)).decode()
        self.assertIn('"writers":4', candidate_worker)
        self.assertIn('"batch_size":64', candidate_worker)
        self.assertIn('"insert_mode":"claim-free-put"', candidate_worker)
        self.assertEqual(candidate.expected["runtime_profile"], "lifecycle")

        class IncompleteLifecycleReceiptAws:
            def find_execution_instance(
                self, _job: object, *, purchase_option: str
            ) -> None:
                self.purchase_option = purchase_option
                return None

            def execution_markers(self, _job: object):
                return ("complete",)

            def read_receipt(self, _job: object):
                return {
                    "schema_version": 5,
                    "status": "complete",
                    "role": "runtime",
                    "attempt": 1,
                    **runtime.expected,
                    "execution_contract_sha256": "9" * 64,
                    "lifecycle_summary_sha256": "8" * 64,
                    "lifecycle_costs_sha256": "7" * 64,
                    "lifecycle_samples_sha256": "6" * 64,
                    "artifact_upload_reconciliations": 0,
                }

            def terminate(self, _instance: str) -> None:
                raise AssertionError("completed observation has no active instance")

        with self.assertRaisesRegex(ValueError, "artifact digest"):
            run_execution_job(
                runtime.job,
                request=runtime.request,
                expected=runtime.expected,
                aws=IncompleteLifecycleReceiptAws(),
                timeout_seconds=60,
                poll_seconds=0.01,
                purchase_option="spot",
            )

    def test_runtime_plan_uses_small_host_exact_build_and_distinct_retry_attempt(
        self,
    ) -> None:
        manifest = json.loads(MANIFEST.read_text())
        manifest["source"] = {
            "state": "frozen",
            "git_commit": "1" * 40,
            "archive_sha256": "2" * 64,
            "cargo_lock_sha256": "3" * 64,
            "python_lock_sha256": "4" * 64,
            "node_lock_sha256": "5" * 64,
        }
        cell = qualification_cell(
            manifest,
            dataset_id="sift-128",
            workload_kind="read-recall",
            build_attempt=1,
        )
        build_job = ExecutionJob.build(cell, attempt=1)

        class RuntimePlanAws:
            def execution_markers(self, _job: object):
                return ("complete",)

            def read_receipt(self, _job: object):
                return {
                    "schema_version": 2,
                    "status": "complete",
                    "role": "build",
                    "attempt": 1,
                    "attempt_id": f"{build_job.cell_tag}-a0001",
                    "source_archive_sha256": "2" * 64,
                    "manifest_sha256": "6" * 64,
                    "protocol_sha256": "7" * 64,
                    "index_uri": build_job.index_uri,
                    "binary_sha256": "8" * 64,
                    "purchase_option": "spot",
                    "artifact_upload_reconciliations": 0,
                }

        prepared = prepare_qualification_execution(
            manifest,
            operation="read-recall-sift",
            source_uri="s3://bucket/source.tar.gz",
            source_sha256="2" * 64,
            manifest_uri="s3://bucket/manifest.json",
            manifest_sha256="6" * 64,
            protocol_uri="s3://bucket/protocol.json",
            protocol_sha256="7" * 64,
            launch=LaunchEnvironment(
                "ami-x",
                "subnet-x",
                "sg-x",
                "arn:aws:iam::453182569524:instance-profile/x",
                "aarch64",
                "eu-central-1",
            ),
            aws=RuntimePlanAws(),
            attempt=2,
            build_attempt=1,
            purchase_option="on-demand",
        )
        self.assertEqual(prepared.job.role, "runtime")
        self.assertEqual(prepared.job.attempt, 2)
        self.assertTrue(
            prepared.job.terminal_prefix.endswith(
                "/runtime-recall/arms/0000/attempts/0002"
            )
        )
        self.assertEqual(prepared.request["InstanceType"], "c7g.xlarge")
        self.assertEqual(len(prepared.request["BlockDeviceMappings"]), 2)
        tags = {
            item["Key"]: item["Value"]
            for item in prepared.request["TagSpecifications"][0]["Tags"]
        }
        self.assertEqual(tags["Role"], "runtime")
        self.assertEqual(tags["PurchaseOption"], "on-demand")
        self.assertNotIn("InstanceMarketOptions", prepared.request)
        self.assertEqual(tags["Cell"], prepared.job.cell_tag)
        user_data = base64.b64decode(prepared.request["UserData"]).decode()
        self.assertLess(len(prepared.request["UserData"].encode()), 16 * 1024)
        worker_payload = user_data.split("printf '%s' '", 1)[1].split("'", 1)[0]
        worker = gzip.decompress(base64.b64decode(worker_payload)).decode()
        self.assertIn(build_job.terminal_prefix, worker)
        self.assertIn("8" * 64, worker)
        self.assertEqual(prepared.timeout_seconds, 7200)
        self.assertEqual(prepared.expected["binary_sha256"], "8" * 64)
        self.assertEqual(prepared.expected["purchase_option"], "on-demand")
        self.assertEqual(prepared.expected["max_parallel_decode_rank_tasks"], 2)

        concurrency = prepare_qualification_execution(
            manifest,
            operation="read-concurrency-sift",
            source_uri="s3://bucket/source.tar.gz",
            source_sha256="2" * 64,
            manifest_uri="s3://bucket/manifest.json",
            manifest_sha256="6" * 64,
            protocol_uri="s3://bucket/protocol.json",
            protocol_sha256="7" * 64,
            launch=LaunchEnvironment(
                "ami-x",
                "subnet-x",
                "sg-x",
                "arn:aws:iam::453182569524:instance-profile/x",
                "aarch64",
                "eu-central-1",
            ),
            aws=RuntimePlanAws(),
            attempt=3,
            build_attempt=1,
            arm_index=1,
        )
        concurrency_user_data = base64.b64decode(
            concurrency.request["UserData"]
        ).decode()
        self.assertLess(len(concurrency.request["UserData"].encode()), 16 * 1024)
        concurrency_payload = concurrency_user_data.split("printf '%s' '", 1)[1].split(
            "'", 1
        )[0]
        concurrency_worker = gzip.decompress(
            base64.b64decode(concurrency_payload)
        ).decode()
        self.assertIn("--runtime-profile concurrency", concurrency_worker)
        self.assertIn("--arm-index 1", concurrency_worker)
        self.assertEqual(concurrency.expected["runtime_profile"], "concurrency")
        self.assertEqual(concurrency.expected["arm_index"], 1)
        self.assertEqual(concurrency.expected["max_active_searches"], 16)
        self.assertEqual(concurrency.expected["max_waiting_searches"], 64)
        self.assertEqual(concurrency.expected["leaf_read_width"], 32)
        self.assertEqual(concurrency.expected["max_inflight_leaf_reads"], 96)
        self.assertEqual(concurrency.expected["max_parallel_decode_rank_tasks"], 2)
        self.assertEqual(concurrency.expected["disk_cache_max_bytes"], 68719476736)
        self.assertEqual(
            concurrency.expected["exact_read_max_physical_amplification"], 2
        )
        for argument in (
            "--disk-cache-max-bytes 68719476736",
            "--exact-read-max-physical-amplification 2",
            "--max-active-searches 16",
            "--max-waiting-searches 64",
            "--leaf-read-width 32",
            "--max-inflight-leaf-reads 96",
            "--max-parallel-decode-rank-tasks 2",
            "--cpu-threads 3",
            "--io-threads 160",
            "--s3-get-concurrency 128",
            "--ram-budget-bytes 3221225472",
        ):
            self.assertIn(argument, concurrency_worker)
        self.assertIn(
            'test "$actual_max_parallel_decode_rank_tasks" = 2',
            concurrency_worker,
        )
        self.assertEqual(concurrency.expected["cpu_threads"], 3)
        self.assertEqual(concurrency.expected["io_threads"], 160)
        self.assertEqual(concurrency.expected["s3_get_concurrency"], 128)
        self.assertIn(
            "/runtime-concurrency/arms/0001/attempts/0003",
            concurrency.job.terminal_prefix,
        )
        self.assertTrue(concurrency.job.cell_tag.startswith("runtime-concurrency-"))

    def test_runtime_requires_exact_completed_build_authority(self) -> None:
        manifest = unstaged_sift_manifest()
        manifest["source"] = {
            "state": "frozen",
            "git_commit": "1" * 40,
            "archive_sha256": "2" * 64,
            "cargo_lock_sha256": "3" * 64,
            "python_lock_sha256": "4" * 64,
            "node_lock_sha256": "5" * 64,
        }
        cell = qualification_cell(
            manifest, dataset_id="sift-128", workload_kind="read-recall"
        )
        job = ExecutionJob.build(cell, attempt=1)

        class BuildAuthorityAws:
            def execution_markers(self, _job: object):
                return ("complete",)

            def read_receipt(self, _job: object):
                return {
                    "schema_version": 2,
                    "status": "complete",
                    "role": "build",
                    "attempt": 1,
                    "attempt_id": f"{job.cell_tag}-a0001",
                    "source_archive_sha256": "2" * 64,
                    "manifest_sha256": "6" * 64,
                    "protocol_sha256": "7" * 64,
                    "index_uri": job.index_uri,
                    "binary_sha256": "8" * 64,
                    "purchase_option": "spot",
                    "artifact_upload_reconciliations": 0,
                }

        authority = completed_build_authority(
            job,
            aws=BuildAuthorityAws(),
            expected={
                "source_archive_sha256": "2" * 64,
                "manifest_sha256": "6" * 64,
                "protocol_sha256": "7" * 64,
            },
        )
        self.assertEqual(authority["binary_sha256"], "8" * 64)
        self.assertEqual(authority["build_prefix"], job.terminal_prefix)

        for markers, message in (
            ((), "not complete"),
            (("complete", "failed"), "conflict"),
            (("unknown",), "differ"),
        ):
            aws = BuildAuthorityAws()
            aws.execution_markers = lambda _job, value=markers: value
            with (
                self.subTest(markers=markers),
                self.assertRaisesRegex(ValueError, message),
            ):
                completed_build_authority(
                    job,
                    aws=aws,
                    expected={
                        "source_archive_sha256": "2" * 64,
                        "manifest_sha256": "6" * 64,
                        "protocol_sha256": "7" * 64,
                    },
                )

        aws = BuildAuthorityAws()
        original_receipt = aws.read_receipt(job)
        for field, value, message in (
            ("status", "failed", "differs from frozen"),
            ("attempt_id", "wrong", "differs from frozen"),
            ("index_uri", "s3://wrong/index", "differs from frozen"),
            ("source_archive_sha256", "9" * 64, "differs from frozen"),
            ("manifest_sha256", "9" * 64, "differs from frozen"),
            ("protocol_sha256", "9" * 64, "differs from frozen"),
            ("purchase_option", "on-demand", "differs from frozen"),
            ("binary_sha256", "short", "canonical binary"),
            ("binary_sha256", "A" * 64, "canonical binary"),
        ):
            candidate = {**original_receipt, field: value}
            aws.read_receipt = lambda _job, receipt=candidate: receipt
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, message):
                completed_build_authority(
                    job,
                    aws=aws,
                    expected={
                        "source_archive_sha256": "2" * 64,
                        "manifest_sha256": "6" * 64,
                        "protocol_sha256": "7" * 64,
                    },
                )
        with self.assertRaisesRegex(ValueError, "requires a build job"):
            completed_build_authority(
                ExecutionJob.runtime(cell, attempt=1),
                aws=BuildAuthorityAws(),
                expected={},
            )

    def test_execution_job_launches_once_accepts_bound_terminal_and_terminates(
        self,
    ) -> None:
        manifest = unstaged_sift_manifest()
        manifest["source"] = {
            "state": "frozen",
            "git_commit": "1" * 40,
            "archive_sha256": "2" * 64,
            "cargo_lock_sha256": "3" * 64,
            "python_lock_sha256": "4" * 64,
            "node_lock_sha256": "5" * 64,
        }
        cell = qualification_cell(
            manifest, dataset_id="sift-128", workload_kind="read-recall"
        )
        job = ExecutionJob.build(cell, attempt=1)

        class FakeExecutionAws:
            def __init__(self) -> None:
                self.launched = 0
                self.terminated: list[str] = []
                self.observations = 0

            def find_execution_instance(self, _job: object, *, purchase_option: str):
                self.assert_purchase_option = purchase_option
                return None

            def execution_markers(self, _job: object):
                self.observations += 1
                return () if self.observations == 1 else ("complete",)

            def read_receipt(self, _job: object):
                return {
                    "schema_version": 2,
                    "status": "complete",
                    "role": "build",
                    "attempt": 1,
                    "attempt_id": "build-0001",
                    "instance_id": "i-0123456789abcdef0",
                    "source_archive_sha256": "2" * 64,
                    "manifest_sha256": "6" * 64,
                    "protocol_sha256": "7" * 64,
                    "index_uri": job.index_uri,
                    "binary_sha256": "8" * 64,
                    "purchase_option": "spot",
                    "artifact_upload_reconciliations": 0,
                }

            def launch(self, _job: object, _request: object):
                self.launched += 1
                return "i-0123456789abcdef0"

            def instance_state(self, _instance: str):
                return "running"

            def terminate(self, instance: str):
                self.terminated.append(instance)

            def wait(self, _seconds: float):
                pass

        aws = FakeExecutionAws()
        with self.assertRaisesRegex(ValueError, "purchase option"):
            run_execution_job(
                job,
                request={"InstanceType": "r7g.8xlarge"},
                expected={"purchase_option": "on-demand"},
                aws=aws,
                timeout_seconds=60,
                poll_seconds=0.01,
                purchase_option="spot",
            )
        receipt = run_execution_job(
            job,
            request={"InstanceType": "r7g.8xlarge"},
            expected={
                "attempt_id": "build-0001",
                "source_archive_sha256": "2" * 64,
                "manifest_sha256": "6" * 64,
                "protocol_sha256": "7" * 64,
                "purchase_option": "spot",
            },
            aws=aws,
            timeout_seconds=60,
            poll_seconds=0.01,
            purchase_option="spot",
        )
        self.assertEqual(receipt["binary_sha256"], "8" * 64)
        self.assertEqual(aws.launched, 1)
        self.assertEqual(aws.terminated, ["i-0123456789abcdef0"])
        self.assertEqual(aws.assert_purchase_option, "spot")

    def test_runtime_receipt_must_match_imds_observed_purchase_option(self) -> None:
        manifest = unstaged_sift_manifest()
        manifest["source"] = {
            "state": "frozen",
            "git_commit": "1" * 40,
            "archive_sha256": "2" * 64,
            "cargo_lock_sha256": "3" * 64,
            "python_lock_sha256": "4" * 64,
            "node_lock_sha256": "5" * 64,
        }
        job = ExecutionJob.runtime(
            qualification_cell(
                manifest, dataset_id="sift-128", workload_kind="read-recall"
            ),
            attempt=1,
        )

        class RuntimeReceiptAws:
            def find_execution_instance(self, _job: object, *, purchase_option: str):
                self.purchase_option = purchase_option
                return None

            def execution_markers(self, _job: object):
                return ("complete",)

            def read_receipt(self, _job: object):
                return {
                    "schema_version": 1,
                    "status": "complete",
                    "role": "runtime",
                    "attempt": 1,
                    "attempt_id": "runtime-0001",
                    "source_archive_sha256": "2" * 64,
                    "manifest_sha256": "6" * 64,
                    "protocol_sha256": "7" * 64,
                    "binary_sha256": "8" * 64,
                    "purchase_option": "spot",
                }

            def terminate(self, _instance: str):
                raise AssertionError("completed observation has no active instance")

        aws = RuntimeReceiptAws()
        with self.assertRaisesRegex(ValueError, "frozen authority"):
            run_execution_job(
                job,
                request={},
                expected={
                    "attempt_id": "runtime-0001",
                    "source_archive_sha256": "2" * 64,
                    "manifest_sha256": "6" * 64,
                    "protocol_sha256": "7" * 64,
                    "binary_sha256": "8" * 64,
                    "purchase_option": "on-demand",
                },
                aws=aws,
                timeout_seconds=60,
                poll_seconds=0.01,
                purchase_option="on-demand",
            )
        self.assertEqual(aws.purchase_option, "on-demand")

    def test_runtime_schema_five_receipt_matches_flow_control_authority(self) -> None:
        manifest = unstaged_sift_manifest()
        manifest["source"] = {
            "state": "frozen",
            "git_commit": "1" * 40,
            "archive_sha256": "2" * 64,
            "cargo_lock_sha256": "3" * 64,
            "python_lock_sha256": "4" * 64,
            "node_lock_sha256": "5" * 64,
        }
        job = ExecutionJob.runtime(
            qualification_cell(
                manifest, dataset_id="sift-128", workload_kind="read-recall"
            ),
            attempt=1,
        )
        expected = {
            "attempt_id": "runtime-0001",
            "source_archive_sha256": "2" * 64,
            "manifest_sha256": "6" * 64,
            "protocol_sha256": "7" * 64,
            "binary_sha256": "8" * 64,
            "purchase_option": "spot",
            "runtime_profile": "recall",
            "arm_index": 0,
            "max_active_searches": 4,
            "max_waiting_searches": 16,
            "leaf_read_width": 32,
            "max_inflight_leaf_reads": 48,
            "max_parallel_decode_rank_tasks": 1,
            "cpu_threads": 3,
            "io_threads": 88,
            "s3_get_concurrency": 64,
            "ram_budget_bytes": 2 * 1024 * 1024 * 1024,
            "disk_cache_max_bytes": 0,
            "exact_read_max_physical_amplification": 2,
        }

        class RuntimeReceiptAws:
            reconciliation_count: object = 0

            def find_execution_instance(self, _job: object, *, purchase_option: str):
                return None

            def execution_markers(self, _job: object):
                return ("complete",)

            def read_receipt(self, _job: object):
                return {
                    "schema_version": 5,
                    "status": "complete",
                    "role": "runtime",
                    "attempt": 1,
                    **expected,
                    "execution_contract_sha256": "9" * 64,
                    "artifact_upload_reconciliations": self.reconciliation_count,
                }

            def terminate(self, _instance: str):
                raise AssertionError("completed observation has no active instance")

        receipt = run_execution_job(
            job,
            request={},
            expected=expected,
            aws=RuntimeReceiptAws(),
            timeout_seconds=60,
            poll_seconds=0.01,
            purchase_option="spot",
        )
        self.assertEqual(receipt["schema_version"], 5)
        for invalid_count in (True, -1, 0.0, None):
            with (
                self.subTest(invalid_count=invalid_count),
                self.assertRaisesRegex(ValueError, "reconciliation count"),
            ):
                invalid = RuntimeReceiptAws()
                invalid.reconciliation_count = invalid_count
                run_execution_job(
                    job,
                    request={},
                    expected=expected,
                    aws=invalid,
                    timeout_seconds=60,
                    poll_seconds=0.01,
                    purchase_option="spot",
                )

    def test_execution_instance_identity_is_role_cell_and_attempt_bound(self) -> None:
        manifest = unstaged_sift_manifest()
        manifest["source"] = {
            "state": "frozen",
            "git_commit": "1" * 40,
            "archive_sha256": "2" * 64,
            "cargo_lock_sha256": "3" * 64,
            "python_lock_sha256": "4" * 64,
            "node_lock_sha256": "5" * 64,
        }
        cell = qualification_cell(
            manifest, dataset_id="sift-128", workload_kind="read-recall"
        )
        job = ExecutionJob.build(cell, attempt=1)
        commands: list[list[str]] = []

        def run(
            command: list[str], **_kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            commands.append(command)
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps(
                    {
                        "Reservations": [
                            {
                                "Instances": [
                                    {
                                        "InstanceId": "i-0123456789abcdef0",
                                        "InstanceLifecycle": "spot",
                                        "State": {"Name": "running"},
                                        "Tags": [
                                            {
                                                "Key": "Project",
                                                "Value": "BorsukBenchmark",
                                            },
                                            {
                                                "Key": "Campaign",
                                                "Value": manifest["campaign_id"],
                                            },
                                            {"Key": "Cell", "Value": job.cell_tag},
                                            {"Key": "Attempt", "Value": "1"},
                                            {"Key": "Role", "Value": "build"},
                                            {
                                                "Key": "PurchaseOption",
                                                "Value": "spot",
                                            },
                                            {"Key": "AutoTerminate", "Value": "true"},
                                        ],
                                    }
                                ]
                            }
                        ]
                    }
                ),
                "",
            )

        client = AwsCli(manifest, profile="causality", run=run)
        self.assertEqual(
            client.find_execution_instance(job, purchase_option="spot"),
            ("i-0123456789abcdef0", "running"),
        )
        joined = " ".join(commands[0])
        self.assertIn(f"Name=tag:Cell,Values={job.cell_tag}", joined)
        self.assertIn("Name=tag:Role,Values=build", joined)
        self.assertIn("stopped,shutting-down,terminated", joined)

        on_demand_instance = {
            "InstanceId": "i-0123456789abcdef0",
            "State": {"Name": "running"},
            "Tags": [
                {"Key": "Project", "Value": "BorsukBenchmark"},
                {"Key": "Campaign", "Value": manifest["campaign_id"]},
                {
                    "Key": "Cell",
                    "Value": ExecutionJob.runtime(cell, attempt=1).cell_tag,
                },
                {"Key": "Attempt", "Value": "1"},
                {"Key": "Role", "Value": "runtime"},
                {"Key": "AutoTerminate", "Value": "true"},
                {"Key": "PurchaseOption", "Value": "on-demand"},
            ],
        }
        runtime_job = ExecutionJob.runtime(cell, attempt=1)
        client._run = lambda command: subprocess.CompletedProcess(
            command,
            0,
            json.dumps({"Reservations": [{"Instances": [on_demand_instance]}]}),
            "",
        )
        self.assertEqual(
            client.find_execution_instance(runtime_job, purchase_option="on-demand"),
            ("i-0123456789abcdef0", "running"),
        )
        with self.assertRaisesRegex(ValueError, "identity"):
            client.find_execution_instance(runtime_job, purchase_option="spot")
        with self.assertRaisesRegex(ValueError, "only for runtime"):
            client.find_execution_instance(job, purchase_option="on-demand")

    def test_markerless_terminal_instance_writes_immutable_failure_authority(
        self,
    ) -> None:
        manifest = unstaged_sift_manifest()
        manifest["source"] = {
            "state": "frozen",
            "git_commit": "1" * 40,
            "archive_sha256": "2" * 64,
            "cargo_lock_sha256": "3" * 64,
            "python_lock_sha256": "4" * 64,
            "node_lock_sha256": "5" * 64,
        }
        job = ExecutionJob.build(
            qualification_cell(
                manifest, dataset_id="sift-128", workload_kind="read-recall"
            ),
            attempt=2,
        )
        client = AwsCli(manifest, profile="causality")
        client.execution_markers = lambda _job: ()
        uploaded: list[tuple[bytes, str, str]] = []
        client.upload_immutable = lambda path, uri, sha256: uploaded.append(
            (path.read_bytes(), uri, sha256)
        )

        client.record_markerless_execution_failure(
            job,
            instance_id="i-0123456789abcdef0",
            instance_state="terminated",
        )

        self.assertEqual(len(uploaded), 1)
        body, uri, digest = uploaded[0]
        self.assertEqual(
            uri, f"{job.terminal_prefix}/CONTROLLER_TERMINAL_OBSERVED.json"
        )
        self.assertEqual(hashlib.sha256(body).hexdigest(), digest)
        self.assertEqual(
            json.loads(body),
            {
                "attempt": 2,
                "attempt_id": f"{job.cell_tag}-a0002",
                "failure_kind": "instance-terminal-before-marker",
                "instance_id": "i-0123456789abcdef0",
                "role": "build",
                "schema_version": 1,
                "status": "failed",
            },
        )

    def test_immutable_upload_accepts_concurrent_identical_writer(self) -> None:
        manifest = unstaged_sift_manifest()
        body = b'{"status":"failed"}'
        digest = hashlib.sha256(body).hexdigest()
        checksum = base64.b64encode(bytes.fromhex(digest)).decode("ascii")
        heads = iter(
            (
                None,
                {
                    "ContentLength": len(body),
                    "Metadata": {"borsuk-sha256": digest},
                    "ChecksumSHA256": checksum,
                },
            )
        )
        client = AwsCli(manifest, profile="causality")
        client._head = lambda _uri: next(heads)
        client._run = lambda command, check=True: subprocess.CompletedProcess(
            command, 255, "", "PreconditionFailed: 412"
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "failure.json"
            path.write_bytes(body)
            client.upload_immutable(
                path,
                "s3://borsuk-bench-453182569524-euc1/test/failure.json",
                digest,
            )

    def test_direct_controller_entrypoint_reaches_argparse(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("publication_v3_controller.py")),
                "--help",
            ],
            capture_output=True,
            text=True,
            env={
                key: value for key, value in os.environ.items() if key != "PYTHONPATH"
            },
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn(
            "{stage,build-sift,build-read,run-read,diagnose-read,diagnose-v21-selector,read-recall-sift,read-concurrency-sift,build-lifecycle,run-lifecycle,diagnose-lifecycle}",
            completed.stdout,
        )

    def test_stale_completed_receipt_advances_to_fresh_spot_attempt(self) -> None:
        manifest = unstaged_sift_manifest()
        aws = FakeAws(receipt_for(manifest, 4))
        prefix = "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812"
        result = stage_dataset(
            manifest,
            dataset_id="sift-128",
            source_uri=f"{prefix}/source/a.tar.gz",
            source_archive_sha256="a" * 64,
            manifest_uri=f"{prefix}/manifests/m.json",
            manifest_sha256=hashlib.sha256(canonical_json_bytes(manifest)).hexdigest(),
            launch=LaunchEnvironment(
                "ami-x",
                "subnet-x",
                "sg-x",
                "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
                "aarch64",
                "eu-central-1",
            ),
            aws=aws,
            max_attempts=4,
            poll_seconds=0.01,
        )
        self.assertEqual(result["attempt"], 4)
        self.assertEqual(aws.launched, [4])
        self.assertEqual(aws.terminated, ["i-0123456789abcdef0"])

    def test_explicit_start_attempt_skips_orphaned_lower_attempts(self) -> None:
        manifest = unstaged_sift_manifest()
        aws = FakeAws(receipt_for(manifest, 5))
        prefix = "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812"
        result = stage_dataset(
            manifest,
            dataset_id="sift-128",
            source_uri=f"{prefix}/source/a.tar.gz",
            source_archive_sha256="a" * 64,
            manifest_uri=f"{prefix}/manifests/m.json",
            manifest_sha256=hashlib.sha256(canonical_json_bytes(manifest)).hexdigest(),
            launch=LaunchEnvironment(
                "ami-x",
                "subnet-x",
                "sg-x",
                "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
                "aarch64",
                "eu-central-1",
            ),
            aws=aws,
            start_attempt=5,
            max_attempts=6,
            poll_seconds=0.01,
        )
        self.assertEqual(result["attempt"], 5)
        self.assertEqual(aws.launched, [5])

    def test_corrupt_terminal_receipt_fails_closed_instead_of_advancing(self) -> None:
        manifest = unstaged_sift_manifest()
        aws = FakeAws(receipt_for(manifest, 4))
        aws.terminal_markers = lambda _job: ("STAGING_COMPLETE.json",)
        aws.read_receipt = lambda _job: (_ for _ in ()).throw(
            ValueError("staging receipt checksum differs")
        )
        prefix = "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812"
        with self.assertRaisesRegex(ValueError, "checksum differs"):
            stage_dataset(
                manifest,
                dataset_id="sift-128",
                source_uri=f"{prefix}/source/a.tar.gz",
                source_archive_sha256="a" * 64,
                manifest_uri=f"{prefix}/manifests/m.json",
                manifest_sha256=hashlib.sha256(
                    canonical_json_bytes(manifest)
                ).hexdigest(),
                launch=LaunchEnvironment(
                    "ami-x",
                    "subnet-x",
                    "sg-x",
                    "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
                    "aarch64",
                    "eu-central-1",
                ),
                aws=aws,
                max_attempts=4,
                poll_seconds=0.01,
            )
        self.assertEqual(aws.launched, [])

    def test_terminal_receipt_is_bound_to_observed_attempt_and_source(self) -> None:
        manifest = unstaged_sift_manifest()
        prefix = "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812"
        for candidate, message in (
            (receipt_for(manifest, 4), "observed attempt"),
            (
                {**receipt_for(manifest, 1), "source_archive_sha256": "e" * 64},
                "source archive",
            ),
        ):
            aws = FakeAws(candidate)
            aws.terminal_markers = lambda _job: ("STAGING_COMPLETE.json",)
            aws.read_receipt = lambda _job, value=candidate: value
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(ValueError, message),
            ):
                stage_dataset(
                    manifest,
                    dataset_id="sift-128",
                    source_uri=f"{prefix}/source/a.tar.gz",
                    source_archive_sha256="a" * 64,
                    manifest_uri=f"{prefix}/manifests/m.json",
                    manifest_sha256=hashlib.sha256(
                        canonical_json_bytes(manifest)
                    ).hexdigest(),
                    launch=LaunchEnvironment(
                        "ami-x",
                        "subnet-x",
                        "sg-x",
                        "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
                        "aarch64",
                        "eu-central-1",
                    ),
                    aws=aws,
                    max_attempts=1,
                    poll_seconds=0.01,
                )

    def test_valid_complete_receipt_wins_over_late_failure_marker(self) -> None:
        manifest = unstaged_sift_manifest()
        candidate = receipt_for(manifest, 1)
        aws = FakeAws(candidate)
        aws.terminal_markers = lambda _job: (
            "STAGING_COMPLETE.json",
            "STAGING_FAILED.json",
        )
        aws.read_receipt = lambda _job: candidate
        prefix = "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812"
        result = stage_dataset(
            manifest,
            dataset_id="sift-128",
            source_uri=f"{prefix}/source/a.tar.gz",
            source_archive_sha256="a" * 64,
            manifest_uri=f"{prefix}/manifests/m.json",
            manifest_sha256=hashlib.sha256(canonical_json_bytes(manifest)).hexdigest(),
            launch=LaunchEnvironment(
                "ami-x",
                "subnet-x",
                "sg-x",
                "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
                "aarch64",
                "eu-central-1",
            ),
            aws=aws,
            max_attempts=1,
            poll_seconds=0.01,
        )
        self.assertEqual(result["attempt"], 1)

    def test_controller_terminates_launched_or_stopped_instance_on_failure(
        self,
    ) -> None:
        manifest = unstaged_sift_manifest()
        prefix = "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812"
        launch = LaunchEnvironment(
            "ami-x",
            "subnet-x",
            "sg-x",
            "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
            "aarch64",
            "eu-central-1",
        )
        for stopped in (False, True):
            aws = FakeAws(receipt_for(manifest, 1))
            observations = iter(((), ("STAGING_COMPLETE.json",)))
            aws.terminal_markers = lambda _job, values=observations: next(values, ())
            aws.find_instance = (
                (lambda _job: ("i-0123456789abcdef0", "stopped"))
                if stopped
                else (lambda _job: None)
            )
            aws.instance_state = lambda _id, is_stopped=stopped: (
                "stopped" if is_stopped else "running"
            )
            aws.read_receipt = lambda _job: (_ for _ in ()).throw(
                ValueError("corrupt receipt")
            )
            with self.subTest(stopped=stopped), self.assertRaises(ValueError):
                stage_dataset(
                    manifest,
                    dataset_id="sift-128",
                    source_uri=f"{prefix}/source/a.tar.gz",
                    source_archive_sha256="a" * 64,
                    manifest_uri=f"{prefix}/manifests/m.json",
                    manifest_sha256=hashlib.sha256(
                        canonical_json_bytes(manifest)
                    ).hexdigest(),
                    launch=launch,
                    aws=aws,
                    max_attempts=1,
                    poll_seconds=0.01,
                )
            self.assertEqual(aws.terminated, ["i-0123456789abcdef0"])

    def test_aws_client_rejects_foreign_active_instance_before_reuse(self) -> None:
        manifest = unstaged_sift_manifest()
        job = next(j for j in staging_jobs(manifest) if j.dataset_id == "sift-128")

        def response(
            command: list[str], **_kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            instance = {
                "InstanceId": "i-0123456789abcdef0",
                "InstanceLifecycle": "spot",
                "State": {"Name": "running"},
                "Tags": [
                    {"Key": "Project", "Value": "BorsukBenchmark"},
                    {"Key": "Campaign", "Value": "foreign-campaign"},
                    {"Key": "Cell", "Value": "stage-sift-128"},
                    {"Key": "Attempt", "Value": "1"},
                    {"Key": "Role", "Value": "staging"},
                    {"Key": "AutoTerminate", "Value": "true"},
                ],
            }
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps({"Reservations": [{"Instances": [instance]}]}),
                "",
            )

        client = AwsCli(manifest, profile="causality", run=response)
        with self.assertRaisesRegex(ValueError, "identity"):
            client.find_instance(job)

    def test_immutable_upload_rejects_existing_object_with_wrong_s3_checksum(
        self,
    ) -> None:
        manifest = json.loads(MANIFEST.read_text())
        client = AwsCli(
            manifest, profile="causality", run=lambda *_args, **_kwargs: None
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.tar.gz"
            path.write_bytes(b"frozen source")
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            client._head = lambda _uri: {
                "ContentLength": path.stat().st_size,
                "Metadata": {"borsuk-sha256": digest},
                "ChecksumSHA256": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            }
            with self.assertRaisesRegex(ValueError, "different bytes"):
                client.upload_immutable(path, "s3://bucket/source.tar.gz", digest)


if __name__ == "__main__":
    unittest.main()
