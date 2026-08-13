from __future__ import annotations

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
    LaunchEnvironment,
    run_execution_job,
    stage_dataset,
)
from scripts.publication_v3_execution import ExecutionJob, qualification_cell
from scripts.publication_v3_protocol import canonical_json_bytes

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

            def find_execution_instance(self, _job: object):
                return None

            def execution_markers(self, _job: object):
                self.observations += 1
                return () if self.observations == 1 else ("complete",)

            def read_receipt(self, _job: object):
                return {
                    "schema_version": 1,
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
        receipt = run_execution_job(
            job,
            request={"InstanceType": "r7g.8xlarge"},
            expected={
                "attempt_id": "build-0001",
                "source_archive_sha256": "2" * 64,
                "manifest_sha256": "6" * 64,
                "protocol_sha256": "7" * 64,
            },
            aws=aws,
            timeout_seconds=60,
            poll_seconds=0.01,
        )
        self.assertEqual(receipt["binary_sha256"], "8" * 64)
        self.assertEqual(aws.launched, 1)
        self.assertEqual(aws.terminated, ["i-0123456789abcdef0"])

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
            client.find_execution_instance(job),
            ("i-0123456789abcdef0", "running"),
        )
        joined = " ".join(commands[0])
        self.assertIn(f"Name=tag:Cell,Values={job.cell_tag}", joined)
        self.assertIn("Name=tag:Role,Values=build", joined)

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
        self.assertIn("{stage,build-sift}", completed.stdout)

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
