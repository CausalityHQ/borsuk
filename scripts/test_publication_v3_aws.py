import base64
import gzip
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock

from scripts.publication_v3_aws import (
    AttemptObservation,
    build_launch_request,
    build_staging_receipt,
    build_staging_worker_script,
    classify_attempt,
    main,
    promote_staging_receipts,
    reconcile_staging_attempt,
    staging_jobs,
    terminal_failure_reporter_script,
    validate_staging_receipt,
    worker_supervisor_script,
)
from scripts.publication_v3_protocol import canonical_json_bytes, validate_manifest

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs/research/publication-v3-manifest.json"


class PublicationV3AwsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        upstream_sources = {
            "deep-image-96": "https://ann-benchmarks.com/deep-image-96-angular.hdf5",
            "fashion-mnist-784": "https://ann-benchmarks.com/fashion-mnist-784-euclidean.hdf5",
            "gist-960": "https://ann-benchmarks.com/gist-960-euclidean.hdf5",
            "glove-100": "https://ann-benchmarks.com/glove-100-angular.hdf5",
            "nytimes-256": "https://ann-benchmarks.com/nytimes-256-angular.hdf5",
            "sift-128": "https://ann-benchmarks.com/sift-128-euclidean.hdf5",
            "cohere-medium-1m-768": "s3://assets.zilliz.com/benchmark/cohere_medium_1m",
            "cohere-large-10m-768": "s3://assets.zilliz.com/benchmark/cohere_large_10m",
            "laion-100m-768": "s3://assets.zilliz.com/benchmark/laion_large_100m",
            "scifact": "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip",
            "nfcorpus": "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/nfcorpus.zip",
            "fiqa": "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/fiqa.zip",
        }
        for dataset in self.manifest["datasets"]:
            if dataset["source"]["state"] == "staged-generated":
                dataset["source"] = {
                    "state": "generated",
                    "generator": dataset["source"]["generator"],
                    "seed": dataset["source"]["seed"],
                }
                continue
            expected_source = upstream_sources.get(dataset["id"])
            if expected_source is None:
                continue
            dataset["source"] = {
                "state": "unstaged",
                "expected_source": expected_source,
                "license": dataset["source"]["license"],
            }
        directory = tempfile.TemporaryDirectory(prefix="borsuk-v3-unstaged-")
        self.addCleanup(directory.cleanup)
        self.unstaged_manifest = Path(directory.name) / "manifest.json"
        self.unstaged_manifest.write_text(
            json.dumps(self.manifest, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )

    def test_committed_manifest_has_only_promoted_dataset_authority(self) -> None:
        manifest = validate_manifest(json.loads(MANIFEST.read_text(encoding="utf-8")))
        jobs = staging_jobs(manifest)
        self.assertEqual(jobs, ())
        external = [
            dataset
            for dataset in manifest["datasets"]
            if dataset["kind"] in {"standard-ann", "realistic-dense", "beir-hybrid"}
        ]
        self.assertEqual(len(external), 12)
        for dataset in external:
            source = dataset["source"]
            self.assertEqual(source["state"], "staged")
            self.assertEqual(
                source["url"].rsplit("/attempts/", 1)[0],
                f"{manifest['prefixes']['dataset']}/{dataset['id']}",
            )
            self.assertRegex(source["sha256"], r"^[0-9a-f]{64}$")
        synthetic = [
            dataset
            for dataset in manifest["datasets"]
            if dataset["id"].startswith("synthetic-")
        ]
        self.assertEqual(len(synthetic), 11)
        self.assertTrue(
            all(
                dataset["source"]["state"] == "staged-generated"
                for dataset in synthetic
            )
        )

    def test_staging_jobs_cover_external_and_generated_datasets_with_exact_adapters(
        self,
    ) -> None:
        jobs = staging_jobs(self.manifest)
        self.assertEqual(len(jobs), 23)
        self.assertEqual(
            [job.dataset_id for job in jobs], sorted(job.dataset_id for job in jobs)
        )
        self.assertEqual(
            sum(job.dataset_id.startswith("synthetic-") for job in jobs), 11
        )
        by_id = {job.dataset_id: job for job in jobs}
        self.assertEqual(by_id["deep-image-96"].adapter, "ann-benchmarks")
        self.assertEqual(by_id["laion-100m-768"].adapter, "vdbbench")
        self.assertEqual(by_id["scifact"].adapter, "beir")
        self.assertEqual(by_id["synthetic-clustered-100m-768"].adapter, "synthetic")
        self.assertEqual(
            by_id["scifact"].output_uri,
            "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/datasets/scifact/attempts/0001/materialized",
        )
        for job in jobs:
            self.assertEqual(job.attempt, 1)
            self.assertTrue(job.terminal_uri.endswith("/STAGING_COMPLETE.json"))
            self.assertTrue(job.failure_uri.endswith("/STAGING_FAILED.json"))
        retries = staging_jobs(self.manifest, attempt=2)
        self.assertTrue(all(job.attempt == 2 for job in retries))
        self.assertTrue(all("/attempts/0002/" in job.output_uri for job in retries))

    def test_launch_request_is_one_time_spot_hardened_and_role_sized(self) -> None:
        request = build_launch_request(
            self.manifest,
            role="runtime",
            system="borsuk",
            image_id="ami-0123456789abcdef0",
            subnet_id="subnet-0123456789abcdef0",
            security_group_id="sg-0123456789abcdef0",
            instance_profile_arn="arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
            image_architecture="aarch64",
            subnet_region="eu-central-1",
            campaign_id="publication-v3-20260812",
            cell_id="read-sift-r01",
            attempt=2,
            worker_script="echo run-cell",
            terminal_failure_uri=(
                "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/"
                "results/read-sift-r01/runtime/attempts/0002/"
                "RUNTIME_TERMINAL_FAILED.json"
            ),
            terminal_detail_log_path="/var/lib/borsuk-publication/cell/runtime.log",
            max_seconds=7200,
        )
        self.assertEqual(request["InstanceType"], "c7g.xlarge")
        self.assertEqual(request["MinCount"], 1)
        self.assertEqual(request["MaxCount"], 1)
        self.assertRegex(request["ClientToken"], r"^borsuk-[0-9a-f]{40}$")
        self.assertEqual(request["InstanceMarketOptions"]["MarketType"], "spot")
        self.assertEqual(
            request["InstanceMarketOptions"]["SpotOptions"],
            {
                "SpotInstanceType": "one-time",
                "InstanceInterruptionBehavior": "terminate",
            },
        )
        self.assertEqual(request["InstanceInitiatedShutdownBehavior"], "terminate")
        self.assertEqual(request["MetadataOptions"]["HttpTokens"], "required")
        self.assertEqual(request["MetadataOptions"]["HttpPutResponseHopLimit"], 1)
        user_data = base64.b64decode(request["UserData"]).decode("utf-8")
        self.assertIn("timeout --signal=TERM --kill-after=60 7200", user_data)
        self.assertIn("shutdown -h now", user_data)
        self.assertIn("/var/lib/borsuk-publication-failure-reporter.sh", user_data)
        self.assertIn("controller-timeout", user_data)
        self.assertIn("--canary", user_data)
        self.assertIn("/var/lib/borsuk-publication/cell/runtime.log", user_data)
        payload = user_data.split("printf '%s' '", 1)[1].split("'", 1)[0]
        self.assertEqual(gzip.decompress(base64.b64decode(payload)), b"echo run-cell")
        self.assertIn("base64 -d | gzip -d", user_data)
        self.assertIn("export HOME=/root", user_data)
        self.assertLess(
            user_data.index("export HOME=/root"),
            user_data.index("/bin/bash /var/lib/borsuk-publication-worker.sh"),
        )
        self.assertEqual(len(request["BlockDeviceMappings"]), 2)
        root, cache = request["BlockDeviceMappings"]
        self.assertEqual(root["DeviceName"], "/dev/xvda")
        self.assertEqual(root["Ebs"]["VolumeSize"], 16)
        self.assertEqual(cache["DeviceName"], "/dev/sdf")
        volume = cache["Ebs"]
        self.assertEqual(volume["VolumeSize"], 96)
        self.assertEqual(volume["VolumeType"], "gp3")
        self.assertEqual(volume["Iops"], 3000)
        self.assertEqual(volume["Throughput"], 125)
        self.assertTrue(volume["Encrypted"])
        self.assertTrue(volume["DeleteOnTermination"])

        tags = {
            item["Key"]: item["Value"]
            for item in request["TagSpecifications"][0]["Tags"]
        }
        self.assertEqual(tags["Campaign"], "publication-v3-20260812")
        self.assertEqual(tags["Cell"], "read-sift-r01")
        self.assertEqual(tags["Attempt"], "2")
        self.assertEqual(tags["Role"], "runtime")
        self.assertEqual(tags["PurchaseOption"], "spot")
        self.assertEqual(tags["AutoTerminate"], "true")
        self.assertEqual(
            {item["ResourceType"] for item in request["TagSpecifications"]},
            {"instance", "volume"},
        )
        with self.assertRaisesRegex(ValueError, "architecture"):
            build_launch_request(
                self.manifest,
                role="runtime",
                system="borsuk",
                image_id="ami-0123456789abcdef0",
                subnet_id="subnet-0123456789abcdef0",
                security_group_id="sg-0123456789abcdef0",
                instance_profile_arn="arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
                image_architecture="x86_64",
                subnet_region="eu-central-1",
                campaign_id="publication-v3-20260812",
                cell_id="read-sift-r01",
                attempt=2,
                worker_script="echo run-cell",
                terminal_failure_uri=(
                    "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/"
                    "results/read-sift-r01/runtime/attempts/0002/"
                    "RUNTIME_TERMINAL_FAILED.json"
                ),
                terminal_detail_log_path=(
                    "/var/lib/borsuk-publication/cell/runtime.log"
                ),
                max_seconds=7200,
            )

    def test_v21_diagnostic_launch_uses_build_resources_with_runtime_timeout(
        self,
    ) -> None:
        request = build_launch_request(
            self.manifest,
            role="diagnostic",
            system="borsuk",
            image_id="ami-0123456789abcdef0",
            subnet_id="subnet-0123456789abcdef0",
            security_group_id="sg-0123456789abcdef0",
            instance_profile_arn="arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
            image_architecture="aarch64",
            subnet_region="eu-central-1",
            campaign_id="publication-v3-20260812",
            cell_id="runtime-v21-r01",
            attempt=1,
            worker_script="echo v21",
            terminal_failure_uri=(
                "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/"
                "results/runtime-v21-r01/runtime-v21-feasibility/attempts/0001/"
                "RUNTIME_TERMINAL_FAILED.json"
            ),
            terminal_detail_log_path="/var/lib/borsuk-publication/worker.log",
            max_seconds=7_200,
            purchase_option="spot",
        )
        self.assertEqual(request["InstanceType"], "r7g.8xlarge")
        self.assertEqual(len(request["BlockDeviceMappings"]), 1)
        self.assertEqual(request["BlockDeviceMappings"][0]["Ebs"]["VolumeSize"], 4096)
        tags = {
            item["Key"]: item["Value"]
            for item in request["TagSpecifications"][0]["Tags"]
        }
        self.assertEqual(tags["Role"], "diagnostic")
        self.assertEqual(tags["PurchaseOption"], "spot")

    def test_worker_supervisor_reports_failure_after_timeout_kills_worker(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(prefix="borsuk-v3-supervisor-") as directory:
            root = Path(directory)
            worker = root / "worker.sh"
            reporter = root / "reporter.sh"
            receipt = root / "receipt.txt"
            detail_log = root / "worker.log"
            detail_log.write_text("bounded diagnostic\n", encoding="utf-8")
            worker.write_text(
                "#!/usr/bin/env bash\ntrap '' TERM\nwhile :; do sleep 10; done\n",
                encoding="utf-8",
            )
            reporter.write_text(
                "#!/usr/bin/env bash\n"
                f'printf \'%s\\n%s\\n%s\\n\' "$1" "$2" "$3" >{receipt!s}\n',
                encoding="utf-8",
            )
            worker.chmod(0o700)
            reporter.chmod(0o700)

            result = subprocess.run(
                [
                    "bash",
                    "-c",
                    worker_supervisor_script(worker, reporter, 1, 1, detail_log),
                ],
                text=True,
                capture_output=True,
                timeout=5,
                check=False,
            )

            self.assertNotEqual(result.returncode, 0)
            status, stage, reported_log = receipt.read_text(
                encoding="utf-8"
            ).splitlines()
            self.assertGreater(int(status), 0)
            self.assertEqual(stage, "controller-timeout")
            self.assertEqual(reported_log, str(detail_log))

    def test_terminal_failure_reporter_publishes_receipt_and_bounded_log(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(prefix="borsuk-v3-reporter-") as directory:
            root = Path(directory)
            binaries = root / "bin"
            captured = root / "captured"
            work = root / "work"
            binaries.mkdir()
            captured.mkdir()
            fake_aws = binaries / "aws"
            fake_aws.write_text(
                "#!/usr/bin/env bash\n"
                "set -euo pipefail\n"
                "operation=${2:-}\n"
                "body= key= query=\n"
                "while [[ $# -gt 0 ]]; do\n"
                "  case $1 in\n"
                "    --body) body=$2; shift 2;;\n"
                "    --key) key=$2; shift 2;;\n"
                "    --query) query=$2; shift 2;;\n"
                "    *) shift;;\n"
                "  esac\n"
                "done\n"
                'target="$CAPTURE_DIR/${key##*/}"\n'
                "if [[ $operation = head-object ]]; then\n"
                "  [[ $query = 'Metadata.\"borsuk-sha256\"' ]]\n"
                '  test -f "$target"\n'
                "  sha256sum \"$target\" | awk '{print $1}'\n"
                "  exit 0\n"
                "fi\n"
                'if [[ ${AWS_SKIP_PUT_COPY:-0} != 1 ]]; then cp "$body" "$target"; fi\n'
                "if [[ ${AWS_PRECONDITION_AFTER_PUT:-0} = 1 ]]; then\n"
                "  echo 'PreconditionFailed (412)' >&2\n"
                "  exit 255\n"
                "fi\n",
                encoding="utf-8",
            )
            fake_aws.chmod(0o700)
            detail_log = root / "worker.log"
            detail_log.write_bytes(b"x" * 70_000)
            reporter = root / "reporter.sh"
            reporter.write_text(
                terminal_failure_reporter_script(
                    "s3://bucket/results/runtime/attempts/0001/"
                    "RUNTIME_TERMINAL_FAILED.json"
                ),
                encoding="utf-8",
            )
            reporter.chmod(0o700)
            environment = {
                **dict(os.environ),
                "PATH": f"{binaries}:{os.environ['PATH']}",
                "CAPTURE_DIR": str(captured),
                "BORSUK_FAILURE_WORK": str(work),
                "AWS_PRECONDITION_AFTER_PUT": "1",
            }

            canary = subprocess.run(
                [str(reporter), "--canary"],
                text=True,
                capture_output=True,
                env=environment,
                check=False,
            )
            self.assertEqual(canary.returncode, 0, canary.stderr)
            self.assertEqual(
                json.loads(
                    (captured / "RECEIPT_CANARY.json").read_text(encoding="utf-8")
                ),
                {"schema_version": 1, "status": "receipt-channel-ready"},
            )

            result = subprocess.run(
                [str(reporter), "0", "Bad Stage!", str(detail_log)],
                text=True,
                capture_output=True,
                env=environment,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                json.loads(
                    (captured / "RUNTIME_TERMINAL_FAILED.json").read_text(
                        encoding="utf-8"
                    )
                ),
                {
                    "schema_version": 1,
                    "status": "failed",
                    "exit_code": 1,
                    "stage": "bad-stage-",
                },
            )
            failure_log = (captured / "FAILURE.log").read_bytes()
            self.assertEqual(len(failure_log), 65_536)
            self.assertEqual(failure_log, detail_log.read_bytes()[-65_536:])

            (captured / "RECEIPT_CANARY.json").write_text(
                '{"foreign":true}\n', encoding="utf-8"
            )
            foreign = subprocess.run(
                [str(reporter), "--canary"],
                text=True,
                capture_output=True,
                env={**environment, "AWS_SKIP_PUT_COPY": "1"},
                check=False,
            )
            self.assertNotEqual(foreign.returncode, 0)

    def test_runtime_on_demand_exception_is_explicit_tagged_and_idempotently_distinct(
        self,
    ) -> None:
        common = {
            "manifest": self.manifest,
            "role": "runtime",
            "system": "borsuk",
            "image_id": "ami-0123456789abcdef0",
            "subnet_id": "subnet-0123456789abcdef0",
            "security_group_id": "sg-0123456789abcdef0",
            "instance_profile_arn": "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
            "image_architecture": "aarch64",
            "subnet_region": "eu-central-1",
            "campaign_id": "publication-v3-20260812",
            "cell_id": "read-sift-r01",
            "attempt": 2,
            "worker_script": "echo run-cell",
            "terminal_failure_uri": (
                "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/"
                "results/read-sift-r01/runtime/attempts/0002/"
                "RUNTIME_TERMINAL_FAILED.json"
            ),
            "terminal_detail_log_path": (
                "/var/lib/borsuk-publication/cell/runtime.log"
            ),
            "max_seconds": 7200,
        }
        spot = build_launch_request(**common, purchase_option="spot")
        on_demand = build_launch_request(**common, purchase_option="on-demand")
        self.assertNotIn("InstanceMarketOptions", on_demand)
        self.assertEqual(
            spot["ClientToken"],
            "borsuk-"
            + hashlib.sha256(
                ("publication-v3-20260812\0read-sift-r01\0" + "2").encode()
            ).hexdigest()[:40],
        )
        self.assertNotEqual(spot["ClientToken"], on_demand["ClientToken"])
        tags = {
            item["Key"]: item["Value"]
            for item in on_demand["TagSpecifications"][0]["Tags"]
        }
        self.assertEqual(tags["PurchaseOption"], "on-demand")
        with self.assertRaisesRegex(ValueError, "purchase option"):
            build_launch_request(**common, purchase_option="reserved")
        with self.assertRaisesRegex(ValueError, "runtime"):
            build_launch_request(
                **{**common, "role": "build"}, purchase_option="on-demand"
            )

    def test_attempt_classification_uses_only_terminal_markers_and_instance_state(
        self,
    ) -> None:
        success = classify_attempt(
            AttemptObservation(
                instance_state="running",
                terminal_markers=("CELL_COMPLETE",),
            )
        )
        self.assertEqual(success.action, "terminate-success")
        failure = classify_attempt(
            AttemptObservation(
                instance_state="running",
                terminal_markers=("CELL_FAILED",),
            )
        )
        self.assertEqual(failure.action, "terminate-failure")
        self.assertTrue(failure.discard_measurements)
        interrupted = classify_attempt(
            AttemptObservation(instance_state="terminated", terminal_markers=())
        )
        self.assertEqual(interrupted.action, "retry-fresh-attempt")
        self.assertTrue(interrupted.discard_measurements)
        running = classify_attempt(
            AttemptObservation(instance_state="running", terminal_markers=())
        )
        self.assertEqual(running.action, "monitor")
        shutting_down = classify_attempt(
            AttemptObservation(instance_state="shutting-down", terminal_markers=())
        )
        self.assertEqual(shutting_down.action, "monitor")
        with self.assertRaisesRegex(ValueError, "conflicting terminal markers"):
            classify_attempt(
                AttemptObservation(
                    instance_state="running",
                    terminal_markers=("CELL_COMPLETE", "CELL_FAILED"),
                )
            )
        with self.assertRaisesRegex(ValueError, "EC2 instance state"):
            classify_attempt(
                AttemptObservation(
                    instance_state="not-an-ec2-state",
                    terminal_markers=("CELL_COMPLETE",),
                )
            )

    def test_plan_staging_cli_is_canonical_and_contains_no_aws_side_effect(
        self,
    ) -> None:
        command = [
            sys.executable,
            "scripts/publication_v3_aws.py",
            "plan-staging",
            str(self.unstaged_manifest),
        ]
        first = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        second = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(first.stdout, second.stdout)
        value = json.loads(first.stdout)
        self.assertEqual(value["schema_version"], 1)
        self.assertEqual(value["campaign_id"], "publication-v3-20260812")
        self.assertRegex(value["manifest_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(value["job_count"], 23)
        self.assertEqual(len(value["jobs"]), 23)
        self.assertIn("sift-128", {job["dataset_id"] for job in value["jobs"]})
        self.assertNotIn("instance_id", first.stdout)

    def test_reconcile_staging_cli_emits_a_fresh_attempt_without_aws(self) -> None:
        command = [
            sys.executable,
            "scripts/publication_v3_aws.py",
            "reconcile-staging",
            str(self.unstaged_manifest),
            "--dataset",
            "gist-960",
            "--attempt",
            "2",
            "--instance-id",
            "i-0123456789abcdef0",
            "--instance-state",
            "terminated",
            "--max-attempts",
            "3",
        ]
        completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        value = json.loads(completed.stdout)
        self.assertEqual(value["action"], "retry-fresh-attempt")
        self.assertFalse(value["terminate_instance"])
        self.assertEqual(value["next_job"]["attempt"], 3)
        self.assertEqual(value["next_job"]["dataset_id"], "gist-960")

    def test_staging_receipt_rejects_single_object_and_attests_spot_inventory(
        self,
    ) -> None:
        job = next(
            job for job in staging_jobs(self.manifest) if job.dataset_id == "sift-128"
        )
        common = {
            "source_archive_sha256": "a" * 64,
            "source_provenance": {
                "schema_version": 1,
                "dataset": "sift-128",
                "source": "https://ann-benchmarks.com/sift-128-euclidean.hdf5",
                "source_sha256": "b" * 64,
                "materialization_sha256": "c66ceeb981504f9de03a84700e3ef410b3298f67dd92a3768a8cab6de4b2c3ee",
            },
            "provenance_sha256": "d" * 64,
            "instance_id": "i-0123456789abcdef0",
            "instance_type": "r7g.8xlarge",
            "availability_zone": "eu-central-1a",
            "purchase_option": "spot",
        }
        with self.assertRaisesRegex(ValueError, "multi-object"):
            build_staging_receipt(
                self.manifest,
                job,
                objects=(
                    {
                        "role": "train",
                        "format": "parquet",
                        "uri": f"{job.output_uri}/train-00000000.parquet",
                        "sha256": "c" * 64,
                        "bytes": 1024,
                        "rows": 10,
                    },
                ),
                **common,
            )
        objects = (
            ("train", "train-00000000.parquet", 10),
            ("query", "test.parquet", 2),
            ("ground-truth", "neighbors.parquet", 2),
            ("metadata", "meta.json", 1),
        )
        receipt = build_staging_receipt(
            self.manifest,
            job,
            objects=tuple(
                {
                    "role": role,
                    "format": "json" if role == "metadata" else "parquet",
                    "uri": f"{job.output_uri}/{name}",
                    "sha256": f"{index + 1:064x}",
                    "bytes": 1024,
                    "rows": rows,
                }
                for index, (role, name, rows) in enumerate(objects)
            ),
            **common,
        )
        self.assertEqual(receipt["object_count"], 4)
        self.assertRegex(receipt["dataset_content_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(receipt["campaign_id"], "publication-v3-20260812")
        self.assertRegex(receipt["manifest_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(receipt["failure_uri"], job.failure_uri)
        self.assertEqual(receipt["source_provenance"], common["source_provenance"])
        self.assertEqual(receipt["provenance_sha256"], "d" * 64)
        self.assertEqual(receipt["provenance_uri"], job.provenance_uri)
        self.assertEqual(receipt["purchase_option"], "spot")
        self.assertEqual(receipt["terminal_uri"], job.terminal_uri)
        self.assertEqual(
            receipt["object_bytes"], sum(item["bytes"] for item in receipt["objects"])
        )
        with self.assertRaisesRegex(ValueError, "source provenance"):
            build_staging_receipt(
                self.manifest,
                job,
                objects=receipt["objects"],
                **{
                    **common,
                    "source_provenance": {
                        **common["source_provenance"],
                        "materialization_sha256": "0" * 64,
                    },
                },
            )
        with self.assertRaisesRegex(ValueError, "Spot"):
            build_staging_receipt(
                self.manifest,
                job,
                objects=receipt["objects"],
                **{**common, "purchase_option": "on-demand"},
            )
        with self.assertRaisesRegex(ValueError, "instance type"):
            build_staging_receipt(
                self.manifest,
                job,
                objects=receipt["objects"],
                **{**common, "instance_type": "m5.large"},
            )
        with self.assertRaisesRegex(ValueError, "unrecognized terminal marker"):
            classify_attempt(
                AttemptObservation(
                    instance_state="running",
                    terminal_markers=("metrics.csv",),
                )
            )

    def test_staging_worker_uses_frozen_inputs_python312_and_terminal_diagnostics(
        self,
    ) -> None:
        job = next(
            job
            for job in staging_jobs(self.manifest, attempt=4)
            if job.dataset_id == "sift-128"
        )
        script = build_staging_worker_script(
            self.manifest,
            job,
            source_uri="s3://borsuk-bench-453182569524-euc1/source/archive.tar.gz",
            source_archive_sha256="a" * 64,
            manifest_uri="s3://borsuk-bench-453182569524-euc1/manifests/frozen.json",
            manifest_sha256="b" * 64,
        )
        self.assertIn("dnf install -y python3.12 python3.12-pip", script)
        self.assertIn("python3.12 -m venv", script)
        self.assertIn("region=eu-central-1", script)
        self.assertIn("trap 'fail 143' TERM", script)
        self.assertIn("/var/lib/borsuk-publication-failure-reporter.sh", script)
        self.assertIn("complete=1", script)
        self.assertNotIn("failed()", script)
        self.assertIn("--attempt 4", script)
        self.assertIn("--dataset sift-128", script)
        self.assertNotIn("python3 -m venv", script)

    def test_beir_staging_worker_uses_isolated_pinned_dependencies(self) -> None:
        job = next(
            job for job in staging_jobs(self.manifest) if job.dataset_id == "scifact"
        )
        script = build_staging_worker_script(
            self.manifest,
            job,
            source_uri="s3://borsuk-bench-453182569524-euc1/source/archive.tar.gz",
            source_archive_sha256="a" * 64,
            manifest_uri="s3://borsuk-bench-453182569524-euc1/manifests/frozen.json",
            manifest_sha256="b" * 64,
        )
        self.assertIn("scripts/requirements-beir-stage.txt", script)
        self.assertIn("--require-hashes", script)
        self.assertIn("--only-binary=:all:", script)
        self.assertNotIn("scripts/requirements-format-bench.txt", script)

    def test_synthetic_worker_builds_generator_on_build_class_spot_host(self) -> None:
        job = next(
            item
            for item in staging_jobs(self.manifest)
            if item.dataset_id == "synthetic-uniform-100m-768"
        )
        script = build_staging_worker_script(
            self.manifest,
            job,
            source_uri="s3://borsuk-bench-453182569524-euc1/source/archive.tar.gz",
            source_archive_sha256="a" * 64,
            manifest_uri="s3://borsuk-bench-453182569524-euc1/manifests/frozen.json",
            manifest_sha256="b" * 64,
        )
        self.assertIn("rustup.rs", script)
        self.assertIn(
            "cargo build --locked --release --example generate_synthetic_dataset",
            script,
        )
        self.assertIn("scripts/promote_publication_v3_dataset.py", script)
        self.assertIn("--source-archive-sha256", script)

    def test_staging_receipt_roundtrip_verifier_rejects_substitution(self) -> None:
        job = next(
            job for job in staging_jobs(self.manifest) if job.dataset_id == "sift-128"
        )
        objects = tuple(
            {
                "role": role,
                "format": "json" if role == "metadata" else "parquet",
                "uri": f"{job.output_uri}/{name}",
                "sha256": f"{index + 1:064x}",
                "bytes": 1024,
                "rows": rows,
            }
            for index, (role, name, rows) in enumerate(
                (
                    ("train", "train-00000000.parquet", 10),
                    ("query", "test.parquet", 2),
                    ("ground-truth", "neighbors.parquet", 2),
                    ("metadata", "meta.json", 1),
                )
            )
        )
        provenance = {
            "schema_version": 1,
            "dataset": "sift-128",
            "source": "https://ann-benchmarks.com/sift-128-euclidean.hdf5",
            "source_sha256": "b" * 64,
            "materialization_sha256": "c66ceeb981504f9de03a84700e3ef410b3298f67dd92a3768a8cab6de4b2c3ee",
        }
        receipt = build_staging_receipt(
            self.manifest,
            job,
            source_archive_sha256="a" * 64,
            source_provenance=provenance,
            provenance_sha256="d" * 64,
            objects=objects,
            instance_id="i-0123456789abcdef0",
            instance_type="r7g.8xlarge",
            availability_zone="eu-central-1a",
            purchase_option="spot",
        )
        self.assertEqual(validate_staging_receipt(self.manifest, receipt), receipt)
        promoted = promote_staging_receipts(self.manifest, [receipt])
        promoted_sift = next(
            dataset for dataset in promoted["datasets"] if dataset["id"] == "sift-128"
        )
        self.assertEqual(
            promoted_sift["source"],
            {
                "state": "staged",
                "url": job.output_uri,
                "sha256": receipt["dataset_content_sha256"],
                "license": "upstream-dataset-license",
            },
        )
        self.assertEqual(
            next(
                dataset
                for dataset in promoted["datasets"]
                if dataset["id"] == "gist-960"
            )["source"]["state"],
            "unstaged",
        )
        with self.assertRaisesRegex(ValueError, "receipt"):
            validate_staging_receipt(
                self.manifest, {**receipt, "dataset_content_sha256": "0" * 64}
            )

        current = json.loads(json.dumps(self.manifest))
        current["master_seed"] += 1
        with self.assertRaisesRegex(ValueError, "exact manifest authority"):
            promote_staging_receipts(current, [receipt])
        promoted_historical = promote_staging_receipts(
            current,
            [receipt],
            historical_manifests={receipt["manifest_sha256"]: self.manifest},
        )
        self.assertEqual(
            next(
                dataset
                for dataset in promoted_historical["datasets"]
                if dataset["id"] == "sift-128"
            )["source"]["state"],
            "staged",
        )
        incompatible = json.loads(json.dumps(current))
        next(
            dataset
            for dataset in incompatible["datasets"]
            if dataset["id"] == "sift-128"
        )["dimensions"] += 1
        with self.assertRaisesRegex(ValueError, "dataset contract"):
            promote_staging_receipts(
                incompatible,
                [receipt],
                historical_manifests={receipt["manifest_sha256"]: self.manifest},
            )
        corrupt_authority = json.loads(json.dumps(self.manifest))
        corrupt_authority["master_seed"] += 1
        with self.assertRaisesRegex(ValueError, "checksum differs"):
            promote_staging_receipts(
                current,
                [receipt],
                historical_manifests={receipt["manifest_sha256"]: corrupt_authority},
            )

    def test_generated_receipt_promotes_recipe_to_staged_generated_authority(
        self,
    ) -> None:
        dataset_id = "synthetic-clustered-1m-768"
        dataset = next(
            item for item in self.manifest["datasets"] if item["id"] == dataset_id
        )
        job = next(
            item
            for item in staging_jobs(self.manifest)
            if item.dataset_id == dataset_id
        )
        objects = tuple(
            {
                "role": role,
                "format": "json" if role == "metadata" else "parquet",
                "uri": f"{job.output_uri}/{name}",
                "sha256": f"{index + 1:064x}",
                "bytes": 1024,
                "rows": rows,
            }
            for index, (role, name, rows) in enumerate(
                (
                    ("train", "train-00000000.parquet", 1_000_000),
                    ("query", "test.parquet", 1_000),
                    ("ground-truth", "neighbors.parquet", 1_000),
                    ("metadata", "meta.json", 1),
                )
            )
        )
        identity = [
            {
                **{
                    key: item[key]
                    for key in ("role", "format", "sha256", "bytes", "rows")
                },
                "path": str(item["uri"]).removeprefix(job.output_uri + "/"),
            }
            for item in sorted(objects, key=lambda item: str(item["uri"]))
        ]
        content_sha = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
        provenance = {
            "schema_version": 1,
            "dataset": dataset_id,
            "source": "generated",
            "source_sha256": hashlib.sha256(
                canonical_json_bytes(
                    {
                        "dataset": dataset_id,
                        "generator": dataset["source"]["generator"],
                        "seed": dataset["source"]["seed"],
                        "kind": dataset["kind"],
                        "rows": dataset["scale"]["rows"],
                        "dimensions": dataset["dimensions"],
                        "metric": dataset["metric"],
                    }
                )
            ).hexdigest(),
            "materialization_sha256": content_sha,
            "generator": dataset["source"]["generator"],
            "seed": dataset["source"]["seed"],
            "kind": dataset["kind"],
            "rows": dataset["scale"]["rows"],
            "dimensions": dataset["dimensions"],
            "metric": dataset["metric"],
            "generator_source_archive_sha256": "a" * 64,
        }
        receipt = build_staging_receipt(
            self.manifest,
            job,
            source_archive_sha256="a" * 64,
            source_provenance=provenance,
            provenance_sha256="d" * 64,
            objects=objects,
            instance_id="i-0123456789abcdef0",
            instance_type="r7g.8xlarge",
            availability_zone="eu-central-1a",
            purchase_option="spot",
        )
        promoted = promote_staging_receipts(self.manifest, [receipt])
        source = next(
            item for item in promoted["datasets"] if item["id"] == dataset_id
        )["source"]
        self.assertEqual(source["state"], "staged-generated")
        self.assertEqual(source["generator"], dataset["source"]["generator"])
        self.assertEqual(source["seed"], dataset["source"]["seed"])
        self.assertEqual(source["generator_source_archive_sha256"], "a" * 64)
        self.assertEqual(source["url"], job.output_uri)
        self.assertEqual(source["sha256"], content_sha)
        self.assertEqual(source["receipt_uri"], job.terminal_uri)
        self.assertEqual(
            source["receipt_sha256"],
            hashlib.sha256(canonical_json_bytes(receipt) + b"\n").hexdigest(),
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest_path = root / "manifest.json"
            receipt_path = root / "receipt.json"
            output_path = root / "promoted.json"
            manifest_path.write_bytes(canonical_json_bytes(self.manifest) + b"\n")
            receipt_path.write_bytes(canonical_json_bytes(receipt) + b"\n")
            arguments = [
                "publication_v3_aws.py",
                "promote-staging",
                str(manifest_path),
                "--receipt",
                str(receipt_path),
                "--output",
                str(output_path),
            ]
            with mock.patch.object(sys, "argv", arguments), redirect_stdout(StringIO()):
                self.assertEqual(main(), 0)
            promoted_bytes = output_path.read_bytes()
            self.assertEqual(
                promoted_bytes,
                canonical_json_bytes(json.loads(promoted_bytes)) + b"\n",
            )
            promoted_source = next(
                item
                for item in json.loads(promoted_bytes)["datasets"]
                if item["id"] == dataset_id
            )["source"]
            self.assertEqual(promoted_source["state"], "staged-generated")
            with self.assertRaisesRegex(FileExistsError, "promoted.json"):
                with (
                    mock.patch.object(sys, "argv", arguments),
                    redirect_stdout(StringIO()),
                ):
                    main()

    def test_reconciler_validates_success_and_bounds_fresh_attempts(self) -> None:
        job = next(
            job
            for job in staging_jobs(self.manifest, attempt=2)
            if job.dataset_id == "sift-128"
        )
        objects = tuple(
            {
                "role": role,
                "format": "json" if role == "metadata" else "parquet",
                "uri": f"{job.output_uri}/{name}",
                "sha256": f"{index + 1:064x}",
                "bytes": 1024,
                "rows": rows,
            }
            for index, (role, name, rows) in enumerate(
                (
                    ("train", "train-00000000.parquet", 10),
                    ("query", "test.parquet", 2),
                    ("ground-truth", "neighbors.parquet", 2),
                    ("metadata", "meta.json", 1),
                )
            )
        )
        receipt = build_staging_receipt(
            self.manifest,
            job,
            source_archive_sha256="a" * 64,
            source_provenance={
                "schema_version": 1,
                "dataset": "sift-128",
                "source": "https://ann-benchmarks.com/sift-128-euclidean.hdf5",
                "source_sha256": "b" * 64,
                "materialization_sha256": "c66ceeb981504f9de03a84700e3ef410b3298f67dd92a3768a8cab6de4b2c3ee",
            },
            provenance_sha256="d" * 64,
            objects=objects,
            instance_id="i-0123456789abcdef0",
            instance_type="r7g.8xlarge",
            availability_zone="eu-central-1a",
            purchase_option="spot",
        )
        success = reconcile_staging_attempt(
            self.manifest,
            job,
            instance_id="i-0123456789abcdef0",
            observation=AttemptObservation(
                instance_state="running",
                terminal_markers=("STAGING_COMPLETE.json",),
            ),
            terminal_receipt=receipt,
            max_attempts=3,
        )
        self.assertEqual(success.action, "terminate-success")
        self.assertTrue(success.terminate_instance)
        self.assertIsNone(success.next_job)
        with self.assertRaisesRegex(ValueError, "receipt"):
            reconcile_staging_attempt(
                self.manifest,
                job,
                instance_id="i-0123456789abcdef0",
                observation=AttemptObservation(
                    instance_state="running",
                    terminal_markers=("STAGING_COMPLETE.json",),
                ),
                terminal_receipt={**receipt, "dataset_content_sha256": "0" * 64},
                max_attempts=3,
            )
        with self.assertRaisesRegex(ValueError, "observed attempt"):
            reconcile_staging_attempt(
                self.manifest,
                job,
                instance_id="i-fffffffffffffffff",
                observation=AttemptObservation(
                    instance_state="running",
                    terminal_markers=("STAGING_COMPLETE.json",),
                ),
                terminal_receipt=receipt,
                max_attempts=3,
            )
        with self.assertRaisesRegex(ValueError, "staging terminal marker"):
            reconcile_staging_attempt(
                self.manifest,
                job,
                instance_id="i-0123456789abcdef0",
                observation=AttemptObservation(
                    instance_state="running",
                    terminal_markers=("CELL_COMPLETE",),
                ),
                terminal_receipt=receipt,
                max_attempts=3,
            )

        failed = reconcile_staging_attempt(
            self.manifest,
            job,
            instance_id="i-0123456789abcdef0",
            observation=AttemptObservation(
                instance_state="running",
                terminal_markers=("STAGING_FAILED.json",),
            ),
            terminal_receipt=None,
            max_attempts=3,
        )
        self.assertEqual(failed.action, "terminate-failure")
        self.assertTrue(failed.terminate_instance)
        self.assertEqual(failed.next_job.attempt, 3)

        stopped = reconcile_staging_attempt(
            self.manifest,
            job,
            instance_id="i-0123456789abcdef0",
            observation=AttemptObservation(
                instance_state="stopped",
                terminal_markers=(),
            ),
            terminal_receipt=None,
            max_attempts=3,
        )
        self.assertEqual(stopped.action, "retry-fresh-attempt")
        self.assertTrue(stopped.terminate_instance)
        self.assertEqual(stopped.next_job.attempt, 3)

        exhausted_job = next(
            job
            for job in staging_jobs(self.manifest, attempt=3)
            if job.dataset_id == "sift-128"
        )
        exhausted = reconcile_staging_attempt(
            self.manifest,
            exhausted_job,
            instance_id="i-0123456789abcdef0",
            observation=AttemptObservation(
                instance_state="terminated",
                terminal_markers=(),
            ),
            terminal_receipt=None,
            max_attempts=3,
        )
        self.assertEqual(exhausted.action, "exhausted-failure")
        self.assertFalse(exhausted.terminate_instance)
        self.assertIsNone(exhausted.next_job)


if __name__ == "__main__":
    unittest.main()
