import base64
import json
import subprocess
import sys
import unittest
from pathlib import Path

from scripts.publication_v3_aws import (
    AttemptObservation,
    build_spot_launch_request,
    build_staging_receipt,
    classify_attempt,
    staging_jobs,
)

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs/research/publication-v3-manifest.json"


class PublicationV3AwsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))

    def test_staging_jobs_cover_only_external_datasets_with_exact_adapters(self) -> None:
        jobs = staging_jobs(self.manifest)
        self.assertEqual(len(jobs), 12)
        self.assertEqual([job.dataset_id for job in jobs], sorted(job.dataset_id for job in jobs))
        self.assertFalse(
            any(job.dataset_id.startswith("synthetic-") for job in jobs)
        )
        by_id = {job.dataset_id: job for job in jobs}
        self.assertEqual(by_id["deep-image-96"].adapter, "ann-benchmarks")
        self.assertEqual(by_id["laion-100m-768"].adapter, "vdbbench")
        self.assertEqual(by_id["scifact"].adapter, "beir")
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
        self.assertTrue(
            all("/attempts/0002/" in job.output_uri for job in retries)
        )

    def test_launch_request_is_one_time_spot_hardened_and_role_sized(self) -> None:
        request = build_spot_launch_request(
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
        self.assertIn(base64.b64encode(b"echo run-cell").decode("ascii"), user_data)
        volume = request["BlockDeviceMappings"][0]["Ebs"]
        self.assertEqual(volume["VolumeSize"], 32)
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
        self.assertEqual(tags["AutoTerminate"], "true")
        self.assertEqual(
            {item["ResourceType"] for item in request["TagSpecifications"]},
            {"instance", "volume"},
        )
        with self.assertRaisesRegex(ValueError, "architecture"):
            build_spot_launch_request(
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
                max_seconds=7200,
            )

    def test_attempt_classification_uses_only_terminal_markers_and_instance_state(self) -> None:
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

    def test_plan_staging_cli_is_canonical_and_contains_no_aws_side_effect(self) -> None:
        command = [
            sys.executable,
            "scripts/publication_v3_aws.py",
            "plan-staging",
            str(MANIFEST),
        ]
        first = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        second = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(first.stdout, second.stdout)
        value = json.loads(first.stdout)
        self.assertEqual(value["schema_version"], 1)
        self.assertEqual(value["campaign_id"], "publication-v3-20260812")
        self.assertRegex(value["manifest_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(value["job_count"], 12)
        self.assertEqual(len(value["jobs"]), 12)
        self.assertNotIn("instance_id", first.stdout)

    def test_staging_receipt_rejects_single_object_and_attests_spot_inventory(self) -> None:
        job = next(job for job in staging_jobs(self.manifest) if job.dataset_id == "sift-128")
        common = {
            "source_archive_sha256": "a" * 64,
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
        self.assertEqual(receipt["purchase_option"], "spot")
        self.assertEqual(receipt["terminal_uri"], job.terminal_uri)
        self.assertEqual(
            receipt["object_bytes"], sum(item["bytes"] for item in receipt["objects"])
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


if __name__ == "__main__":
    unittest.main()
