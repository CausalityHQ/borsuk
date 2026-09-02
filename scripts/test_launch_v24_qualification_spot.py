from __future__ import annotations

import ast
import base64
import hashlib
import io
import json
import re
import unittest
from unittest import mock

from scripts import launch_v24_qualification_spot as subject


class _AwsError(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}


class V24QualificationSpotTests(unittest.TestCase):
    def plan(self, phase: str = "witness-training") -> subject.V24SpotPlan:
        return subject.build_v24_spot_plan(
            run_id=f"v24-{phase}-fixture",
            phase=phase,
            source_commit="1" * 40,
            source_archive_uri="s3://fixture/v24/source.tar.zst",
            source_archive_sha256="2" * 64,
            source_archive_bytes=8192,
            binary_uri="s3://fixture/v24/v24_witness_page_router",
            binary_sha256="3" * 64,
            binary_bytes=4096,
            manifest_uri=f"s3://fixture/v24/{phase}-manifest.json",
            manifest_sha256="4" * 64,
            manifest_bytes=2048,
            output_prefix=f"s3://fixture/v24/results/{phase}/",
        )

    def test_build_v24_launch_specs_are_causality_spot_only_in_every_registered_zone(
        self,
    ) -> None:
        self.assertEqual(subject.PROFILE, "causality")
        self.assertEqual(subject.REGION, "eu-central-1")
        self.assertEqual(
            [target.availability_zone for target in subject.SPOT_TARGETS],
            ["eu-central-1c", "eu-central-1b", "eu-central-1a"],
        )
        for phase in subject.PHASES:
            specs = subject.build_v24_launch_specs(
                self.plan(phase), launch_nonce="a" * 32
            )
            self.assertEqual(len(specs), 3)
            self.assertEqual(
                [spec["SubnetId"] for spec in specs],
                [target.subnet_id for target in subject.SPOT_TARGETS],
            )
            self.assertEqual(len({spec["ClientToken"] for spec in specs}), 3)
            for spec in specs:
                self.assertEqual(spec["MinCount"], 1)
                self.assertEqual(spec["MaxCount"], 1)
                self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
                self.assertEqual(
                    spec["InstanceMarketOptions"]["SpotOptions"],
                    {
                        "InstanceInterruptionBehavior": "terminate",
                        "SpotInstanceType": "one-time",
                    },
                )
                self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")

        first = subject.build_v24_launch_specs(self.plan(), launch_nonce="a" * 32)
        restarted = subject.build_v24_launch_specs(self.plan(), launch_nonce="b" * 32)
        self.assertNotEqual(
            [spec["ClientToken"] for spec in first],
            [spec["ClientToken"] for spec in restarted],
        )
        self.assertGreaterEqual(
            subject.controller_wall_seconds("input-preparation"), 21_600
        )
        self.assertGreaterEqual(
            subject.controller_wall_seconds("witness-training"), 10_800
        )

    def test_worker_authenticates_exact_inputs_and_runs_one_offline_monitored_phase(
        self,
    ) -> None:
        plan = self.plan()
        script = base64.b64decode(
            subject.build_v24_launch_specs(plan, launch_nonce="a" * 32)[0]["UserData"]
        ).decode()
        for authority in (
            plan.source_archive_sha256,
            plan.binary_sha256,
            plan.manifest_sha256,
            str(plan.source_archive_bytes),
            str(plan.binary_bytes),
            str(plan.manifest_bytes),
        ):
            self.assertIn(authority, script)
        self.assertIn("stage_v24_witness_inputs", script)
        self.assertIn("python3 -m scripts.run_v24_witness_page_router", script)
        self.assertNotIn("python3 scripts/run_v24_witness_page_router.py", script)
        self.assertIn("ATTEMPT_COMPLETE.json", script)
        self.assertIn("ATTEMPT_FAILED.json", script)
        self.assertIn("--generate-cli-skeleton input", script)
        self.assertIn("grep -q '\"IfNoneMatch\"'", script)
        self.assertIn("--if-none-match '*'", script)
        self.assertIn("shutdown -h now", script)
        self.assertNotIn("on-demand", script.lower())
        self.assertNotIn("dgx", script.lower())
        self.assertNotIn("ldd", script.lower())
        self.assertNotIn("mount ", script.lower())
        self.assertNotIn("d3", script.lower())

        binding_script = base64.b64decode(
            subject.build_v24_launch_specs(
                self.plan("holdout-binding"), launch_nonce="a" * 32
            )[0]["UserData"]
        ).decode()
        self.assertIn('result_path="$outputs/holdout-binding.json"', binding_script)
        self.assertIn(
            'put_once "$root/stdout.json" holdout-binding.json', binding_script
        )
        training_script = base64.b64decode(
            subject.build_v24_launch_specs(
                self.plan("witness-training"), launch_nonce="a" * 32
            )[0]["UserData"]
        ).decode()
        self.assertIn(
            'put_once "$root/stdout.json" training-result.json', training_script
        )
        development_script = base64.b64decode(
            subject.build_v24_launch_specs(
                self.plan("development-evaluation"), launch_nonce="a" * 32
            )[0]["UserData"]
        ).decode()
        self.assertIn(
            'put_once "$root/stdout.json" development-result.json',
            development_script,
        )
        pseudoquery_script = base64.b64decode(
            subject.build_v24_launch_specs(
                self.plan("pseudoquery-evaluation"), launch_nonce="a" * 32
            )[0]["UserData"]
        ).decode()
        self.assertIn("runner_phase=evaluate-pseudoqueries", pseudoquery_script)
        self.assertIn('--phase "$runner_phase"', pseudoquery_script)
        self.assertIn(
            'put_once "$outputs/pseudoquery-evidence.parquet" pseudoquery-evidence.parquet',
            pseudoquery_script,
        )
        self.assertIn(
            'put_once "$root/stdout.json" pseudoquery-result.json',
            pseudoquery_script,
        )
        self.assertIn(
            'put_once "$outputs/pseudoquery-pass-receipt.json" pseudoquery-pass-receipt.json',
            pseudoquery_script,
        )
        self.assertIn(
            'if [[ -f "$outputs/pseudoquery-pass-receipt.json" ]]; then',
            pseudoquery_script,
        )
        holdout_script = base64.b64decode(
            subject.build_v24_launch_specs(
                self.plan("holdout-evaluation"), launch_nonce="a" * 32
            )[0]["UserData"]
        ).decode()
        self.assertIn(
            'put_once "$root/stdout.json" holdout-result.json', holdout_script
        )

    def test_worker_inline_python_is_syntactically_valid(self) -> None:
        script = base64.b64decode(
            subject.build_v24_launch_specs(
                self.plan("input-preparation"), launch_nonce="a" * 32
            )[0]["UserData"]
        ).decode()
        blocks = re.findall(r"<<'PY'[^\n]*\n(.*?)\nPY\n", script, re.DOTALL)
        self.assertGreaterEqual(len(blocks), 4)
        for block in blocks:
            ast.parse(block)

    def test_preparation_worker_uses_direct_preparer_and_exact_output_uris(
        self,
    ) -> None:
        script = base64.b64decode(
            subject.build_v24_launch_specs(
                self.plan("input-preparation"), launch_nonce="a" * 32
            )[0]["UserData"]
        ).decode()
        self.assertIn("--execute-preparation", script)
        self.assertIn("--manifest-sha256", script)
        self.assertIn("--construction-uri", script)
        self.assertIn("construction-rows.parquet", script)
        self.assertIn("--page-rows-uri", script)
        self.assertIn("page-rows.parquet", script)
        self.assertIn("preparation-receipt.json", script)
        self.assertIn("monitor_process_group", script)
        self.assertIn("offline_environment", script)
        self.assertIn("blake3==1.0.8", script)

    def test_capacity_fallback_uses_one_fresh_instance_and_always_terminates(
        self,
    ) -> None:
        plan = self.plan()
        ec2 = mock.Mock()
        ec2.run_instances.side_effect = [
            _AwsError("InsufficientInstanceCapacity"),
            {"Instances": [{"InstanceId": "i-v24-fixture"}]},
        ]
        ec2.describe_instances.return_value = {
            "Reservations": [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-v24-fixture",
                            "State": {"Name": "running"},
                            "StateReason": {"Code": "pending"},
                        }
                    ]
                }
            ]
        }
        terminal = subject.canonical_v24_terminal_bytes(
            plan,
            instance_id="i-v24-fixture",
            status="complete",
            result_sha256="5" * 64,
            result_bytes=1024,
        )
        terminal_value = json.loads(terminal)
        self.assertEqual(
            {
                key: terminal_value[key]
                for key in (
                    "source_archive_uri",
                    "source_archive_sha256",
                    "source_archive_bytes",
                    "binary_uri",
                    "binary_sha256",
                    "binary_bytes",
                    "manifest_uri",
                    "manifest_sha256",
                    "manifest_bytes",
                )
            },
            {
                "source_archive_uri": plan.source_archive_uri,
                "source_archive_sha256": plan.source_archive_sha256,
                "source_archive_bytes": plan.source_archive_bytes,
                "binary_uri": plan.binary_uri,
                "binary_sha256": plan.binary_sha256,
                "binary_bytes": plan.binary_bytes,
                "manifest_uri": plan.manifest_uri,
                "manifest_sha256": plan.manifest_sha256,
                "manifest_bytes": plan.manifest_bytes,
            },
        )
        s3 = mock.Mock()
        s3.get_object.side_effect = [
            _AwsError("NoSuchKey"),
            _AwsError("NoSuchKey"),
            _AwsError("NoSuchKey"),
            _AwsError("NoSuchKey"),
            _AwsError("NoSuchKey"),
            {"Body": io.BytesIO(terminal), "ContentLength": len(terminal)},
        ]
        with mock.patch.object(subject.time, "sleep"):
            uri = subject.run_v24_spot_phase(plan, ec2_client=ec2, s3_client=s3)
        self.assertEqual(
            uri,
            "s3://fixture/v24/results/witness-training/ATTEMPT_COMPLETE.json",
        )
        self.assertEqual(ec2.run_instances.call_count, 2)
        ec2.terminate_instances.assert_called_once_with(InstanceIds=["i-v24-fixture"])

    def test_noncapacity_error_terminal_no_clobber_and_timeout_never_fall_through(
        self,
    ) -> None:
        plan = self.plan()
        ec2 = mock.Mock()
        ec2.run_instances.side_effect = _AwsError("UnauthorizedOperation")
        s3 = mock.Mock()
        s3.get_object.side_effect = _AwsError("NoSuchKey")
        with self.assertRaises(_AwsError):
            subject.run_v24_spot_phase(plan, ec2_client=ec2, s3_client=s3)
        self.assertEqual(ec2.run_instances.call_count, 1)
        ec2.terminate_instances.assert_not_called()

        existing = subject.canonical_v24_terminal_bytes(
            plan,
            instance_id="i-existing",
            status="complete",
            result_sha256="5" * 64,
            result_bytes=1024,
        )
        s3 = mock.Mock()
        s3.get_object.return_value = {
            "Body": io.BytesIO(existing),
            "ContentLength": len(existing),
        }
        ec2 = mock.Mock()
        with self.assertRaisesRegex(ValueError, "already exists"):
            subject.run_v24_spot_phase(plan, ec2_client=ec2, s3_client=s3)
        ec2.run_instances.assert_not_called()

        mutated = json.loads(existing)
        mutated["manifest_sha256"] = "6" * 64
        with self.assertRaisesRegex(ValueError, "terminal"):
            subject.validate_v24_terminal_bytes(
                subject.canonical_json_bytes(mutated), plan, "complete"
            )

    def test_transient_control_plane_failure_does_not_kill_healthy_worker(self) -> None:
        plan = self.plan()
        ec2 = mock.Mock()
        ec2.run_instances.return_value = {
            "Instances": [{"InstanceId": "i-v24-transient"}]
        }
        ec2.describe_instances.side_effect = [
            _AwsError("RequestLimitExceeded"),
            {
                "Reservations": [
                    {
                        "Instances": [
                            {
                                "InstanceId": "i-v24-transient",
                                "State": {"Name": "running"},
                            }
                        ]
                    }
                ]
            },
        ]
        terminal = subject.canonical_v24_terminal_bytes(
            plan,
            instance_id="i-v24-transient",
            status="complete",
            result_sha256="5" * 64,
            result_bytes=1024,
        )
        s3 = mock.Mock()
        s3.get_object.side_effect = [
            _AwsError("NoSuchKey"),
            _AwsError("NoSuchKey"),
            _AwsError("NoSuchKey"),
            _AwsError("NoSuchKey"),
            _AwsError("SlowDown"),
            _AwsError("NoSuchKey"),
            {"Body": io.BytesIO(terminal), "ContentLength": len(terminal)},
        ]
        with mock.patch.object(subject.time, "sleep"):
            uri = subject.run_v24_spot_phase(plan, ec2_client=ec2, s3_client=s3)
        self.assertEqual(
            uri,
            "s3://fixture/v24/results/witness-training/ATTEMPT_COMPLETE.json",
        )
        ec2.terminate_instances.assert_called_once_with(InstanceIds=["i-v24-transient"])

    def test_pseudoquery_terminal_authenticates_pass_receipt_and_result_binding(
        self,
    ) -> None:
        plan = self.plan("pseudoquery-evaluation")
        result_sha256 = "5" * 64
        result_bytes = 1024
        output = plan.output_prefix
        receipt = subject.canonical_json_bytes(
            {
                "benchmark_query_reads": 0,
                "claim_eligible": False,
                "distance_backend": "aarch64-neon-fma",
                "evidence": {
                    "digest": "6" * 64,
                    "digest_algorithm": "sha256",
                    "encoded_bytes": 2048,
                    "generation": "generation-v24-fixture",
                    "role": "pseudoquery-evidence",
                    "uri": output + "pseudoquery-evidence.parquet",
                },
                "generation": "generation-v24-fixture",
                "ordered_inputs": [],
                "page_body_reads": 0,
                "passed": True,
                "pseudoquery_count": 1024,
                "result": {
                    "digest": result_sha256,
                    "digest_algorithm": "sha256",
                    "encoded_bytes": result_bytes,
                    "generation": "generation-v24-fixture",
                    "role": "pseudoquery-result",
                    "uri": output + "pseudoquery-result.json",
                },
                "schema": "borsuk-v24-pseudoquery-pass-receipt-v1",
                "source_ordinals_sha256": "7" * 64,
                "split_seed": 0x123456789ABCDEF0,
                "witness_count": 4096,
            }
        )
        terminal = subject.canonical_v24_terminal_bytes(
            plan,
            instance_id="i-v24-pseudoquery",
            status="complete",
            result_sha256=result_sha256,
            result_bytes=result_bytes,
            pass_receipt_sha256=hashlib.sha256(receipt).hexdigest(),
            pass_receipt_bytes=len(receipt),
        )

        def run(receipt_bytes: bytes, terminal_bytes: bytes) -> None:
            calls: dict[str, int] = {}

            def get_object(**request: object) -> dict[str, object]:
                key = str(request["Key"])
                calls[key] = calls.get(key, 0) + 1
                if key.endswith("ATTEMPT_FAILED.json"):
                    raise _AwsError("NoSuchKey")
                if key.endswith("ATTEMPT_COMPLETE.json") and calls[key] == 1:
                    raise _AwsError("NoSuchKey")
                if key.endswith("ATTEMPT_COMPLETE.json"):
                    return {
                        "Body": io.BytesIO(terminal_bytes),
                        "ContentLength": len(terminal_bytes),
                    }
                if key.endswith("pseudoquery-pass-receipt.json"):
                    return {
                        "Body": io.BytesIO(receipt_bytes),
                        "ContentLength": len(receipt_bytes),
                    }
                raise AssertionError(key)

            ec2 = mock.Mock()
            ec2.run_instances.return_value = {
                "Instances": [{"InstanceId": "i-v24-pseudoquery"}]
            }
            s3 = mock.Mock()
            s3.get_object.side_effect = get_object
            uri = subject.run_v24_spot_phase(plan, ec2_client=ec2, s3_client=s3)
            self.assertTrue(uri.endswith("ATTEMPT_COMPLETE.json"))
            ec2.terminate_instances.assert_called_once_with(
                InstanceIds=["i-v24-pseudoquery"]
            )

        run(receipt, terminal)
        drifted = json.loads(receipt)
        drifted["result"]["digest"] = "8" * 64
        drifted_receipt = subject.canonical_json_bytes(drifted)
        drifted_terminal = subject.canonical_v24_terminal_bytes(
            plan,
            instance_id="i-v24-pseudoquery",
            status="complete",
            result_sha256=result_sha256,
            result_bytes=result_bytes,
            pass_receipt_sha256=hashlib.sha256(drifted_receipt).hexdigest(),
            pass_receipt_bytes=len(drifted_receipt),
        )
        with self.assertRaisesRegex(ValueError, "pass receipt"):
            run(drifted_receipt, drifted_terminal)


if __name__ == "__main__":
    unittest.main()
