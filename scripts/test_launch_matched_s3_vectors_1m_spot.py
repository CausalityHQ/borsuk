from __future__ import annotations

import base64
import io
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.launch_matched_s3_vectors_1m_spot import (
    ObjectIdentity,
    SpotTarget,
    build_launch_specs,
    build_plan,
    canonical_terminal_bytes,
    monitor_and_terminate,
    worker_script,
)


def _identity(role: str, byte: str = "a") -> ObjectIdentity:
    return ObjectIdentity(
        role=role,
        uri=f"s3://frozen/{role}",
        sha256=byte * 64,
        bytes=123,
    )


class _TerminalS3:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def get_object(self, **_: object) -> dict[str, object]:
        return {"Body": io.BytesIO(self.body), "ContentLength": len(self.body)}


class _Ec2:
    def __init__(self) -> None:
        self.terminated: list[list[str]] = []

    def terminate_instances(self, *, InstanceIds: list[str]) -> None:
        self.terminated.append(InstanceIds)


class MatchedS3VectorsSpotLauncherTests(unittest.TestCase):
    def _plan(self):
        return build_plan(
            profile="causality",
            source_commit="1" * 40,
            source_archive=_identity("source_archive", "f"),
            inputs={
                "source": _identity("source", "1"),
                "queries": _identity("queries", "2"),
                "truth": _identity("truth", "3"),
            },
            output_prefix="s3://evidence/matched-s3-vectors/run/a0001",
            image_id="ami-06121aa3085b6f918",
            image_architecture="x86_64",
            security_group_id="sg-0b1fd3e4fbde4af0d",
            instance_profile_arn=(
                "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
            ),
            targets=(
                SpotTarget("eu-central-1c", "subnet-0a12dbed0ca6fac25"),
                SpotTarget("eu-central-1b", "subnet-00243d923761c047c"),
            ),
            spot_price_usd_per_hour_micros=688_000,
            vector_bucket="borsuk-match-123",
        )

    def test_plan_emits_one_time_spot_specs_and_authenticated_bootstrap(self) -> None:
        plan = self._plan()

        script = worker_script(plan)
        specs = build_launch_specs(plan)

        self.assertEqual(len(specs), 2)
        self.assertLessEqual(len(script.encode()), 16_384)
        self.assertEqual(
            [spec["Placement"]["AvailabilityZone"] for spec in specs],
            ["eu-central-1c", "eu-central-1b"],
        )
        for spec in specs:
            self.assertEqual(spec["InstanceType"], "c7i.8xlarge")
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertEqual(
                spec["InstanceMarketOptions"]["SpotOptions"],
                {
                    "InstanceInterruptionBehavior": "terminate",
                    "SpotInstanceType": "one-time",
                },
            )
            self.assertEqual(base64.b64decode(spec["UserData"]).decode(), script)
        self.assertIn("MATCHED_SOURCE_SHA256=" + "1" * 64, script)
        self.assertIn("MATCHED_VECTOR_BUCKET=borsuk-match-123", script)
        self.assertIn("exec bash repo/scripts/run_matched_s3_vectors_1m_remote.sh", script)

    def test_terminal_requires_cleanup_evidence_and_always_terminates(self) -> None:
        plan = self._plan()
        evidence = {
            role: _identity(role, character)
            for role, character in (
                ("cleanup", "4"),
                ("resources", "5"),
                ("result", "6"),
                ("samples", "7"),
                ("worker_log", "8"),
            )
        }
        body = canonical_terminal_bytes(
            plan,
            instance_id="i-0123456789abcdef0",
            status="complete",
            exit_code=0,
            evidence=evidence,
        )
        ec2 = _Ec2()

        terminal = monitor_and_terminate(
            plan,
            s3_client=_TerminalS3(body),
            ec2_client=ec2,
            instance_id="i-0123456789abcdef0",
            monotonic=lambda: 0.0,
            sleep=lambda _: None,
        )

        self.assertTrue(terminal.claim_eligible)
        self.assertEqual(set(terminal.evidence), set(evidence))
        self.assertEqual(ec2.terminated, [["i-0123456789abcdef0"]])
        with self.assertRaisesRegex(ValueError, "terminal differs"):
            canonical_terminal_bytes(
                plan,
                instance_id="i-0123456789abcdef0",
                status="complete",
                exit_code=0,
                evidence={role: value for role, value in evidence.items() if role != "cleanup"},
            )

    def test_direct_script_help_resolves_repository_imports(self) -> None:
        script = Path(__file__).with_name("launch_matched_s3_vectors_1m_spot.py")
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                ["/usr/bin/python3", str(script), "--help"],
                cwd=directory,
                capture_output=True,
                text=True,
                check=False,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--source-commit", result.stdout)


if __name__ == "__main__":
    unittest.main()
