"""Immutable Spot launch and terminal authority for the sign96 screen."""

from __future__ import annotations

import dataclasses
import json
import unittest

from scripts.launch_native_geometric_layout_spot import (
    DEFAULT_TARGETS,
    FROZEN_QUERIES,
    FROZEN_SOURCE,
    FROZEN_TRUTH,
    SourceArchiveIdentity,
    SpotLayoutPlan,
)
from scripts.launch_native_hundred_thousand_sign96_spot import (
    build_launch_specs,
    validate_terminal_bytes,
)
from scripts.native_hundred_thousand_sign96_worker import ARTIFACT_FILES


class Sign96SpotTest(unittest.TestCase):
    @staticmethod
    def plan() -> SpotLayoutPlan:
        commit = "a" * 40
        return SpotLayoutPlan(
            profile="causality",
            source_commit=commit,
            source_archive=SourceArchiveIdentity("s3://bucket/archive", "b" * 64, 123),
            source=FROZEN_SOURCE, queries=FROZEN_QUERIES, truth=FROZEN_TRUTH,
            requirements_sha256="c" * 64,
            output_prefix=(
                "s3://bucket/research/native-hundred-thousand-sign96/"
                f"{commit}/runs/relaion-100k-dev1000-a0001"
            ),
            image_id="ami-1", security_group_id="sg-1",
            instance_profile_arn="arn:aws:iam::123:instance-profile/x",
            targets=DEFAULT_TARGETS,
        )

    def test_specs_are_serial_one_time_spot(self) -> None:
        specs = build_launch_specs(self.plan())
        self.assertEqual(len(specs), len(DEFAULT_TARGETS))
        self.assertEqual(len({spec["ClientToken"] for spec in specs}), len(specs))
        for spec in specs:
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertEqual(
                spec["InstanceMarketOptions"]["SpotOptions"]["SpotInstanceType"],
                "one-time",
            )
            self.assertEqual(spec["MinCount"], spec["MaxCount"])
            self.assertEqual(spec["MinCount"], 1)
            self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")

    def test_terminal_requires_exact_artifacts_and_decision(self) -> None:
        plan = self.plan()
        instance_id = "i-0123456789abcdef0"
        terminal = {
            "schema": "borsuk-hundred-thousand-sign96-terminal-v1",
            "status": "complete", "phase": "complete", "exit_code": 0,
            "elapsed_seconds": 123, "instance_id": instance_id, "failure_reason": "",
            "source_commit": plan.source_commit,
            "source_archive": dataclasses.asdict(plan.source_archive),
            "requirements_sha256": plan.requirements_sha256,
            "attempt": 1, "claim_eligible": False, "decision": "reject",
            "artifacts": {
                role: {
                    "role": role, "encoded_bytes": 123,
                    "sha256": "d" * 64,
                    "uri": plan.output_prefix + "/artifacts/" + path,
                }
                for role, path in ARTIFACT_FILES.items()
            },
        }

        def encode(value: dict[str, object]) -> bytes:
            return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()

        self.assertEqual(validate_terminal_bytes(encode(terminal), plan, instance_id), terminal)
        terminal["artifacts"].pop("sign-groups")
        with self.assertRaisesRegex(ValueError, "artifact roster"):
            validate_terminal_bytes(encode(terminal), plan, instance_id)
        terminal["status"] = "failed"
        terminal["phase"] = "plan"
        terminal["exit_code"] = 97
        terminal["decision"] = ""
        terminal["failure_reason"] = "resource_cap"
        terminal["artifacts"] = {}
        self.assertEqual(validate_terminal_bytes(encode(terminal), plan, instance_id), terminal)


if __name__ == "__main__":
    unittest.main()
