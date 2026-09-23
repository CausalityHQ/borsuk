"""Create-only Spot launch and terminal authority for the returned scorer."""

from __future__ import annotations

import dataclasses
import hashlib
import io
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
from scripts.launch_native_two_bit_returned_replay_spot import (
    build_launch_specs,
    readback_artifacts,
    validate_terminal_bytes,
)
from scripts.native_two_bit_returned_spot_worker import ARTIFACT_FILES


class ReturnedSpotTests(unittest.TestCase):
    @staticmethod
    def plan() -> SpotLayoutPlan:
        commit = "a" * 40
        return SpotLayoutPlan(
            profile="causality", source_commit=commit,
            source_archive=SourceArchiveIdentity("s3://bucket/archive", "b" * 64, 123),
            source=FROZEN_SOURCE, queries=FROZEN_QUERIES, truth=FROZEN_TRUTH,
            requirements_sha256="c" * 64,
            output_prefix=("s3://bucket/research/native-two-bit-norm-g0b/"
                           f"{commit}/runs/relaion-100k-dev1000-a0001"),
            image_id="ami-1", security_group_id="sg-1",
            instance_profile_arn="arn:aws:iam::123:instance-profile/x",
            targets=DEFAULT_TARGETS,
        )

    def test_single_spot_instance_and_replay_worker(self) -> None:
        specs = build_launch_specs(self.plan())
        self.assertEqual(len(specs), len(DEFAULT_TARGETS))
        self.assertEqual(len({spec["ClientToken"] for spec in specs}), len(specs))
        for spec in specs:
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertEqual(spec["MinCount"], spec["MaxCount"])
            self.assertEqual(spec["MinCount"], 1)
            self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
            self.assertIn("scripts.native_two_bit_returned_replay", spec["UserData"])
            self.assertIn("scripts.validate_native_two_bit_returned_replay", spec["UserData"])

    def test_terminal_requires_exact_artifact_roster(self) -> None:
        plan = self.plan()
        instance = "i-0123456789abcdef0"
        terminal = {
            "schema": "borsuk-two-bit-norm-terminal-v1",
            "status": "complete", "phase": "complete", "exit_code": 0,
            "elapsed_seconds": 123, "instance_id": instance,
            "source_commit": plan.source_commit,
            "source_archive": dataclasses.asdict(plan.source_archive),
            "requirements_sha256": plan.requirements_sha256,
            "attempt": 1, "claim_eligible": False,
            "artifacts": {
                role: {"role": role, "encoded_bytes": 123,
                       "sha256": "d" * 64,
                       "uri": plan.output_prefix + "/artifacts/" + path}
                for role, path in ARTIFACT_FILES.items()
            },
        }

        def encode(value: dict[str, object]) -> bytes:
            return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()

        self.assertEqual(validate_terminal_bytes(encode(terminal), plan, instance), terminal)
        terminal["artifacts"].pop("returned-result")
        with self.assertRaisesRegex(ValueError, "artifact roster"):
            validate_terminal_bytes(encode(terminal), plan, instance)

    def test_complete_terminal_readback_checks_every_artifact_digest(self) -> None:
        plan = self.plan()
        bodies = {role: role.encode() for role in ARTIFACT_FILES}
        terminal = {"status": "complete", "artifacts": {
            role: {
                "role": role, "encoded_bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                "uri": plan.output_prefix + "/artifacts/" + ARTIFACT_FILES[role],
            }
            for role, body in bodies.items()
        }}

        class FakeS3:
            def get_object(self, *, Bucket: str, Key: str):
                role = next(role for role, path in ARTIFACT_FILES.items()
                            if Key.endswith(path))
                self.assert_bucket = Bucket
                return {"Body": io.BytesIO(bodies[role])}

        self.assertEqual(len(readback_artifacts(FakeS3(), terminal)),
                         len(ARTIFACT_FILES))
        bodies["returned-result"] = b"tampered"
        with self.assertRaisesRegex(ValueError, "readback"):
            readback_artifacts(FakeS3(), terminal)
