"""Contract tests for the single immutable microcluster Spot decision cell."""

from __future__ import annotations

import dataclasses
import json
import subprocess
import unittest

from scripts.launch_native_geometric_layout_spot import (
    DEFAULT_TARGETS,
    FROZEN_QUERIES,
    FROZEN_SOURCE,
    FROZEN_TRUTH,
    SourceArchiveIdentity,
)
from scripts.launch_native_page_microcluster_spot import (
    PRIOR_MEMBERSHIP,
    _validate_terminal_bytes,
    build_launch_specs,
    build_plan,
    worker_script,
)


class MicroclusterSpotTests(unittest.TestCase):
    @staticmethod
    def plan():
        return build_plan(
            profile="causality",
            source_commit="12" * 20,
            source_archive=SourceArchiveIdentity(
                uri="s3://frozen/source.tar.gz", sha256="34" * 32, encoded_bytes=1234
            ),
            source=FROZEN_SOURCE,
            queries=FROZEN_QUERIES,
            truth=FROZEN_TRUTH,
            requirements_sha256="56" * 32,
            output_prefix=(
                "s3://borsuk-bench-453182569524-euc1/research/native-page-microcluster/"
                + "12" * 20
                + "/runs/relaion-100k-dev1000-a0001"
            ),
            image_id="ami-06121aa3085b6f918",
            security_group_id="sg-0b1fd3e4fbde4af0d",
            instance_profile_arn=(
                "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
            ),
            targets=DEFAULT_TARGETS,
        )

    def test_worker_seals_microclusters_before_query_capability(self) -> None:
        script = worker_script(self.plan())
        syntax = subprocess.run(
            ["bash", "-n"], input=script, text=True, capture_output=True, check=False
        )
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        before_queries = script[: script.index("phase=evaluate")]
        construct = script[
            script.index("phase=construct") : script.index("phase=evaluate")
        ]
        self.assertIn(PRIOR_MEMBERSHIP.sha256, before_queries)
        self.assertIn("unshare --net", construct)
        self.assertNotIn(FROZEN_QUERIES.uri, construct)
        self.assertNotIn(FROZEN_TRUTH.uri, construct)
        self.assertIn(
            "run_capped /usr/bin/time -v -o evaluate-resources.txt timeout 7200 setpriv",
            script,
        )
        self.assertLess(len(script.encode()), 16_384)

    def test_launch_specs_are_one_time_spot_with_new_client_token(self) -> None:
        specs = build_launch_specs(self.plan())
        self.assertEqual(len(specs), 3)
        for spec in specs:
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
            self.assertIn("native-page-microcluster", spec["ClientToken"])

    def test_interrupted_cell_can_restart_at_new_immutable_attempt(self) -> None:
        first = self.plan()
        second = build_plan(
            **{
                **dataclasses.asdict(first),
                "attempt": 2,
                "output_prefix": first.output_prefix.replace("a0001", "a0002"),
            }
        )
        self.assertEqual(second.attempt, 2)
        first_tokens = {spec["ClientToken"] for spec in build_launch_specs(first)}
        second_tokens = {spec["ClientToken"] for spec in build_launch_specs(second)}
        self.assertFalse(first_tokens & second_tokens)

    def test_terminal_rejects_complete_without_exact_artifact_roster(self) -> None:
        plan = self.plan()
        terminal = {
            "schema": "borsuk-page-microcluster-terminal-v1",
            "artifacts": {},
            "attempt": 1,
            "claim_eligible": False,
            "elapsed_seconds": 100,
            "exit_code": 0,
            "instance_id": "i-0123456789abcdef0",
            "phase": "complete",
            "source_commit": plan.source_commit,
            "status": "complete",
        }
        body = (
            json.dumps(terminal, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        with self.assertRaisesRegex(ValueError, "artifact"):
            _validate_terminal_bytes(body, plan, terminal["instance_id"])

    def test_terminal_accepts_exact_complete_and_interrupted_receipts(self) -> None:
        plan = self.plan()
        instance_id = "i-0123456789abcdef0"
        names = {
            "representatives": "representatives.parquet",
            "sealed": "sealed.json",
            "evidence": "evidence.parquet",
            "result": "result.json",
            "validation": "validation.json",
            "construct-resources": "construct-resources.txt",
            "evaluate-resources": "evaluate-resources.txt",
            "validate-resources": "validate-resources.txt",
        }
        terminal = {
            "schema": "borsuk-page-microcluster-terminal-v1",
            "artifacts": {
                role: {
                    "role": role,
                    "uri": plan.output_prefix + "/artifacts/" + name,
                    "sha256": "ab" * 32,
                    "encoded_bytes": 42,
                }
                for role, name in names.items()
            },
            "attempt": 1,
            "claim_eligible": False,
            "elapsed_seconds": 100,
            "exit_code": 0,
            "instance_id": instance_id,
            "phase": "complete",
            "source_commit": plan.source_commit,
            "status": "complete",
        }
        body = (
            json.dumps(terminal, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        self.assertEqual(_validate_terminal_bytes(body, plan, instance_id), terminal)
        terminal.update(artifacts={}, exit_code=143, phase="evaluate", status="failed")
        body = (
            json.dumps(terminal, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        self.assertEqual(_validate_terminal_bytes(body, plan, instance_id), terminal)


if __name__ == "__main__":
    unittest.main()
