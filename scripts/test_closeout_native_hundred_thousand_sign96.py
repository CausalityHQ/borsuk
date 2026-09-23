"""Closed sign96 evidence is bound to readback bytes and resource receipts."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.closeout_native_hundred_thousand_sign96 import closeout_receipts
from scripts.native_hundred_thousand_sign96_worker import ARTIFACT_FILES
from scripts.test_launch_native_hundred_thousand_sign96_spot import Sign96SpotTest


def _body(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


class Sign96CloseoutTest(unittest.TestCase):
    def test_closed_readback_and_resource_gate(self) -> None:
        plan = Sign96SpotTest.plan()
        archive = b"archive"
        plan = dataclasses.replace(
            plan,
            source_archive=dataclasses.replace(
                plan.source_archive,
                sha256=hashlib.sha256(archive).hexdigest(), encoded_bytes=len(archive),
            ),
        )
        cap = 3 * 1024**3 - 64 * 1024**2
        resources = {
            phase: {"maximum_rss_bytes": 1024, "tree_peak_bytes": 1024, "swaps": 0}
            for phase in ("construct", "plan", "evaluate", "validate")
        }
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.tar.gz").write_bytes(archive)
            (root / "reservation.json").write_bytes(_body({
                "schema": "borsuk-hundred-thousand-sign96-reservation-v1",
                "attempt": 1, "source_commit": plan.source_commit,
                "source_archive": dataclasses.asdict(plan.source_archive),
                "requirements_sha256": plan.requirements_sha256,
            }))
            for role, path in ARTIFACT_FILES.items():
                target = root / path
                target.parent.mkdir(parents=True, exist_ok=True)
                if role.endswith("-resources"):
                    target.write_text(
                        "Maximum resident set size (kbytes): 1\nSwaps: 0\n"
                    )
                elif role.endswith("-peak"):
                    target.write_text("1024\n")
                else:
                    target.write_bytes(b"sealed")
            result = {"quality_advance_candidate": True}
            validation = {"valid": True}
            (root / ARTIFACT_FILES["result"]).write_bytes(_body(result))
            (root / ARTIFACT_FILES["validation"]).write_bytes(_body(validation))
            (root / ARTIFACT_FILES["decision"]).write_bytes(_body({
                "schema": "borsuk-hundred-thousand-sign96-decision-v1",
                "decision": "advance", "resource_cap_bytes": cap,
                "quality_advance_candidate": True, "resource_pass": True,
                "resources": resources,
                "result_sha256": hashlib.sha256(_body(result)).hexdigest(),
                "validation_sha256": hashlib.sha256(_body(validation)).hexdigest(),
            }))
            artifacts = {
                role: {
                    "role": role, "uri": plan.output_prefix + "/artifacts/" + path,
                    "encoded_bytes": len((root / path).read_bytes()),
                    "sha256": hashlib.sha256((root / path).read_bytes()).hexdigest(),
                }
                for role, path in ARTIFACT_FILES.items()
            }
            instance = "i-0123456789abcdef0"
            (root / "terminal.json").write_bytes(_body({
                "schema": "borsuk-hundred-thousand-sign96-terminal-v1",
                "status": "complete", "phase": "complete", "exit_code": 0,
                "elapsed_seconds": 1, "instance_id": instance, "failure_reason": "",
                "source_commit": plan.source_commit,
                "source_archive": dataclasses.asdict(plan.source_archive),
                "requirements_sha256": plan.requirements_sha256,
                "attempt": 1, "claim_eligible": False, "decision": "advance",
                "artifacts": artifacts,
            }))
            self.assertEqual(closeout_receipts(root, plan, instance)["decision"], "advance")
            (root / ARTIFACT_FILES["plan-peak"]).write_text("2048\n")
            with self.assertRaisesRegex(ValueError, "readback differs"):
                closeout_receipts(root, plan, instance)


if __name__ == "__main__":
    unittest.main()
