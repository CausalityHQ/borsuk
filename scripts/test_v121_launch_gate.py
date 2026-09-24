"""The untouched D96 split opens only after the sealed 100k screen passes."""

import hashlib
import json
import unittest

from scripts.launch_v121_deep_image_paired_spot import validate_v122_gate


class V121LaunchGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.summary = {
            "schema": "borsuk-v122-deep-image-100k-development-v1",
            "dataset": "deep-image-96-angular-random-100k",
            "split": "test-ordinals-9000-through-9999",
            "query_count": 1000,
            "rows": 100000,
            "dimensions": 96,
            "returned_hits": {"candidate": 99106, "baseline": 98203},
            "p05_hits": {"candidate": 98, "baseline": 94},
            "sub90_queries": {"candidate": 0, "baseline": 12},
            "maximum_gets": {"candidate": 1, "baseline": 32},
            "maximum_bytes": {"candidate": 10800000, "baseline": 3760128},
            "qualifies_100k_screen": True,
            "live_s3_measured": False,
        }

    def sealed(self):
        raw = (json.dumps(self.summary, sort_keys=True) + "\n").encode()
        terminal = {
            "schema": "borsuk-v122-deep-image-100k-spot-v1",
            "source_commit": "afe07cb5a9ba8518263375595f589639fdf3f4f1",
            "instance_id": "i-0bffec86c5ba0f78b",
            "status": "complete", "exit_code": 0, "phase": "complete",
            "artifacts": {"summary.json": {"bytes": len(raw),
                "sha256": hashlib.sha256(raw).hexdigest()}},
        }
        return terminal, raw

    def test_accepts_sealed_passing_screen(self) -> None:
        terminal, raw = self.sealed()
        self.assertEqual(validate_v122_gate(terminal, raw), terminal["instance_id"])

    def test_rejects_false_pass_and_split_change(self) -> None:
        for change in ({"qualifies_100k_screen": False},
                       {"split": "test-ordinals-0-through-999"},
                       {"returned_hits": {"candidate": 98000, "baseline": 98203}}):
            with self.subTest(change=change):
                self.summary.update(change)
                terminal, raw = self.sealed()
                with self.assertRaises(ValueError):
                    validate_v122_gate(terminal, raw)
                self.setUp()

    def test_rejects_tampered_summary_or_failed_terminal(self) -> None:
        terminal, raw = self.sealed()
        with self.assertRaises(ValueError):
            validate_v122_gate(terminal, raw + b" ")
        terminal["status"] = "failed"
        with self.assertRaises(ValueError):
            validate_v122_gate(terminal, raw)


if __name__ == "__main__":
    unittest.main()
