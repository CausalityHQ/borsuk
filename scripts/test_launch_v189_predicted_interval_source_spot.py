"""A failed terminal must preserve its original phase and status."""

import unittest

from scripts.launch_v189_predicted_interval_source_spot import require_complete_terminal


class V189LauncherTests(unittest.TestCase):
    def test_failed_fit_plan_terminal_is_reported_without_missing_label_lookup(self):
        terminal = {"status": "failed", "phase": "fit-plan", "exit_code": 1,
                    "artifacts": {"out/features.jsonl": {}}}
        with self.assertRaisesRegex(RuntimeError, "fit-plan"):
            require_complete_terminal(terminal)


if __name__ == "__main__":
    unittest.main()
