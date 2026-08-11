#!/usr/bin/env python3

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUNNER = (ROOT / "scripts/bench_object_store_floor.sh").read_text(encoding="utf-8")
BENCH = (ROOT / "crates/borsuk/examples/object_store_floor.rs").read_text(
    encoding="utf-8"
)


class ObjectStoreFloorRunnerTest(unittest.TestCase):
    def test_paid_run_is_explicit_and_uses_fresh_paths(self) -> None:
        self.assertIn("BORSUK_RUN_OBJECT_STORE_FLOOR", RUNNER)
        self.assertIn('[[ ! -e "$OUTPUT" ]]', RUNNER)
        self.assertIn("refusing to replace output", BENCH)
        self.assertIn(
            "a frozen source archive must provide BORSUK_SOURCE_SHA256", RUNNER
        )

    def test_chain_times_payload_then_conditional_head_update(self) -> None:
        self.assertIn('"put_then_conditional_update"', BENCH)
        self.assertIn("PutMode::Update(chain_version)", BENCH)
        self.assertIn("OBJECT_STORE_FLOOR_COMPLETE", BENCH)

    def test_runner_preserves_resources_and_validates_before_sync(self) -> None:
        self.assertIn("resources.txt", RUNNER)
        self.assertLess(
            RUNNER.index("validate_object_store_floor.py"),
            RUNNER.index("sync_results\ntrap - EXIT"),
        )


if __name__ == "__main__":
    unittest.main()
