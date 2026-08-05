#!/usr/bin/env python3
"""Static fail-closed contract tests for the scalability runner."""

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
RUNNER = (ROOT / "scripts/bench_group_commit_scalability.sh").read_text()
BENCH = (ROOT / "crates/borsuk/examples/group_commit_bench.rs").read_text()


class GroupCommitScalabilityRunnerTest(unittest.TestCase):
    def test_production_uses_the_manifest_bound_and_library_default(self) -> None:
        self.assertIn(
            'MAX_RECORDS="$(python3 -c \'import json,sys; '
            'print(json.load(open(sys.argv[1]))["max_group_records"])\' "$MANIFEST")"',
            RUNNER,
        )
        self.assertIn("max_records != 1_024", BENCH)

    def test_smoke_retains_its_small_independent_bound(self) -> None:
        self.assertIn("MAX_RECORDS=8", RUNNER)
        self.assertIn("max_records != 8", BENCH)

    def test_point_visibility_uses_one_batched_routing_traversal(self) -> None:
        self.assertIn("let point_records = reopened.get_records(", BENCH)
        self.assertNotIn("reopened\n                .get_record", BENCH)


if __name__ == "__main__":
    unittest.main()
