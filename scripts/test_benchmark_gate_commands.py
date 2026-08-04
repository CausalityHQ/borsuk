import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


class BenchmarkGateCommandTests(unittest.TestCase):
    def test_production_scripts_run_the_current_pending_publication_fault_gate(self):
        expected = (
            "collection_transaction_is_invisible_when_pending_publication_fails"
        )
        obsolete = (
            "collection_transaction_is_invisible_when_frontier_publication_fails"
        )
        for relative in (
            "scripts/bench_group_commit_scalability.sh",
            "scripts/bench_logical_cell_routing.sh",
        ):
            with self.subTest(script=relative):
                script = (ROOT / relative).read_text(encoding="utf-8")
                self.assertIn(f"run_exact_test fault_injection {expected}", script)
                self.assertNotIn(obsolete, script)
                self.assertIn(
                    "test result: ok. 1 passed; 0 failed;",
                    script,
                )


if __name__ == "__main__":
    unittest.main()
