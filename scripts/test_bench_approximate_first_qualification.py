import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNNER = (ROOT / "scripts/bench_approximate_first_qualification.sh").read_text()
MANIFEST = json.loads(
    (ROOT / "docs/research/approximate-first-cohere-1m-local-qualification.json").read_text()
)


class ApproximateFirstRunnerContractTest(unittest.TestCase):
    def test_manifest_freezes_full_realistic_corpus_and_all_queries(self):
        self.assertEqual(MANIFEST["dataset"], "cohere-medium-1M")
        self.assertEqual(MANIFEST["corpus_vectors"], 1_000_000)
        self.assertEqual(MANIFEST["dimensions"], 768)
        self.assertEqual(MANIFEST["queries"], 1_000)
        self.assertEqual(MANIFEST["k"], 10)
        self.assertEqual(MANIFEST["cache_profile"], "uncached")

    def test_runner_captures_immutable_identity_before_measurement(self):
        for evidence in (
            "source_commit",
            "source_archive_sha256",
            "manifest_sha256",
            "dataset_descriptor_sha256",
            "binary_sha256",
            "source_tree_clean",
            "origin_main_ancestor",
        ):
            self.assertIn(evidence, RUNNER)
        self.assertLess(
            RUNNER.index("qualification_identity.json"),
            RUNNER.index('BORSUK_BENCH_APPROXIMATE_FIRST_PAIR=1'),
        )

    def test_valid_rejection_is_terminal_not_infrastructure_failure(self):
        self.assertIn("APPROXIMATE_FIRST_QUALIFICATION_REJECTED", RUNNER)
        self.assertIn("decision_status -eq 1", RUNNER)
        self.assertIn('-s "$output/approximate-first-decision.json"', RUNNER)
        self.assertIn("evaluator failed without a valid decision", RUNNER)
        self.assertIn("APPROXIMATE_FIRST_QUALIFICATION_FAILED", RUNNER)

    def test_interrupts_and_termination_are_failure_marked(self):
        self.assertIn("trap 'mark_failure 130; exit 130' INT", RUNNER)
        self.assertIn("trap 'mark_failure 143; exit 143' TERM", RUNNER)

    def test_gitless_remote_execution_verifies_the_source_archive(self):
        self.assertIn("gitless execution requires BORSUK_SOURCE_COMMIT", RUNNER)
        self.assertIn("gitless execution requires BORSUK_SOURCE_ARCHIVE", RUNNER)
        self.assertIn("source archive SHA-256 mismatch", RUNNER)

    def test_devbox_compiler_wrapper_is_only_enabled_when_installed(self):
        self.assertIn("-x /usr/local/libexec/devbox-rustc-wrapper", RUNNER)
        self.assertNotIn(
            'RUSTC_WRAPPER="${RUSTC_WRAPPER:-/usr/local/libexec/devbox-rustc-wrapper}"',
            RUNNER,
        )


if __name__ == "__main__":
    unittest.main()
