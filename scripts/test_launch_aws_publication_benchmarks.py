#!/usr/bin/env python3
"""Static safety checks for the detached AWS publication launcher."""

import unittest
from pathlib import Path


class AwsPublicationLauncherTest(unittest.TestCase):
    def test_launcher_is_content_addressed_detached_and_runs_full_publication_campaign(
        self,
    ) -> None:
        source = (
            Path(__file__).resolve().parent / "launch_aws_publication_benchmarks.sh"
        ).read_text()
        self.assertIn("git ls-files", source)
        self.assertIn("shasum -a 256", source)
        self.assertIn("EXPECTED_ACCOUNT", source)
        self.assertIn("ec2 wait instance-status-ok", source)
        self.assertIn("ssm wait command-executed", source)
        self.assertIn("tmux new-session -d", source)
        self.assertIn("bench_publication_aws.sh", source)
        self.assertIn("BORSUK_SOURCE_SHA256", source)
        self.assertIn("BORSUK_PUBLICATION_RUN_ID", source)
        self.assertIn("COPYFILE_DISABLE=1 tar", source)
        self.assertIn("PUBLICATION_FULL_COMPLETE", source)
        self.assertIn("runner_sentinel=", source)
        self.assertIn('[[ -f "$runner_sentinel" ]]', source)
        self.assertNotIn(
            'if [[ "$pane_dead" == "0" ]]; then startup_observed=1; fi',
            source,
        )

    def test_remote_runner_measures_dense_and_real_hybrid_rows_then_stops_worker(
        self,
    ) -> None:
        source = (
            Path(__file__).resolve().parent / "bench_publication_aws.sh"
        ).read_text()
        self.assertIn("bench_s3_full.sh", source)
        self.assertIn("bench_hybrid_retrieval_matrix.sh", source)
        self.assertIn("fetch_beir_dataset.py", source)
        self.assertIn("prepare_hybrid_dataset.py", source)
        self.assertIn("render_hybrid_retrieval_charts.py", source)
        self.assertIn("fiqa|dense sparse dense+sparse", source)
        self.assertIn("scifact|dense text dense+text", source)
        self.assertIn("nfcorpus|sparse text sparse+text", source)
        self.assertIn("BAAI/bge-small-en-v1.5", source)
        self.assertIn("5c38ec7c405ec4b44b94cc5a9bb96e735b38267a", source)
        self.assertIn("BORSUK_HYBRID_SEARCH_POINTS='128:32 256:64 512:128'", source)
        self.assertIn("BORSUK_HYBRID_HOT_FRACTIONS='0 0.25 0.5 0.75 1'", source)
        self.assertIn("BORSUK_HYBRID_RRF_KS='1 5 10 30 60'", source)
        self.assertIn("BORSUK_HYBRID_REPETITIONS=5", source)
        self.assertIn('for prefix in "$RESULT_PREFIX" "$INDEX_PREFIX"', source)
        self.assertIn("refusing to overwrite non-empty S3 prefix", source)
        self.assertIn("BORSUK_RUST_TOOLCHAIN_BIN", source)
        self.assertIn("stable-aarch64-unknown-linux-gnu/bin", source)
        self.assertIn('! -x "$TOOLCHAIN_BIN/cargo"', source)
        self.assertIn("rustc_version=", source)
        self.assertIn("RUN_DEEP_IMAGE=1", source)
        self.assertIn("s3 sync", source)
        self.assertIn("DENSE_DEFAULT_COMPLETE", source)
        self.assertIn("HYBRID_RETRIEVAL_COMPLETE", source)
        self.assertIn("PUBLICATION_FULL_COMPLETE", source)
        self.assertIn("shutdown -h", source)
        self.assertNotIn("bench_format_qualification_aws.sh", source)
        self.assertLess(
            source.index("trap finalize EXIT"), source.index("for prefix in")
        )
        self.assertIn('> "$ROOT/RUNNER_STARTED"', source)
        self.assertIn('checkpoint_temp="$(mktemp', source)
        self.assertIn('mv "$checkpoint_temp" "$ROOT/$checkpoint"', source)
        self.assertNotIn('> "$ROOT/$checkpoint"', source)


if __name__ == "__main__":
    unittest.main()
