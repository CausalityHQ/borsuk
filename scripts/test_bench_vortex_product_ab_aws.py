#!/usr/bin/env python3
"""Safety contract for the fresh end-to-end Parquet/Vortex product A/B."""

from __future__ import annotations

import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent


class VortexProductAbCampaignTest(unittest.TestCase):
    def test_campaign_rebuilds_both_variants_and_validates_publishable_evidence(
        self,
    ) -> None:
        source = (SCRIPTS / "bench_vortex_product_ab_aws.sh").read_text()

        self.assertIn("${BORSUK_VORTEX_PRODUCT_RUN_ID:?", source)
        self.assertIn("${BORSUK_VORTEX_PRODUCT_RESULT_PREFIX:?", source)
        self.assertIn("${BORSUK_VORTEX_PRODUCT_INDEX_PREFIX:?", source)
        self.assertIn("${BORSUK_SOURCE_SHA256:?", source)
        self.assertIn("fashion-mnist-784", source)
        self.assertIn("refusing to overwrite", source)
        self.assertIn("for variant in parquet vortex", source)
        self.assertIn("for query_path in production segment", source)
        self.assertIn("benchmark_with_resources.py", source)
        self.assertIn("run_vortex_product_ab_variant.sh", source)
        self.assertIn("dnf install -y clang clang-devel", source)
        self.assertIn("LIBCLANG_PATH", source)
        self.assertIn('BORSUK_SEGMENT_TABLE_FORMAT="$variant"', source)
        self.assertIn("BORSUK_PRODUCT_SAMPLES=30", source)
        self.assertIn("materialized_borsuk_query=true", source)
        self.assertIn("query_paths=production,segment", source)
        self.assertIn("external_warmups=0", source)
        self.assertIn("production_bench_internal_cache_warmup=true", source)
        self.assertIn("bench_build.csv", source)
        self.assertIn("bench_recall_latency.csv", source)
        self.assertIn("bench_query_samples.csv", source)
        self.assertIn("bench_cache_states.csv", source)
        self.assertIn("bench_concurrency.csv", source)
        self.assertIn("bench_concurrency_samples.csv", source)
        self.assertIn("bench_cache_coverage.csv", source)
        self.assertIn("stddev_ms", source)
        self.assertIn("render_resource_charts.py", source)
        self.assertIn("render_recall_latency_charts.py", source)
        self.assertIn("CPU utilization", source)
        self.assertIn("Process memory", source)
        self.assertIn("Disk and cache footprint", source)
        self.assertIn("Network I/O", source)
        self.assertIn("comparison.csv", source)
        self.assertIn('aws --region "$REGION" s3 sync', source)
        self.assertIn("VORTEX_PRODUCT_AB_COMPLETE", source)
        self.assertIn('checkpoint_temp="$(mktemp', source)
        self.assertIn(
            'mv "$checkpoint_temp" "$ROOT/VORTEX_PRODUCT_AB_COMPLETE"',
            source,
        )
        self.assertNotIn('> "$ROOT/VORTEX_PRODUCT_AB_COMPLETE"', source)
        self.assertLess(
            source.index("for variant in parquet vortex"),
            source.index("VORTEX_PRODUCT_AB_COMPLETE"),
        )
        self.assertLess(
            source.index("render_resource_charts.py"),
            source.index("VORTEX_PRODUCT_AB_COMPLETE"),
        )
        self.assertNotIn("bench_vortex_segment_replay_aws.sh", source)

    def test_variant_runner_separates_product_and_forced_segment_paths(self) -> None:
        source = (SCRIPTS / "run_vortex_product_ab_variant.sh").read_text()

        self.assertIn("BORSUK_PRODUCT_QUERY_PATH", source)
        self.assertIn("production|segment", source)
        self.assertIn('BORSUK_BENCH_BUILD_INDEX="$BUILD_INDEX"', source)
        self.assertIn('BORSUK_BENCH_FORCE_SEGMENT_PATH="$FORCE_SEGMENT_PATH"', source)
        self.assertIn('BORSUK_BENCH_QUERIES="$SAMPLES"', source)
        self.assertIn('BORSUK_BENCH_UNCACHED_QUERIES="$SAMPLES"', source)
        self.assertIn("BORSUK_BENCH_CONCURRENCY=1,4,16", source)
        self.assertIn("BORSUK_BENCH_READ_ONLY=1", source)
        self.assertIn("target/release/examples/production_bench", source)
        self.assertNotIn("BORSUK_BENCH_RECALL_ONLY=1", source)
        self.assertNotIn("for warmup in", source)
        self.assertNotIn("cargo run", source)

    def test_read_only_segment_arms_do_not_require_a_build_artifact(self) -> None:
        source = (SCRIPTS / "bench_vortex_product_ab_aws.sh").read_text()

        self.assertIn('if [[ "$query_path" == "production" ]]; then', source)
        self.assertIn(
            'required_artifacts="bench_build.csv,$required_artifacts"', source
        )
        self.assertIn(
            'if query_path == "production":\n    build = rows("bench_build.csv")',
            source,
        )
        self.assertNotIn(
            "--required bench_build.csv,bench_recall_latency.csv",
            source,
        )


if __name__ == "__main__":
    unittest.main()
