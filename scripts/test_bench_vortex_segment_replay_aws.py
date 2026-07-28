#!/usr/bin/env python3
"""Safety contract for the real-artifact Parquet/Vortex AWS replay."""

from __future__ import annotations

import unittest
from pathlib import Path


class VortexSegmentReplayCampaignTest(unittest.TestCase):
    def test_campaign_is_fresh_materialized_and_resource_sampled(self) -> None:
        source = (
            Path(__file__).resolve().parent / "bench_vortex_segment_replay_aws.sh"
        ).read_text()

        self.assertIn("${BORSUK_VORTEX_SOURCE_URI:?", source)
        self.assertIn("${BORSUK_VORTEX_RESULT_PREFIX:?", source)
        self.assertIn("${BORSUK_SOURCE_SHA256:?", source)
        self.assertIn("head-object", source)
        self.assertIn("refusing to overwrite", source)
        self.assertIn("python 3.13", source)
        self.assertIn("pyarrow==24.0.0", source)
        self.assertIn("vortex-data==0.81.0", source)
        self.assertIn("benchmark_with_resources.py", source)
        self.assertIn("benchmark_borsuk_table_formats.py", source)
        self.assertIn("--families segments", source)
        self.assertIn(
            "--formats parquet,vortex-default,vortex-compact",
            source,
        )
        self.assertIn("--execution-modes materialized_arrow", source)
        self.assertIn("--warmups 3", source)
        self.assertIn("--repetitions 30", source)
        self.assertIn("resources.csv", source)
        self.assertIn("samples.csv", source)
        self.assertIn("summary.csv", source)
        self.assertIn("network_receive_bytes", source)
        self.assertIn("process_read_bytes", source)
        self.assertIn("rss_bytes", source)
        self.assertIn("stddev_ms", source)
        self.assertIn("render_resource_charts.py", source)
        self.assertIn("vortex-segment-replay-materialized-arrow-experiment.svg", source)
        self.assertIn("render_borsuk_table_format_charts.py", source)
        self.assertIn("vortex-segment-replay-table-formats.svg", source)
        self.assertIn("Storage footprint", source)
        self.assertIn("Latency distributions by workload", source)
        for panel in (
            "CPU utilization",
            "Process memory",
            "Disk and cache footprint",
            "Network I/O",
        ):
            self.assertIn(panel, source)
        self.assertLess(
            source.index("render_resource_charts.py"),
            source.index("VORTEX_SEGMENT_REPLAY_COMPLETE"),
        )
        self.assertLess(
            source.index("render_borsuk_table_format_charts.py"),
            source.index("VORTEX_SEGMENT_REPLAY_COMPLETE"),
        )
        self.assertIn('aws --region "$REGION" s3 sync', source)
        self.assertIn("VORTEX_SEGMENT_REPLAY_COMPLETE", source)
        self.assertIn("resource_scope=process-tree", source)
        self.assertNotIn("--execution-modes compressed_native", source)
        self.assertNotIn("bench_publication_aws.sh", source)


if __name__ == "__main__":
    unittest.main()
