#!/usr/bin/env python3
"""Static safety contract for the staged AWS format campaign."""

from __future__ import annotations

import unittest
from pathlib import Path


class AwsFormatCampaignTest(unittest.TestCase):
    def test_campaign_stops_at_decision_checkpoint_and_samples_every_case(self) -> None:
        source = (
            Path(__file__).resolve().parent / "bench_format_qualification_aws.sh"
        ).read_text()
        self.assertIn("FORMAT_DECISION_REQUIRED", source)
        self.assertNotIn("bench_s3_full.sh", source)
        self.assertIn("benchmark_with_resources.py", source)
        self.assertIn("validate_format_qualification.py", source)
        self.assertIn("render_resource_charts.py", source)
        self.assertIn("local_disk s3", source)
        self.assertIn("parquet vortex-default vortex-compact", source)
        self.assertIn("arrow-ipc vortex-default vortex-compact", source)
        self.assertIn("--vortex-without-segment-cache", source)
        self.assertIn("refusing to mix repetitions", source)
        self.assertIn("BORSUK_S3_METRIC_PROPAGATION_SECONDS", source)
        self.assertIn("cloudwatch get-metric-statistics", source)
        self.assertIn("trap cleanup_metric_filters EXIT", source)
        self.assertIn("local_disk_class=", source)
        self.assertIn("environment.txt", source)
        self.assertIn("dependency preflight", source)
        self.assertIn("decoded.equals(source)", source)
        self.assertIn("probe_table_format_compatibility.py", source)
        self.assertIn("len(cases) != 15", source)
        self.assertIn("probe_vector_format_compatibility.py", source)
        self.assertIn(
            '{"float32", "float16", "bfloat16", "int8", "binary"}',
            source,
        )


if __name__ == "__main__":
    unittest.main()
