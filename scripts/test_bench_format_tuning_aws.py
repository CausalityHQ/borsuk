#!/usr/bin/env python3
"""Static safety contract for the targeted AWS physical-layout sweep."""

from __future__ import annotations

import unittest
from pathlib import Path


class AwsFormatTuningCampaignTest(unittest.TestCase):
    def test_tuning_sweeps_parquet_groups_and_arrow_gaps_only(self) -> None:
        source = (
            Path(__file__).resolve().parent / "bench_format_tuning_aws.sh"
        ).read_text()

        self.assertIn("8192 32768 131072 524288", source)
        self.assertIn("65536 262144 1048576 4194304", source)
        self.assertIn("range-cap", source)
        self.assertIn("1048576:4194304", source)
        self.assertIn("1048576:8388608", source)
        self.assertIn("1048576:16777216", source)
        self.assertIn("--arrow-max-gap-bytes", source)
        self.assertIn("--arrow-max-range-bytes", source)
        self.assertIn("benchmark_with_resources.py", source)
        self.assertIn("validate_format_qualification.py", source)
        self.assertIn("local_disk s3", source)
        self.assertIn("FORMAT_TUNING_COMPLETE", source)
        self.assertNotIn("vortex-default vortex-compact", source)
        self.assertNotIn("bench_s3_full.sh", source)


if __name__ == "__main__":
    unittest.main()
