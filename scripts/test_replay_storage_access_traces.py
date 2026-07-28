#!/usr/bin/env python3
"""Tests for the checked storage-access trace replay gate."""

from __future__ import annotations

import copy
import csv
import tempfile
import unittest
from pathlib import Path

from scripts import replay_storage_access_traces as replay

TRACE_FIELDS = (
    "operation",
    "object_role",
    "path",
    "physical_format",
    "object_bytes",
    "request_count",
    "bytes_fetched",
    "logical_projection",
    "row_selection",
    "logical_rows_requested",
    "logical_rows_decoded",
    "decode_cpu_ns",
    "cache_state",
    "status",
)


class StorageAccessReplayTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        source = self.root / "segments" / "a.parquet"
        source.parent.mkdir()
        source.write_bytes(b"immutable-source")
        self.trace_path = self.root / "trace.csv"
        with self.trace_path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=TRACE_FIELDS)
            writer.writeheader()
            writer.writerow(
                {
                    "operation": "decode",
                    "object_role": "normal_segment",
                    "path": "segments/a.parquet",
                    "physical_format": "parquet",
                    "object_bytes": source.stat().st_size,
                    "request_count": 1,
                    "bytes_fetched": source.stat().st_size,
                    "logical_projection": "record_id|pq_codes",
                    "row_selection": "all",
                    "logical_rows_requested": 4,
                    "logical_rows_decoded": 4,
                    "decode_cpu_ns": 100,
                    "cache_state": "backing",
                    "status": "ok",
                }
            )
        self.manifest = replay.build_manifest(
            self.trace_path,
            self.root,
            formats=("parquet", "vortex-default"),
            minimum_samples=30,
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def valid_rows(self) -> list[dict[str, object]]:
        trace_id = self.manifest["operations"][0]["trace_id"]
        rows = []
        for format_name in ("parquet", "vortex-default"):
            for repetition in range(1, 31):
                rows.append(
                    {
                        "trace_id": trace_id,
                        "format": format_name,
                        "repetition": repetition,
                        "elapsed_ms": 1.0 + repetition / 100,
                        "requests": 2,
                        "bytes_fetched": 9,
                        "decode_cpu_ns": 100,
                        "peak_rss_bytes": 4096,
                        "logical_checksum": "same-values",
                        "materialized": True,
                        "cache_state": "backing",
                        "status": "complete",
                    }
                )
        return rows

    def test_manifest_checksums_sources_and_normalizes_real_trace_operations(
        self,
    ) -> None:
        operation = self.manifest["operations"][0]

        self.assertEqual(operation["projection"], ["record_id", "pq_codes"])
        self.assertEqual(operation["row_selection"], "all")
        self.assertEqual(len(operation["trace_id"]), 64)
        self.assertEqual(set(self.manifest["source_objects"]), {"segments/a.parquet"})
        self.assertEqual(self.manifest["execution_contract"], "materialized_arrow")

    def test_complete_paired_materialized_replay_is_accepted(self) -> None:
        replay.validate_replay(self.manifest, self.valid_rows(), self.root)

    def test_rejects_unpaired_under_sampled_or_cache_drifted_results(self) -> None:
        rows = self.valid_rows()
        with self.assertRaisesRegex(ValueError, "paired repetitions"):
            replay.validate_replay(self.manifest, rows[:-1], self.root)

        rows = self.valid_rows()
        rows = [row for row in rows if int(row["repetition"]) < 30]
        with self.assertRaisesRegex(ValueError, "at least 30"):
            replay.validate_replay(self.manifest, rows, self.root)

        rows = self.valid_rows()
        rows[0]["cache_state"] = "memory"
        with self.assertRaisesRegex(ValueError, "cache state"):
            replay.validate_replay(self.manifest, rows, self.root)

    def test_rejects_unequal_values_or_missing_materialization(self) -> None:
        rows = self.valid_rows()
        rows[-1]["logical_checksum"] = "different-values"
        with self.assertRaisesRegex(ValueError, "logical checksum"):
            replay.validate_replay(self.manifest, rows, self.root)

        rows = self.valid_rows()
        rows[0]["materialized"] = False
        with self.assertRaisesRegex(ValueError, "materialized"):
            replay.validate_replay(self.manifest, rows, self.root)

    def test_rejects_changed_source_checksum(self) -> None:
        (self.root / "segments" / "a.parquet").write_bytes(b"changed")

        with self.assertRaisesRegex(ValueError, "source checksum changed"):
            replay.validate_replay(self.manifest, self.valid_rows(), self.root)

    def test_rejects_manifest_source_checksum_tampering(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["source_objects"]["segments/a.parquet"] = "0" * 64

        with self.assertRaisesRegex(ValueError, "source checksum changed"):
            replay.validate_replay(manifest, self.valid_rows(), self.root)


if __name__ == "__main__":
    unittest.main()
