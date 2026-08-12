import hashlib
import json
import unittest

from scripts.publication_v3_protocol import build_schedule_document, canonical_json_bytes, validate_manifest
from scripts.publication_v3_results import validate_cell_result, validate_object_roster
from scripts.test_publication_v3_protocol import valid_v3_manifest


def data_object(index: int, rows: int, byte_count: int = 64 * 1024 * 1024) -> dict[str, object]:
    return {
        "role": "data-bundle",
        "path": f"bundles/{index:04d}.parquet",
        "format": "parquet",
        "bytes": byte_count,
        "rows": rows,
        "checksum": f"{index + 1:064x}",
    }


def control_object(index: int) -> dict[str, object]:
    return {
        "role": "control",
        "path": f"controls/{index:04d}.json",
        "format": "json",
        "bytes": 1024,
        "rows": 0,
        "checksum": f"{index + 1000:064x}",
    }


class PublicationV3ResultTests(unittest.TestCase):
    def test_cell_result_binds_protocol_source_quality_latency_and_resources(self) -> None:
        manifest = validate_manifest(valid_v3_manifest())
        cell = build_schedule_document(manifest)["cells"][0]
        protocol = canonical_json_bytes(cell) + b"\n"
        roster = [data_object(0, cell["dataset"]["scale"]["rows"])]
        result = {
            "schema_version": 1,
            "status": "complete",
            "cell_id": cell["cell_id"],
            "manifest_sha256": cell["manifest_sha256"],
            "protocol_sha256": hashlib.sha256(protocol).hexdigest(),
            "source_archive_sha256": "a" * 64,
            "attempt_id": "attempt-01",
            "instance_identity": "local-test",
            "metrics": {
                "queries": 1000,
                "correctness_ppm": 950000,
                "latency_p50_us": 1000,
                "latency_p95_us": 2000,
                "latency_p99_us": 3000,
                "throughput_milli_per_second": 100000,
                "cpu_ns": 1000000,
                "peak_rss_bytes": 10000000,
                "disk_read_bytes": 0,
                "disk_write_bytes": 0,
                "storage_gets": 10,
                "storage_puts": 0,
                "storage_bytes_read": 4096,
                "storage_bytes_written": 0,
            },
            "object_roster": roster,
        }
        validated = validate_cell_result(
            result,
            cell=cell,
            protocol_bytes=protocol,
            source_archive_sha256="a" * 64,
        )
        self.assertEqual(validated, result)

        for mutation in (
            {**result, "protocol_sha256": "b" * 64},
            {**result, "source_archive_sha256": "b" * 64},
            {**result, "metrics": {**result["metrics"], "correctness_ppm": 949999}},
        ):
            with self.subTest(mutation=json.dumps(mutation, sort_keys=True)[:100]):
                with self.assertRaises(ValueError):
                    validate_cell_result(
                        mutation,
                        cell=cell,
                        protocol_bytes=protocol,
                        source_archive_sha256="a" * 64,
                    )

    def test_cell_result_rejects_missing_resource_or_storage_telemetry(self) -> None:
        manifest = validate_manifest(valid_v3_manifest())
        cell = build_schedule_document(manifest)["cells"][0]
        protocol = canonical_json_bytes(cell) + b"\n"
        base_metrics = {
            "queries": 1000,
            "correctness_ppm": 950000,
            "latency_p50_us": 1000,
            "latency_p95_us": 2000,
            "latency_p99_us": 3000,
            "throughput_milli_per_second": 100000,
            "cpu_ns": 1000000,
            "peak_rss_bytes": 10000000,
            "disk_read_bytes": 0,
            "disk_write_bytes": 0,
            "storage_gets": 10,
            "storage_puts": 0,
            "storage_bytes_read": 4096,
            "storage_bytes_written": 0,
        }
        for missing in ("peak_rss_bytes", "storage_gets", "latency_p99_us"):
            metrics = {key: value for key, value in base_metrics.items() if key != missing}
            value = {
                "schema_version": 1,
                "status": "complete",
                "cell_id": cell["cell_id"],
                "manifest_sha256": cell["manifest_sha256"],
                "protocol_sha256": hashlib.sha256(protocol).hexdigest(),
                "source_archive_sha256": "a" * 64,
                "attempt_id": "attempt-01",
                "instance_identity": "local-test",
                "metrics": metrics,
                "object_roster": [data_object(0, cell["dataset"]["scale"]["rows"])],
            }
            with self.subTest(missing=missing), self.assertRaises(ValueError):
                validate_cell_result(
                    value,
                    cell=cell,
                    protocol_bytes=protocol,
                    source_archive_sha256="a" * 64,
                )

    def test_large_scale_roster_requires_multiple_bounded_data_bundles(self) -> None:
        roster = [data_object(0, 5_000_000), data_object(1, 5_000_000)]
        summary = validate_object_roster(roster, logical_rows=10_000_000)
        self.assertEqual(summary["data_bundles"], 2)
        self.assertEqual(summary["represented_rows"], 10_000_000)
        self.assertEqual(summary["maximum_object_bytes"], 64 * 1024 * 1024)

        with self.assertRaisesRegex(ValueError, "multiple data bundles"):
            validate_object_roster(
                [data_object(0, 10_000_000, 128 * 1024 * 1024)],
                logical_rows=10_000_000,
            )

    def test_roster_rejects_oversize_duplicate_and_logical_total_drift(self) -> None:
        cases = (
            [data_object(0, 500_000, 128 * 1024 * 1024 + 1), data_object(1, 500_000)],
            [data_object(0, 500_000), data_object(0, 500_000)],
            [data_object(0, 400_000), data_object(1, 400_000)],
        )
        for roster in cases:
            with self.subTest(roster=roster), self.assertRaises(ValueError):
                validate_object_roster(roster, logical_rows=1_000_000)

    def test_roster_rejects_object_per_row_or_logical_cell_amplification(self) -> None:
        with self.assertRaisesRegex(ValueError, "object-count amplification"):
            validate_object_roster(
                [data_object(index, 1, 1024) for index in range(1025)],
                logical_rows=1025,
            )
        per_cell = [data_object(index, 6103, 1024) for index in range(16_383)]
        per_cell.append(data_object(16_383, 100_000_000 - 6103 * 16_383, 1024))
        with self.assertRaisesRegex(ValueError, "object-count amplification"):
            validate_object_roster(
                per_cell,
                logical_rows=100_000_000,
                logical_cells=16_384,
            )
        with self.assertRaisesRegex(ValueError, "control-object amplification"):
            validate_object_roster(
                [data_object(0, 1_000_000), data_object(1, 1_000_000)]
                + [control_object(index) for index in range(257)],
                logical_rows=2_000_000,
            )

    def test_small_dataset_allows_fixed_query_and_directory_objects(self) -> None:
        roster = [data_object(0, 3_633)]
        for index in range(8):
            roster.append(
                {
                    "role": "query-page" if index < 4 else "directory",
                    "path": f"metadata/{index}.parquet",
                    "format": "parquet",
                    "bytes": 4096,
                    "rows": 0,
                    "checksum": f"{index + 2000:064x}",
                }
            )
        summary = validate_object_roster(roster, logical_rows=3_633)
        self.assertEqual(summary["data_objects"], 9)


if __name__ == "__main__":
    unittest.main()
