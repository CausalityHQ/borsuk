import hashlib
import json
import unittest

from scripts.publication_v3_attestation import runtime_attestation_sha256
from scripts.publication_v3_clones import (
    build_clone_receipt,
    clone_receipt_document_sha256,
)
from scripts.publication_v3_protocol import (
    build_schedule_document,
    canonical_json_bytes,
    validate_manifest,
)
from scripts.publication_v3_receipts import build_index_receipt, receipt_document_sha256
from scripts.publication_v3_results import validate_cell_result, validate_object_roster
from scripts.test_publication_v3_protocol import paid_v3_manifest
from scripts.test_publication_v3_receipts import (
    build_artifact,
    build_metrics,
    data_roster,
)


def data_object(index: int, rows: int, byte_count: int = 64 * 1024 * 1024) -> dict[str, object]:
    return {
        "role": "data-bundle",
        "path": f"bundles/{index:04d}.parquet",
        "format": "parquet",
        "bytes": byte_count,
        "rows": rows,
        "checksum": f"{index + 1:064x}",
        "etag": f'"{index + 1:032x}-2"',
    }


def control_object(index: int) -> dict[str, object]:
    return {
        "role": "control",
        "path": f"controls/{index:04d}.json",
        "format": "json",
        "bytes": 1024,
        "rows": 0,
        "checksum": f"{index + 1000:064x}",
        "etag": f'"{index + 1000:032x}"',
    }


def first_read_arm(cell: dict[str, object]) -> dict[str, object]:
    factors = cell["workload"]["factors"]
    return {
        "k": factors["k"][0],
        "candidate_budget": factors["candidate_budgets"][0],
        "routing_cell_budget": factors["routing_cell_budget"],
        "cache_state": factors["cache_states"][0],
    }


def receipt_for(cell: dict[str, object]) -> dict[str, object]:
    return build_index_receipt(
        cell=cell,
        source_archive_sha256="a" * 64,
        dataset_materialization_sha256="d" * 64,
        build_attempt_id="build-attempt-01",
        builder_instance_identity="i-builder-01",
        builder_instance_type=cell["environment_contract"]["build_workers"][cell["system"]]["instance_type"],
        build_artifact=build_artifact(cell),
        object_roster=data_roster(cell),
        build_metrics=build_metrics(),
    )


def runtime_attestation_for(
    cell: dict[str, object], *, instance_id: str = "local-test"
) -> dict[str, object]:
    client = cell["environment_contract"]["runtime_clients"][cell["system"]]
    return {
        "schema_version": 1,
        "cell_id": cell["cell_id"],
        "attempt_id": "attempt-01",
        "instance_id": instance_id,
        "instance_type": client["instance_type"],
        "architecture": cell["environment_contract"]["architecture"],
        "vcpus": client["vcpus"],
        "memory_max_bytes": client["memory_mib"] * 1024 * 1024,
        "memory_peak_bytes": 1024 * 1024 * 1024,
        "swap_max_bytes": 0,
        "swap_current_bytes": 0,
        "swap_peak_bytes": 0,
        "oom_events": 0,
        "oom_kill_events": 0,
        "cache_limit_bytes": client["disk_cache_limit_mib"] * 1024 * 1024,
        "cache_filesystem_bytes": 32 * 1024 * 1024 * 1024,
        "cache_device": "259:1",
        "root_device": "259:0",
        "cache_is_mount": True,
        "source_revision": "1" * 40,
    }


class PublicationV3ResultTests(unittest.TestCase):
    def test_lifecycle_result_binds_clone_operations_accuracy_and_write_metrics(self) -> None:
        manifest = validate_manifest(paid_v3_manifest())
        cell = next(
            cell
            for cell in build_schedule_document(manifest)["cells"]
            if cell["system"] == "borsuk"
            and cell["workload"]["kind"] == "write-update-delete-compact"
        )
        from scripts.run_publication_v3_cell import plan_arms

        arm = next(arm for arm in plan_arms(cell) if arm["writers"] == 1)
        protocol = canonical_json_bytes(cell) + b"\n"
        receipt = receipt_for(cell)
        roster = data_roster(cell)
        clone = build_clone_receipt(
            cell=cell,
            arm=arm,
            attempt_id="attempt-01",
            base_receipt=receipt,
            base_roster=roster,
            copy_inventory=[
                {
                    "path": item["path"],
                    "bytes": item["bytes"],
                    "source_etag": item["etag"],
                    "destination_etag": f'"{index + 10:032x}"',
                }
                for index, item in enumerate(roster)
            ],
        )
        attestation = runtime_attestation_for(cell)
        result = {
            "schema_version": 1,
            "status": "complete",
            "cell_id": cell["cell_id"],
            "manifest_sha256": cell["manifest_sha256"],
            "protocol_sha256": hashlib.sha256(protocol).hexdigest(),
            "source_archive_sha256": "a" * 64,
            "attempt_id": "attempt-01",
            "instance_identity": "local-test",
            "arm": arm,
            "metrics": {
                "insert_ops": 1000,
                "upsert_ops": 100,
                "delete_ops": 100,
                "compact_ops": 1,
                "purge_ops": 1,
                "lifecycle_accuracy_ppm": 1_000_000,
                "batch_latency_p50_us": 1000,
                "batch_latency_p95_us": 2000,
                "batch_latency_p99_us": 3000,
                "throughput_milli_per_second": 100_000,
                "first_publish_us": 500,
                "time_to_searchable_us": 500,
                "time_to_fully_indexed_us": 4000,
                "time_to_consolidated_us": 9000,
                "write_amplification_ppm": 1_500_000,
                "cpu_ns": 1_000_000,
                "peak_rss_bytes": 10_000_000,
                "disk_read_bytes": 0,
                "disk_write_bytes": 0,
                "storage_gets": 10,
                "storage_puts": 20,
                "storage_bytes_read": 4096,
                "storage_bytes_written": 8192,
            },
            "index_receipt_sha256": receipt_document_sha256(receipt),
            "clone_receipt_sha256": clone_receipt_document_sha256(clone),
            "runtime_attestation_sha256": runtime_attestation_sha256(attestation),
        }
        self.assertEqual(
            validate_cell_result(
                result,
                cell=cell,
                protocol_bytes=protocol,
                source_archive_sha256="a" * 64,
                dataset_materialization_sha256="d" * 64,
                index_receipt=receipt,
                clone_receipt=clone,
                runtime_attestation=attestation,
            ),
            result,
        )
        with self.assertRaisesRegex(ValueError, "scheduled mutation mix"):
            validate_cell_result(
                {
                    **result,
                    "metrics": {**result["metrics"], "upsert_ops": 101},
                },
                cell=cell,
                protocol_bytes=protocol,
                source_archive_sha256="a" * 64,
                dataset_materialization_sha256="d" * 64,
                index_receipt=receipt,
                clone_receipt=clone,
                runtime_attestation=attestation,
            )

    def test_cell_result_binds_protocol_source_quality_latency_and_resources(self) -> None:
        manifest = validate_manifest(paid_v3_manifest())
        cell = next(
            cell
            for cell in build_schedule_document(manifest)["cells"]
            if cell["system"] == "borsuk"
        )
        protocol = canonical_json_bytes(cell) + b"\n"
        receipt = receipt_for(cell)
        attestation = runtime_attestation_for(cell)
        result = {
            "schema_version": 1,
            "status": "complete",
            "cell_id": cell["cell_id"],
            "manifest_sha256": cell["manifest_sha256"],
            "protocol_sha256": hashlib.sha256(protocol).hexdigest(),
            "source_archive_sha256": "a" * 64,
            "attempt_id": "attempt-01",
            "instance_identity": "local-test",
            "arm": first_read_arm(cell),
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
            "index_receipt_sha256": receipt_document_sha256(receipt),
            "clone_receipt_sha256": None,
            "runtime_attestation_sha256": runtime_attestation_sha256(attestation),
        }
        validated = validate_cell_result(
            result,
            cell=cell,
            protocol_bytes=protocol,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
            index_receipt=receipt,
            runtime_attestation=attestation,
        )
        self.assertEqual(validated, result)

        for mutation in (
            {**result, "protocol_sha256": "b" * 64},
            {**result, "source_archive_sha256": "b" * 64},
            {**result, "instance_identity": "i-foreign-runtime"},
            {**result, "metrics": {**result["metrics"], "correctness_ppm": 949999}},
            {
                **result,
                "metrics": {
                    **result["metrics"],
                    "peak_rss_bytes": (
                        cell["environment_contract"]["runtime_clients"][cell["system"]]["memory_mib"]
                        * 1024
                        * 1024
                        + 1
                    ),
                },
            },
            {**result, "arm": {**result["arm"], "k": True}},
        ):
            with self.subTest(mutation=json.dumps(mutation, sort_keys=True)[:100]):
                with self.assertRaises(ValueError):
                    validate_cell_result(
                        mutation,
                        cell=cell,
                        protocol_bytes=protocol,
                        source_archive_sha256="a" * 64,
                        dataset_materialization_sha256="d" * 64,
                        index_receipt=receipt,
                        runtime_attestation=attestation,
                    )

    def test_cell_result_rejects_missing_resource_or_storage_telemetry(self) -> None:
        manifest = validate_manifest(paid_v3_manifest())
        cell = next(
            cell
            for cell in build_schedule_document(manifest)["cells"]
            if cell["system"] == "borsuk"
        )
        protocol = canonical_json_bytes(cell) + b"\n"
        receipt = receipt_for(cell)
        attestation = runtime_attestation_for(cell)
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
                "arm": first_read_arm(cell),
                "metrics": metrics,
                "index_receipt_sha256": receipt_document_sha256(receipt),
                "clone_receipt_sha256": None,
                "runtime_attestation_sha256": runtime_attestation_sha256(attestation),
            }
            with self.subTest(missing=missing), self.assertRaises(ValueError):
                validate_cell_result(
                    value,
                    cell=cell,
                    protocol_bytes=protocol,
                    source_archive_sha256="a" * 64,
                    dataset_materialization_sha256="d" * 64,
                    index_receipt=receipt,
                    runtime_attestation=attestation,
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
                + [control_object(index) for index in range(513)],
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
                    "etag": f'"{index + 2000:032x}"',
                }
            )
        summary = validate_object_roster(roster, logical_rows=3_633)
        self.assertEqual(summary["data_objects"], 9)

    def test_roster_represents_real_multi_object_index_formats_without_relabeling(self) -> None:
        roster = [data_object(0, 1_000_000)]
        roster.extend(
            {
                "role": "query-page",
                "path": f"fidx/{index:02x}/filter-{index:04x}.fidx",
                "format": "packed",
                "bytes": 100,
                "rows": 0,
                "checksum": f"{index + 3000:064x}",
                "etag": f'"{index + 3000:032x}"',
            }
            for index in range(62)
        )
        roster.extend(
            {
                "role": "control",
                "path": f"collection/snapshots/{index:064x}.json",
                "format": "json",
                "bytes": 4096,
                "rows": 0,
                "checksum": f"{index + 4000:064x}",
                "etag": f'"{index + 4000:032x}"',
            }
            for index in range(190)
        )
        roster.extend(
            {
                "role": "control",
                "path": f"transactions/{index:032x}/STATE",
                "format": "packed",
                "bytes": 46,
                "rows": 0,
                "checksum": f"{index + 5000:064x}",
                "etag": f'"{index + 5000:032x}"',
            }
            for index in range(150)
        )

        summary = validate_object_roster(roster, logical_rows=1_000_000)

        self.assertEqual(summary["objects"], 403)
        self.assertEqual(summary["control_objects"], 340)
        with self.assertRaisesRegex(ValueError, "role or format"):
            validate_object_roster(
                [{**roster[1], "format": "json"}, *roster[2:]],
                logical_rows=1_000_000,
            )


if __name__ == "__main__":
    unittest.main()
