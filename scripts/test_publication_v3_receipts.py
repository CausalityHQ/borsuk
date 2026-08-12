import hashlib
import unittest

from scripts.publication_v3_protocol import (
    build_schedule_document,
    canonical_json_bytes,
    validate_manifest,
)
from scripts.publication_v3_receipts import (
    build_index_receipt,
    reconcile_index_inventory,
    require_verified_index,
    require_verified_object_roster,
    validate_index_receipt,
)
from scripts.test_publication_v3_protocol import paid_v3_manifest


def scheduled_borsuk_cell() -> dict[str, object]:
    manifest = validate_manifest(paid_v3_manifest())
    return next(
        cell
        for cell in build_schedule_document(manifest)["cells"]
        if cell["system"] == "borsuk" and cell["workload"]["kind"] == "read-recall"
    )


def data_roster(cell: dict[str, object]) -> list[dict[str, object]]:
    rows = cell["dataset"]["scale"]["rows"]
    return [
        {
            "role": "data-bundle",
            "path": "segments/00000000.parquet",
            "format": "parquet",
            "bytes": 64 * 1024 * 1024,
            "rows": rows,
            "checksum": "1" * 64,
        },
        {
            "role": "directory",
            "path": "directory/00000000.arrow",
            "format": "arrow-ipc",
            "bytes": 4096,
            "rows": 0,
            "checksum": "2" * 64,
        },
    ]


def index_stats(cell: dict[str, object]) -> dict[str, object]:
    return {
        "logical_cells": cell["index_profile"]["logical_cells"],
        "records": cell["dataset"]["scale"]["rows"],
        "total_active_index_bytes": 64 * 1024 * 1024,
        "logical_cell_catalog_checksum": "3" * 64,
    }


def build_artifact(cell: dict[str, object]) -> dict[str, dict[str, object]]:
    metrics = build_metrics()
    return {
        "index_stats": index_stats(cell),
        "storage_metrics": {
            field: metrics[field]
            for field in (
                "storage_gets",
                "storage_puts",
                "storage_deletes",
                "storage_heads",
                "storage_lists",
                "storage_bytes_read",
                "storage_bytes_written",
            )
        },
    }


def build_metrics() -> dict[str, int]:
    return {
        "cpu_ns": 10,
        "peak_rss_bytes": 20,
        "disk_read_bytes": 30,
        "disk_write_bytes": 40,
        "storage_gets": 50,
        "storage_puts": 60,
        "storage_deletes": 0,
        "storage_heads": 2,
        "storage_lists": 1,
        "storage_bytes_read": 70,
        "storage_bytes_written": 80,
        "build_elapsed_ns": 90,
    }


class PublicationV3ReceiptTests(unittest.TestCase):
    def test_runtime_requires_canonical_receipt_and_exact_object_inventory(self) -> None:
        cell = scheduled_borsuk_cell()
        receipt = build_index_receipt(
            cell=cell,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
            build_attempt_id="build-attempt-01",
            builder_instance_identity="i-builder-01",
            builder_instance_type="r7g.8xlarge",
            build_artifact=build_artifact(cell),
            object_roster=data_roster(cell),
            build_metrics=build_metrics(),
        )
        payload = canonical_json_bytes(receipt) + b"\n"
        verified, receipt_sha256 = require_verified_index(
            payload,
            cell=cell,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
        )
        self.assertEqual(verified, receipt)
        self.assertEqual(receipt_sha256, hashlib.sha256(payload).hexdigest())
        roster = data_roster(cell)
        roster_payload = canonical_json_bytes(roster) + b"\n"
        self.assertEqual(
            require_verified_object_roster(receipt, roster_payload, cell=cell), roster
        )
        self.assertNotIn("object_roster", receipt)
        self.assertLess(len(payload), 256 * 1024)
        inventory = [
            {key: item[key] for key in ("path", "bytes", "checksum")}
            for item in roster
        ]
        reconcile_index_inventory(roster, inventory)
        for mutation in (
            inventory[:-1],
            [*inventory, {"path": "extra", "bytes": 1, "checksum": "4" * 64}],
            [{**inventory[0], "bytes": inventory[0]["bytes"] + 1}, *inventory[1:]],
            [{**inventory[0], "checksum": "4" * 64}, *inventory[1:]],
        ):
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                reconcile_index_inventory(roster, mutation)
        with self.assertRaises(ValueError):
            require_verified_index(
                canonical_json_bytes(receipt),
                cell=cell,
                source_archive_sha256="a" * 64,
                dataset_materialization_sha256="d" * 64,
            )

    def test_index_receipt_binds_build_authority_and_reuses_across_repetitions(self) -> None:
        first = scheduled_borsuk_cell()
        manifest = validate_manifest(paid_v3_manifest())
        second = next(
            cell
            for cell in build_schedule_document(manifest)["cells"]
            if cell["system"] == "borsuk"
            and cell["workload"]["kind"] == first["workload"]["kind"]
            and cell["dataset"]["id"] == first["dataset"]["id"]
            and cell["repetition_id"] != first["repetition_id"]
        )
        receipt = build_index_receipt(
            cell=first,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
            build_attempt_id="build-attempt-01",
            builder_instance_identity="i-builder-01",
            builder_instance_type="r7g.8xlarge",
            build_artifact=build_artifact(first),
            object_roster=data_roster(first),
            build_metrics=build_metrics(),
        )
        validated = validate_index_receipt(
            receipt,
            cell=second,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
        )
        self.assertEqual(validated, receipt)
        self.assertEqual(receipt["index_uri"], first["index_prefix"])
        self.assertEqual(receipt["builder_instance_type"], "r7g.8xlarge")
        self.assertEqual(
            receipt["receipt_sha256"],
            hashlib.sha256(
                canonical_json_bytes(
                    {key: value for key, value in receipt.items() if key != "receipt_sha256"}
                )
            ).hexdigest(),
        )

    def test_index_receipt_rejects_substitution_and_runtime_builder(self) -> None:
        cell = scheduled_borsuk_cell()
        mismatched_metrics = build_metrics()
        mismatched_metrics["storage_gets"] += 1
        with self.assertRaisesRegex(ValueError, "bench_build.csv"):
            build_index_receipt(
                cell=cell,
                source_archive_sha256="a" * 64,
                dataset_materialization_sha256="d" * 64,
                build_attempt_id="build-attempt-01",
                builder_instance_identity="i-builder-01",
                builder_instance_type="r7g.8xlarge",
                build_artifact=build_artifact(cell),
                object_roster=data_roster(cell),
                build_metrics=mismatched_metrics,
            )
        receipt = build_index_receipt(
            cell=cell,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
            build_attempt_id="build-attempt-01",
            builder_instance_identity="i-builder-01",
            builder_instance_type="r7g.8xlarge",
            build_artifact=build_artifact(cell),
            object_roster=data_roster(cell),
            build_metrics=build_metrics(),
        )
        mutations = (
            {**receipt, "index_uri": "s3://attacker/substitute"},
            {**receipt, "source_archive_sha256": "b" * 64},
            {
                **receipt,
                "builder_instance_type": "c7g.xlarge",
            },
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validate_index_receipt(
                    mutation,
                    cell=cell,
                    source_archive_sha256="a" * 64,
                    dataset_materialization_sha256="d" * 64,
                )


if __name__ == "__main__":
    unittest.main()
