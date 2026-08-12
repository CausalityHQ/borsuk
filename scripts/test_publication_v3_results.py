import unittest

from scripts.publication_v3_results import validate_object_roster


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


if __name__ == "__main__":
    unittest.main()
