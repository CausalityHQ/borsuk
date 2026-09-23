"""Source-only sign96 group construction and authenticated range replay."""

from __future__ import annotations

import dataclasses
import hashlib
import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts.native_rotated_sign96 import encode_records
from scripts.native_rotated_sign96_groups import (
    mean_from_source_batches,
    open_sign96_groups,
    write_sign96_groups,
)


class RotatedSign96GroupTests(unittest.TestCase):
    def test_streaming_mean_uses_source_batch_order(self) -> None:
        vectors, _, _, _ = self.fixture()
        receipt = mean_from_source_batches(
            (vectors[:4], vectors[4:]), expected_rows=8, batch_rows=4
        )
        np.testing.assert_allclose(
            receipt.mean, np.mean(vectors, axis=0, dtype=np.float64).astype(np.float32),
            rtol=0, atol=1e-7,
        )
        self.assertEqual(receipt.batch_rows, 4)
        self.assertEqual(len(receipt.source_rows_sha256), 64)
        with self.assertRaisesRegex(ValueError, "population"):
            mean_from_source_batches((vectors[:4],), expected_rows=8, batch_rows=4)
        with self.assertRaisesRegex(ValueError, "batch boundary"):
            mean_from_source_batches(
                (vectors[:3], vectors[3:7], vectors[7:]), expected_rows=8, batch_rows=4
            )

    @staticmethod
    def fixture():
        rng = np.random.default_rng(58)
        vectors = rng.standard_normal((8, 768)).astype(np.float32) / 10
        mean = np.mean(vectors, axis=0, dtype=np.float64).astype(np.float32)
        counts = (2, 1, 2, 1, 1, 1)
        first = np.asarray([5, 0, 7, 3], dtype=np.int64)
        second = np.asarray([1, 4, 2, 6], dtype=np.int64)
        return vectors, mean, counts, ((first, vectors[first]), (second, vectors[second]))

    def test_round_trip_physical_order_and_group_authentication(self) -> None:
        vectors, _, counts, batches = self.fixture()
        mean_receipt = mean_from_source_batches(
            (batch[1] for batch in batches), expected_rows=8, batch_rows=4
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            written = write_sign96_groups(
                root, mean_receipt, counts, batches,
                source_sha256="11" * 32, layout_sha256="22" * 32,
                physical_order_sha256="33" * 32, rotation_seed=20260923,
            )
            seal_sha = written.seal_sha256
            reader = open_sign96_groups(
                root, source_sha256="11" * 32, layout_sha256="22" * 32,
                physical_order_sha256="33" * 32,
                seal_sha256=seal_sha, rotation_seed=20260923,
            )
            self.assertEqual(written.seal["schema"], "borsuk-rotated-sign96-groups-v1")
            self.assertEqual(written.seal["source_rows_sha256"], mean_receipt.source_rows_sha256)
            self.assertEqual(seal_sha, hashlib.sha256((root / "seal.json").read_bytes()).hexdigest())
            self.assertEqual(len(reader.ranges), 2)
            self.assertEqual(reader.ranges[0][3], 4 + 4 * 4 + 6 * 96)
            self.assertEqual(reader.ranges[1][3], 4 + 4 * 2 + 2 * 96)
            records = np.concatenate([reader.group_records(0), reader.group_records(1)])
            np.testing.assert_array_equal(records, encode_records(vectors, mean_receipt.mean, rotation_seed=20260923))
            with self.assertRaisesRegex(ValueError, "identity"):
                open_sign96_groups(
                    root, source_sha256="ff" * 32, layout_sha256="22" * 32,
                    physical_order_sha256="33" * 32,
                    seal_sha256=seal_sha, rotation_seed=20260923,
                )
            with self.assertRaisesRegex(ValueError, "seal identity"):
                open_sign96_groups(
                    root, source_sha256="11" * 32, layout_sha256="22" * 32,
                    physical_order_sha256="33" * 32,
                    seal_sha256="ff" * 32, rotation_seed=20260923,
                )
            mean_body = bytearray((root / "mean.bin").read_bytes())
            mean_body[0] ^= 1
            (root / "mean.bin").write_bytes(mean_body)
            with self.assertRaisesRegex(ValueError, "object identity"):
                open_sign96_groups(
                    root, source_sha256="11" * 32, layout_sha256="22" * 32,
                    physical_order_sha256="33" * 32,
                    seal_sha256=seal_sha, rotation_seed=20260923,
                )
            (root / "mean.bin").write_bytes(np.asarray(mean_receipt.mean, dtype="<f4").tobytes())
            body = bytearray((root / "groups.bin").read_bytes())
            body[25] ^= 1
            (root / "groups.bin").write_bytes(body)
            with self.assertRaisesRegex(ValueError, "identity"):
                open_sign96_groups(
                    root, source_sha256="11" * 32, layout_sha256="22" * 32,
                    physical_order_sha256="33" * 32,
                    seal_sha256=seal_sha, rotation_seed=20260923,
                )

    def test_rejects_incomplete_duplicate_or_truth_bearing_construction(self) -> None:
        vectors, _, counts, batches = self.fixture()
        mean_receipt = mean_from_source_batches(
            (batch[1] for batch in batches), expected_rows=8, batch_rows=4
        )
        common = dict(
            source_sha256="11" * 32, layout_sha256="22" * 32,
            physical_order_sha256="33" * 32, rotation_seed=20260923,
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(ValueError, "population"):
                write_sign96_groups(root, mean_receipt, counts, batches[:1], **common)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(ValueError, "duplicate"):
                write_sign96_groups(root, mean_receipt, counts, (batches[0], batches[0]), **common)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            negative = batches[0][0].copy()
            negative[2] = -1  # would otherwise alias physical ordinal seven
            with self.assertRaisesRegex(ValueError, "source batch"):
                write_sign96_groups(
                    root, mean_receipt, counts, ((negative, batches[0][1]), batches[1]), **common
                )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            changed = batches[1][1].copy()
            changed[0, 0] += np.float32(0.01)
            with self.assertRaisesRegex(ValueError, "source rows"):
                write_sign96_groups(
                    root, mean_receipt, counts, (batches[0], (batches[1][0], changed)), **common
                )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            changed_mean = mean_receipt.mean.copy()
            changed_mean[0] += np.float32(0.01)
            with self.assertRaisesRegex(ValueError, "mean receipt"):
                write_sign96_groups(
                    root, dataclasses.replace(mean_receipt, mean=changed_mean), counts,
                    batches, **common,
                )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "truth.parquet").write_bytes(b"forbidden")
            with self.assertRaisesRegex(ValueError, "source-only"):
                write_sign96_groups(root, mean_receipt, counts, batches, **common)


if __name__ == "__main__":
    unittest.main()
