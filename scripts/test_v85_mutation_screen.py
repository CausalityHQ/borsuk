import pathlib
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.v85_mutation_screen import MutationScreenRequest, run_mutation_screen
from scripts.v85_qualification import validate_mutation_screen

DIMENSIONS = 8


def _write_source(path: pathlib.Path) -> None:
    rows = 2_000
    vectors = np.empty((rows, DIMENSIONS), dtype=np.float32)
    for row in range(rows):
        for dimension in range(DIMENSIONS):
            vectors[row, dimension] = np.float32(
                ((row * 17 + dimension * 29) % 1_009) / 1_009.0
            )
    table = pa.Table.from_arrays(
        [
            pa.array(np.arange(rows, dtype=np.uint64), type=pa.uint64()),
            pa.FixedSizeListArray.from_arrays(
                pa.array(vectors.reshape(-1), type=pa.float32()), DIMENSIONS
            ),
        ],
        schema=pa.schema(
            [
                pa.field("feature_row_id", pa.uint64(), nullable=False),
                pa.field(
                    "embedding",
                    pa.list_(
                        pa.field("item", pa.float32(), nullable=False), DIMENSIONS
                    ),
                    nullable=False,
                ),
            ]
        ),
    )
    pq.write_table(table, path)


class V85MutationScreenTests(unittest.TestCase):
    def test_real_arrow_trace_binds_replacements_tombstones_and_compaction(
        self,
    ) -> None:
        # Break caught: the qualification fabricates latest-write observations
        # without stale base rows, concrete Arrow directory locations, a new
        # physical replacement, or the compacted survivor/tombstone result.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "source.parquet"
            _write_source(source)
            receipt = run_mutation_screen(
                MutationScreenRequest(
                    source=source,
                    work=root / "work",
                    output=root / "receipt.json",
                    uri_prefix="s3://fixture/v85-mutation-screen",
                    base_rows=1_000,
                    delta_rows=1_000,
                    dimensions=DIMENSIONS,
                    page_rows=256,
                    router_cells=16,
                    base_runs=4,
                    delta_runs=10,
                    seed=85,
                )
            )

            self.assertRegex(receipt["before_generation_sha256"], r"^[0-9a-f]{64}$")
            self.assertNotEqual(
                receipt["before_generation_sha256"],
                receipt["after_generation_sha256"],
            )
            self.assertNotEqual(
                receipt["after_generation_sha256"],
                receipt["compacted_generation_sha256"],
            )
            self.assertEqual(len(receipt["cases"]), 1_000)
            self.assertEqual(
                validate_mutation_screen(receipt),
                {
                    "replacement_rows": 500,
                    "schema": "borsuk-v85-mutation-screen-summary-v1",
                    "tombstone_rows": 500,
                },
            )
            replacements = receipt["cases"][:500]
            tombstones = receipt["cases"][500:]
            self.assertTrue(
                all(case["compacted_sequence"] == 2 for case in replacements)
            )
            self.assertTrue(
                all(case["compacted_sequence"] is None for case in tombstones)
            )
            self.assertEqual(
                (root / "receipt.json").read_text().count("\n"), 1
            )


if __name__ == "__main__":
    unittest.main()
