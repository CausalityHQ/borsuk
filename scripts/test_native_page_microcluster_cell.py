"""Small end-to-end fixture for the immutable microcluster cell program."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.native_geometric_layout_screen import (
    ArtifactIdentity,
    EvaluationLimits,
    LayoutAuthority,
    LayoutMethod,
    MembershipRow,
    write_membership_parquet,
)
from scripts.native_page_microcluster_cell import (
    CellInputs,
    construct_cell,
    evaluate_cell,
    validate_cell,
)


def _identity(path: Path, role: str) -> ArtifactIdentity:
    body = path.read_bytes()
    return ArtifactIdentity(
        role, path.resolve().as_uri(), hashlib.sha256(body).hexdigest(), len(body)
    )


class PageMicroclusterCellTests(unittest.TestCase):
    def test_local_cell_replays_and_rejects_false_result_aggregate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            values = np.asarray(
                [
                    float(value) if value < 50 else float(value + 50)
                    for value in range(100)
                ],
                dtype=np.float32,
            )
            source = pa.table(
                {
                    "feature_row_id": pa.array(range(100), type=pa.int64()),
                    "embedding": pa.FixedSizeListArray.from_arrays(
                        pa.array(values, type=pa.float32()), 1
                    ),
                }
            )
            pq.write_table(source, root / "source.parquet")
            source_id = _identity(root / "source.parquet", "source")
            authority = LayoutAuthority(
                schema="borsuk-native-geometric-layout-authority-v1",
                source=source_id,
                rows=100,
                dimensions=1,
                metric="l2",
                seed=20260921,
                method=LayoutMethod.TWO_MEANS_480K,
                maximum_page_rows=50,
                maximum_page_bytes=4096,
            )
            membership = tuple(
                MembershipRow(
                    stable_id=str(ordinal).encode(),
                    source_ordinal=ordinal,
                    page_ordinal=ordinal // 50,
                    in_page_ordinal=ordinal % 50,
                    page_rows=50,
                    encoded_page_bytes=80,
                    method=LayoutMethod.TWO_MEANS_480K,
                    source_sha256=bytes.fromhex(source_id.sha256),
                    seed=authority.seed,
                    construction_sha256=bytes.fromhex("22" * 32),
                )
                for ordinal in range(100)
            )
            member_id = dataclasses.replace(
                write_membership_parquet(
                    root / "membership.parquet", authority, membership
                ),
                role="geometric-membership",
            )
            query_schema = pa.schema(
                [
                    pa.field("query", pa.uint32(), nullable=False),
                    pa.field(
                        "vector",
                        pa.list_(pa.field("element", pa.float32(), nullable=False), 1),
                        nullable=False,
                    ),
                ]
            )
            pq.write_table(
                pa.Table.from_arrays(
                    [
                        pa.array([0], type=pa.uint32()),
                        pa.FixedSizeListArray.from_arrays(
                            pa.array([0.0], type=pa.float32()), 1
                        ),
                    ],
                    schema=query_schema,
                ),
                root / "queries.parquet",
            )
            truth_schema = pa.schema(
                [
                    pa.field("query", pa.uint32(), nullable=False),
                    pa.field(
                        "neighbors",
                        pa.list_(pa.field("element", pa.int64(), nullable=False), 100),
                        nullable=False,
                    ),
                ]
            )
            pq.write_table(
                pa.Table.from_arrays(
                    [
                        pa.array([0], type=pa.uint32()),
                        pa.FixedSizeListArray.from_arrays(
                            pa.array(range(100), type=pa.int64()), 100
                        ),
                    ],
                    schema=truth_schema,
                ),
                root / "truth.parquet",
            )
            inputs = CellInputs(
                layout=authority,
                membership=member_id,
                queries=_identity(root / "queries.parquet", "queries"),
                truth=_identity(root / "truth.parquet", "truth"),
                limits=EvaluationLimits(1, 80),
            )
            prefix = "s3://sealed-test/runs/a0001"
            construct_cell(root, prefix, inputs)
            out = root / "evaluation"
            out.mkdir()
            evaluate_cell(root, out, prefix, inputs)
            for name in ("evidence.parquet", "result.json"):
                (out / name).rename(root / name)
            result = validate_cell(root, prefix, "12" * 20, inputs)
            self.assertEqual(result["decision"], "killed")
            self.assertEqual(result["mean_recall_at_100_ppm"], 500_000)

            payload = json.loads((root / "result.json").read_text())
            payload["metrics"]["mean_recall_at_100_ppm"] = 990_000
            (root / "result.json").write_text(
                json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
            )
            with self.assertRaisesRegex(ValueError, "result"):
                validate_cell(root, prefix, "12" * 20, inputs)


if __name__ == "__main__":
    unittest.main()
