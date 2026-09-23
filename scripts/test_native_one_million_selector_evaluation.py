"""Fixed 1M group ranking, bytes and all-truth denominator checks."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.native_one_million_group_selector import (
    DIMENSIONS,
    Group,
    SelectorArtifact,
)
from scripts.native_one_million_selector_evaluation import (
    evaluate_selector,
    select_groups,
)
from scripts.test_native_one_million_group_selector import _identity


def _fixture(root: Path):
    groups = tuple(Group("base", index, index * 8, index * 8 + 8, 3, 636) for index in range(40))
    centroids = np.zeros((40, DIMENSIONS), dtype="<f2")
    centroids[:, 0] = np.arange(40, dtype=np.float16)
    artifact = SelectorArtifact(
        groups, centroids, np.arange(120, dtype=np.int64),
        np.repeat(np.arange(40, dtype=np.uint32), 3), {"schema": "fixture"},
    )
    query = np.zeros((1, DIMENSIONS), dtype=np.float32)
    queries = pa.Table.from_arrays([
        pa.array([0], type=pa.uint32()),
        pa.array([999], type=pa.uint64()),
        pa.FixedSizeListArray.from_arrays(pa.array(query.reshape(-1)),
            type=pa.list_(pa.field("item", pa.float32(), nullable=False), DIMENSIONS)),
    ], schema=pa.schema([
        pa.field("query_ordinal", pa.uint32(), nullable=False),
        pa.field("feature_row_id", pa.uint64(), nullable=False),
        pa.field("embedding", pa.list_(pa.field("item", pa.float32(), nullable=False), DIMENSIONS), nullable=False),
    ]))
    truth = pa.Table.from_arrays([
        pa.array([0] * 100, type=pa.uint32()),
        pa.array(range(100), type=pa.uint16()),
        pa.array(range(100), type=pa.uint64()),
        pa.array(np.arange(100, dtype=np.float64), type=pa.float64()),
    ], schema=pa.schema([
        pa.field("query_ordinal", pa.uint32(), nullable=False),
        pa.field("rank", pa.uint16(), nullable=False),
        pa.field("feature_row_id", pa.uint64(), nullable=False),
        pa.field("squared_distance", pa.float64(), nullable=False),
    ]))
    pq.write_table(queries, root / "queries.parquet")
    pq.write_table(truth, root / "truth.parquet")
    identities = {role: _identity(root / (role + ".parquet")) for role in ("queries", "truth")}
    return artifact, identities


class OneMillionSelectorEvaluationTests(unittest.TestCase):
    def test_stable_top32_and_projected_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            artifact, identities = _fixture(root)
            indices = select_groups(np.zeros(DIMENSIONS, dtype=np.float32), artifact)
            self.assertEqual(indices, tuple(range(32)))
            centroids = artifact.centroids.copy()
            centroids[0, 0] = 1
            tied = SelectorArtifact(artifact.groups, centroids, artifact.membership_ids, artifact.membership_groups, artifact.seal)
            self.assertEqual(select_groups(np.zeros(DIMENSIONS, dtype=np.float32), tied)[:2], (0, 1))
            result = evaluate_selector(artifact, root / "queries.parquet", root / "truth.parquet", root / "out", identities, query_count=1)
            self.assertEqual(result["metrics"]["mean_recall_at_100_ppm"], 960000)
            self.assertEqual(result["metrics"]["p05_recall_at_100_ppm"], 960000)
            self.assertEqual(result["metrics"]["mean_recall_at_10_ppm"], 1000000)
            self.assertEqual(result["metrics"]["max_projected_code_bytes"], 32 * 636)
            self.assertEqual(result["decision"], "group-centroid-selector-killed")
            evidence = json.loads((root / "out/evidence.json").read_bytes())
            self.assertEqual(evidence["samples"][0]["selected_groups"], list(range(32)))
            self.assertEqual(evidence["samples"][0]["hits_at_100"], 96)


if __name__ == "__main__":
    unittest.main()
