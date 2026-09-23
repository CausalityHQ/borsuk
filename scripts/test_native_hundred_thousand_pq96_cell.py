"""PQ96 rescue retains source/query/truth phase separation and replay."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np

from scripts.native_hundred_thousand_pq96_cell import (
    run_construct,
    run_evaluate,
    run_plan,
)
from scripts.native_row_score_code_artifacts import _physical_order_sha256
from scripts.validate_native_hundred_thousand_pq96_cell import validate_closed


class Pq96CellTest(unittest.TestCase):
    def test_source_only_construct_then_paired_truth_replay(self) -> None:
        rng = np.random.default_rng(41)
        ids = tuple(index.to_bytes(16, "little") for index in range(100))
        vectors = (rng.standard_normal((100, 768)) / 10).astype(np.float32)
        books = rng.standard_normal((96, 256, 8)).astype(np.float32)
        counts = (1,) * 100
        ordinals = tuple(int(value) for value in rng.permutation(100))
        order_sha = _physical_order_sha256(ordinals)
        membership = tuple(
            SimpleNamespace(stable_id=ids[source], page_ordinal=page)
            for page, source in enumerate(ordinals)
        )
        source = (ids, vectors, membership, ordinals, counts, order_sha)
        query = vectors[:1]
        historical = [{"query_ordinal": 0, "selected_groups": [0, 1]}]
        router = SimpleNamespace(
            pages=tuple(SimpleNamespace(encoded_page_bytes=10) for _ in range(100))
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "opq").mkdir()
            (root / "opq" / "plans.json").write_bytes(b"{}")
            with (
                patch("scripts.native_hundred_thousand_pq96_cell._source_authority", return_value=source),
                patch("scripts.native_hundred_thousand_pq96_cell.fit_source_pq96", return_value=books),
            ):
                run_construct(root, expected_rows=100, expected_pages=100)
            self.assertEqual(json.loads((root / "pq96-source-seal.json").read_bytes())["rows"], 100)
            self.assertTrue((root / "pq96" / "groups.bin").is_file())
            with (
                patch("scripts.native_hundred_thousand_pq96_cell._source_authority", return_value=source),
                patch("scripts.native_hundred_thousand_pq96_cell._read_geometric_queries", return_value=query),
                patch("scripts.native_hundred_thousand_pq96_cell._read_plans", return_value=historical),
                patch("scripts.native_hundred_thousand_pq96_cell.read_geometric_router_parquet", return_value=router),
            ):
                plans = run_plan(root, query_count=1, expected_rows=100, expected_pages=100,
                                 maximum_gets=1, maximum_bytes=10)
            self.assertEqual(len(plans["samples"]), 1)
            self.assertIn("pq96", plans["samples"][0])
            self.assertEqual(plans["samples"][0]["projected_code_gets"], 2)
            (root / "truth.parquet").write_bytes(b"later")
            with self.assertRaisesRegex(ValueError, "truth-free"):
                run_plan(root, query_count=1, expected_rows=100, expected_pages=100)
            with (
                patch("scripts.native_hundred_thousand_pq96_cell._source_authority", return_value=source),
                patch("scripts.native_hundred_thousand_pq96_cell._read_plans", return_value=historical),
                patch("scripts.native_hundred_thousand_pq96_cell._read_queries_truth", return_value=(query, (ids,))),
                patch("scripts.native_hundred_thousand_pq96_cell.read_geometric_router_parquet", return_value=router),
            ):
                result = run_evaluate(
                    root, root / "evaluation", query_count=1,
                    expected_rows=100, expected_pages=100,
                )
            self.assertTrue(result["resource_gate_pending"])
            self.assertEqual(result["metrics"]["source"]["gt100_hits"], 1)
            with (
                patch("scripts.validate_native_hundred_thousand_pq96_cell._source_authority", return_value=source),
                patch("scripts.native_hundred_thousand_pq96_cell._read_plans", return_value=historical),
                patch("scripts.validate_native_hundred_thousand_pq96_cell._read_queries_truth", return_value=(query, (ids,))),
                patch("scripts.validate_native_hundred_thousand_pq96_cell._read_geometric_queries", return_value=query),
                patch("scripts.validate_native_hundred_thousand_pq96_cell.read_geometric_router_parquet", return_value=router),
            ):
                validation = validate_closed(
                    root, root / "evaluation", query_count=1,
                    expected_rows=100, expected_pages=100,
                    maximum_gets=1, maximum_bytes=10,
                )
            self.assertTrue(validation["valid"])


if __name__ == "__main__":
    unittest.main()
