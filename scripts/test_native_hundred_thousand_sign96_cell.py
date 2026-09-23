"""100k sign96 construct/plan phase separation on a small physical fixture."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np

from scripts.native_hundred_thousand_sign96_cell import (
    run_construct,
    run_evaluate,
    run_plan,
)
from scripts.native_page_microcluster_cell import FROZEN_INPUTS
from scripts.native_row_score_code_artifacts import _physical_order_sha256
from scripts.validate_native_hundred_thousand_sign96_cell import validate_closed


class Sign96CellTests(unittest.TestCase):
    def test_construct_then_plan_without_truth(self) -> None:
        rng = np.random.default_rng(40)
        ids = tuple(index.to_bytes(16, "little") for index in range(100))
        vectors = rng.standard_normal((100, 768)).astype(np.float32) / 10
        counts = (1,) * 100
        ordinals = tuple(int(value) for value in rng.permutation(100))
        membership = tuple(
            SimpleNamespace(stable_id=ids[source], page_ordinal=page)
            for page, source in enumerate(ordinals)
        )
        prior = {
            "page_row_counts": list(counts),
            "physical_order_sha256": _physical_order_sha256(ordinals),
        }
        query = vectors[:1]
        fake_router = SimpleNamespace(
            pages=tuple(SimpleNamespace(encoded_page_bytes=10) for _ in range(100))
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (
                patch("scripts.native_hundred_thousand_sign96_cell._read_inputs", return_value=(ids, vectors, membership)),
                patch("scripts.native_hundred_thousand_sign96_cell._read_prior_code_seal", return_value=prior),
                patch("scripts.native_hundred_thousand_sign96_cell._physical_order", return_value=(ordinals, counts, bytes.fromhex(FROZEN_INPUTS.layout.source.sha256))),
            ):
                run_construct(root, expected_rows=100, expected_pages=100)
                source_seal = json.loads((root / "sign96-source-seal.json").read_bytes())
                self.assertEqual(source_seal["rows"], 100)
                self.assertTrue((root / "sign96" / "groups.bin").is_file())
                with (
                    patch("scripts.native_hundred_thousand_sign96_cell._read_geometric_queries", return_value=query),
                    patch("scripts.native_hundred_thousand_sign96_cell._read_plans", return_value=[{"query_ordinal": 0, "selected_groups": [0, 1]}]),
                    patch("scripts.native_hundred_thousand_sign96_cell.read_geometric_router_parquet", return_value=fake_router),
                ):
                    plans = run_plan(root, query_count=1, expected_rows=100, expected_pages=100,
                                     maximum_gets=1, maximum_bytes=10)
                self.assertEqual(len(plans["samples"]), 1)
                self.assertEqual(set(plans["samples"][0]), {
                    "query_ordinal", "selected_groups", "projected_code_gets",
                    "projected_code_bytes", "score_error", "sign96", "source"
                })
                self.assertGreaterEqual(plans["samples"][0]["score_error"]["absolute_p99"], 0)
                (root / "truth.parquet").write_bytes(b"forbidden")
                with self.assertRaisesRegex(ValueError, "truth-free"):
                    run_plan(root, query_count=1, expected_rows=100, expected_pages=100)
                with (
                    patch("scripts.native_hundred_thousand_sign96_cell._read_queries_truth", return_value=(query, (ids,))),
                    patch("scripts.native_hundred_thousand_sign96_cell._read_plans", return_value=[{"query_ordinal": 0, "selected_groups": [0, 1]}]),
                    patch("scripts.native_hundred_thousand_sign96_cell.read_geometric_router_parquet", return_value=fake_router),
                ):
                    result = run_evaluate(
                        root, root / "evaluation", query_count=1,
                        expected_rows=100, expected_pages=100,
                    )
                self.assertTrue(result["resource_gate_pending"])
                self.assertEqual(result["metrics"]["sign96"]["gt100_hits"], 1)
                with (
                    patch("scripts.validate_native_hundred_thousand_sign96_cell._read_queries_truth", return_value=(query, (ids,))),
                    patch("scripts.validate_native_hundred_thousand_sign96_cell._read_geometric_queries", return_value=query),
                    patch("scripts.validate_native_hundred_thousand_sign96_cell.read_geometric_router_parquet", return_value=fake_router),
                    patch("scripts.native_hundred_thousand_sign96_cell._read_plans", return_value=[{"query_ordinal": 0, "selected_groups": [0, 1]}]),
                ):
                    validated = validate_closed(
                        root, root / "evaluation", query_count=1,
                        expected_rows=100, expected_pages=100,
                        maximum_gets=1, maximum_bytes=10,
                    )
                self.assertTrue(validated["valid"])
                evidence_path = root / "evaluation" / "sign96-evidence.json"
                evidence_path.write_bytes(evidence_path.read_bytes() + b"x")
                with (
                    patch("scripts.validate_native_hundred_thousand_sign96_cell._read_queries_truth", return_value=(query, (ids,))),
                    patch("scripts.validate_native_hundred_thousand_sign96_cell._read_geometric_queries", return_value=query),
                    patch("scripts.validate_native_hundred_thousand_sign96_cell.read_geometric_router_parquet", return_value=fake_router),
                    patch("scripts.native_hundred_thousand_sign96_cell._read_plans", return_value=[{"query_ordinal": 0, "selected_groups": [0, 1]}]),
                ):
                    with self.assertRaises(ValueError):
                        validate_closed(
                            root, root / "evaluation", query_count=1,
                            expected_rows=100, expected_pages=100,
                            maximum_gets=1, maximum_bytes=10,
                        )


if __name__ == "__main__":
    unittest.main()
