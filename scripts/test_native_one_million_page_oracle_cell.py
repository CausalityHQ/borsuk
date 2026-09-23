"""Small full-path oracle and independent-map validation checks."""

from __future__ import annotations

import dataclasses
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

from scripts.native_one_million_group_selector import Group
from scripts.native_one_million_page_oracle_cell import (
    MAP_DTYPE,
    _projected,
    run_evaluate,
)
from scripts.native_one_million_range_selector_evaluation import plan_group_ranges
from scripts.validate_native_one_million_page_oracle import validate


class PageOracleCellTest(unittest.TestCase):
    def test_evaluate_validate_and_reject_corrupted_physical_map(self) -> None:
        groups = (
            Group("base", 0, 0, 1, 50, 10_008),
            Group("base", 1, 1, 2, 50, 10_008),
        )
        group_dicts = [dataclasses.asdict(group) for group in groups]
        selected, intervals, gets, used = plan_group_ranges(_projected(groups), [0, 1])
        plan = {
            "ranked_groups": [0, 1],
            "selected_groups": list(selected),
            "intervals": [list(item) for item in intervals],
            "projected_code_gets": gets,
            "projected_code_bytes": used,
        }
        plans = {
            "samples": [
                {"query_ordinal": ordinal, "candidate": plan, "control": plan}
                for ordinal in range(1000)
            ]
        }
        membership = np.empty(100, dtype=MAP_DTYPE)
        membership["id"] = np.arange(100)
        membership["page"] = np.repeat(np.arange(2), 50)
        page_groups = np.asarray([0, 1], dtype="<u4")
        page_bytes = np.asarray([1024, 1024], dtype="<u4")
        seal = {
            "rows": 100,
            "pages": 2,
            "groups": group_dicts,
            "page_order_sha256": "d" * 64,
        }
        truth = np.tile(np.arange(100, dtype=np.int64), (1000, 1))
        source = {"groups": group_dicts}
        expected_hits = {"candidate": (100_000, 10_000), "control": (100_000, 10_000)}
        id_to_page = {row_id: row_id // 50 for row_id in range(100)}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "seal.json").write_text("{}\n")
            (root / "generation.json").write_text(
                json.dumps({"runs": [{"pages": [{"bytes": 1024}, {"bytes": 1024}]}]})
            )
            with (
                patch(
                    "scripts.native_one_million_page_oracle_cell._prior_authority",
                    return_value=(source, plans),
                ),
                patch(
                    "scripts.native_one_million_page_oracle_cell._read_map",
                    return_value=(seal, groups, membership, page_groups, page_bytes),
                ),
                patch(
                    "scripts.native_one_million_page_oracle_cell._query_truth",
                    return_value=(None, truth),
                ),
            ):
                result = run_evaluate(root, root, expected_group_hits=expected_hits)
            self.assertEqual(
                result["decision"], "page-count-ceiling-advance-source-scores"
            )
            with (
                patch(
                    "scripts.validate_native_one_million_page_oracle._prior_authority",
                    return_value=(source, plans),
                ),
                patch(
                    "scripts.validate_native_one_million_page_oracle._read_map",
                    return_value=(seal, groups, membership, page_groups, page_bytes),
                ),
                patch(
                    "scripts.validate_native_one_million_page_oracle._query_truth",
                    return_value=(None, truth),
                ),
                patch(
                    "scripts.validate_native_one_million_page_oracle._authenticate_object"
                ),
                patch(
                    "scripts.validate_native_one_million_page_oracle._page_membership",
                    return_value=(groups, id_to_page, page_groups, "d" * 64),
                ),
            ):
                validated = validate(
                    root,
                    root,
                    expected_rows=100,
                    expected_pages=2,
                    expected_group_hits=expected_hits,
                )
            self.assertEqual(
                validated["metrics"]["candidate"]["oracle_gt100_hits"], 100_000
            )
            corrupt = membership.copy()
            corrupt["page"][0] = 1
            with (
                patch(
                    "scripts.validate_native_one_million_page_oracle._prior_authority",
                    return_value=(source, plans),
                ),
                patch(
                    "scripts.validate_native_one_million_page_oracle._read_map",
                    return_value=(seal, groups, corrupt, page_groups, page_bytes),
                ),
                patch(
                    "scripts.validate_native_one_million_page_oracle._authenticate_object"
                ),
                patch(
                    "scripts.validate_native_one_million_page_oracle._page_membership",
                    return_value=(groups, id_to_page, page_groups, "d" * 64),
                ),
                self.assertRaisesRegex(ValueError, "physical map validation differs"),
            ):
                validate(
                    root,
                    root,
                    expected_rows=100,
                    expected_pages=2,
                    expected_group_hits=expected_hits,
                )


if __name__ == "__main__":
    unittest.main()
