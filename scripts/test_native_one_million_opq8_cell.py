"""Phase-separated 1M OPQ8 plan and historical control replay."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

from scripts.native_geometric_layout_screen import ArtifactIdentity
from scripts.native_hundred_thousand_opq8_router import (
    CENTROIDS,
    DIMENSIONS,
    SUBSPACES,
    Opq8Model,
    write_opq8_model,
)
from scripts.native_one_million_opq8_cell import (
    run_construct,
    run_evaluate,
    run_plan,
    run_validate,
)
from scripts.native_one_million_page_selector import (
    build_page_selector,
    read_page_selector,
)
from scripts.native_one_million_page_selector_evaluation import rank_page_groups
from scripts.native_one_million_range_selector_evaluation import plan_group_ranges
from scripts.test_native_one_million_group_selector import _fixture as source_fixture
from scripts.test_native_one_million_selector_evaluation import (
    _fixture as development_fixture,
)


def _identity(path: Path, role: str) -> ArtifactIdentity:
    body = path.read_bytes()
    return ArtifactIdentity(role, "s3://frozen/" + path.name, hashlib.sha256(body).hexdigest(), len(body))


class OneMillionOpq8CellTests(unittest.TestCase):
    def test_sealed_phase_order_and_control_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = source_fixture(root, base_rows=256)
            books = np.zeros((SUBSPACES, CENTROIDS, DIMENSIONS // SUBSPACES), dtype="<f4")
            books[:, 1, 0] = 1
            write_opq8_model(root / "model.bin", Opq8Model(
                np.zeros(DIMENSIONS, dtype="<f4"), np.eye(DIMENSIONS, dtype="<f4"), books,
            ))
            model = _identity(root / "model.bin", "model")
            control_dir = root / "control"
            build_page_selector(root, control_dir, source, batch_rows=17)
            control = {name: _identity(control_dir / filename, name) for name, filename in (
                ("centroids", "centroids.bin"), ("membership", "membership.bin"), ("seal", "seal.json"),
            )}
            with (
                patch("scripts.native_one_million_opq8_cell.SOURCE_IDENTITIES", source),
                patch("scripts.native_one_million_opq8_cell.MODEL_IDENTITY", model),
                patch("scripts.native_one_million_opq8_cell.CONTROL_SOURCE", control),
            ):
                run_construct(root, expected_rows=257, expected_groups=33)
                _, development = development_fixture(root)
                from scripts.native_one_million_selector_evaluation import _query_truth
                queries, ids = _query_truth(root / "queries.parquet", root / "truth.parquet", development, query_count=1)
                artifact = read_page_selector(control_dir, source)
                projected = tuple(dataclasses.replace(
                    group, code_bytes=4 + 4 * (group.end_page - group.first_page) + 96 * group.row_count,
                ) for group in artifact.groups)
                ranked = rank_page_groups(queries[0], artifact)
                selected, intervals, gets, used = plan_group_ranges(projected, ranked)
                owner = {int(i): int(g) for i, g in zip(artifact.membership_ids, artifact.membership_groups, strict=True)}
                chosen = set(selected)
                sample = {
                    "query_ordinal": 0, "selected_groups": list(selected),
                    "intervals": [list(item) for item in intervals],
                    "projected_code_gets": gets, "projected_code_bytes": used,
                    "hits_at_10": sum(owner[int(i)] in chosen for i in ids[0, :10]),
                    "hits_at_100": sum(owner[int(i)] in chosen for i in ids[0]),
                }
                historical = {"schema": "test-control", "samples": [sample]}
                with patch("scripts.native_one_million_opq8_cell.DEVELOPMENT_IDENTITIES", development):
                    with self.assertRaisesRegex(ValueError, "query-only"):
                        run_plan(root, root / "planning", query_count=1, expected_rows=257, expected_groups=33)
                truth_path = root / "truth.parquet"
                hidden_truth = root / "hidden-truth.parquet"
                truth_path.rename(hidden_truth)
                with patch("scripts.native_one_million_opq8_cell.DEVELOPMENT_IDENTITIES", development):
                    plans = run_plan(root, root / "planning", query_count=1, expected_rows=257, expected_groups=33)
                    self.assertEqual(len(plans["samples"]), 1)
                    self.assertTrue((root / "planning/plan-seal.json").is_file())
                    hidden_truth.rename(truth_path)
                    (root / "historical-evidence.json").write_bytes(
                        (json.dumps(historical, sort_keys=True, separators=(",", ":")) + "\n").encode(),
                    )
                    historical_id = _identity(root / "historical-evidence.json", "evidence")
                    with patch("scripts.native_one_million_opq8_cell.HISTORICAL_EVIDENCE", historical_id):
                        plan_path = root / "planning/plans.json"
                        plan_body = plan_path.read_bytes()
                        plan_path.write_bytes(plan_body + b"x")
                        with self.assertRaises(ValueError):
                            run_evaluate(root, root / "planning", root / "evaluation", query_count=1,
                                         expected_rows=257, expected_groups=33)
                        plan_path.write_bytes(plan_body)
                        result = run_evaluate(root, root / "planning", root / "evaluation", query_count=1,
                                              expected_rows=257, expected_groups=33)
                        self.assertEqual(result["metrics"]["control_gt100_hits"], sample["hits_at_100"])
                        self.assertEqual(result["metrics"]["control_total_bytes"], used)
                        self.assertEqual(result["metrics"]["control_total_groups"], len(selected))
                        validation = run_validate(root, root / "planning", root / "evaluation", root / "validation",
                                                  query_count=1, expected_rows=257, expected_groups=33)
                        self.assertEqual(validation["metrics"], result["metrics"])
                        changed = json.loads((root / "historical-evidence.json").read_bytes())
                        changed["samples"][0]["hits_at_100"] -= 1
                        (root / "historical-evidence.json").write_text(json.dumps(changed, sort_keys=True, separators=(",", ":")) + "\n")
                        changed_id = _identity(root / "historical-evidence.json", "evidence")
                        with patch("scripts.native_one_million_opq8_cell.HISTORICAL_EVIDENCE", changed_id):
                            with self.assertRaisesRegex(ValueError, "control replay"):
                                run_evaluate(root, root / "planning", root / "evaluation", query_count=1,
                                             expected_rows=257, expected_groups=33)


if __name__ == "__main__":
    unittest.main()
