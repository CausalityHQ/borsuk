"""Hostile reducer tests for V99 ranked-gap evidence."""

from __future__ import annotations

import copy
import hashlib
import json
import pathlib
import tempfile
import unittest

from scripts import test_v98_hierarchical_row_router as v98_test
from scripts import test_v99_ranked_gap_range_router as v99_test
from scripts.v97_row_width_screen import PageKey
from scripts.v99_ranked_gap_range_router import canonical_v99_result_bytes, evaluate_v99
from scripts.v99_ranked_gap_range_router_rescore import (
    canonical_v99_rescore_bytes,
    rescore_v99_result,
    validate_v99_result,
)


class V99RescoreTests(unittest.TestCase):
    def setUp(self) -> None:
        inputs = v98_test.V98ProducerTests.inputs(
            dimensions=96,
            base_pages=128,
            delta_pages=9,
            query_count=2,
            truth_key=PageKey("base", 0),
            far_truth=False,
        )
        self.inputs = v99_test.V99EvaluationTests.contiguous(inputs)
        self.authority = v98_test.V98ProducerTests.authority(self.inputs)
        self.config = v99_test.V99EvaluationTests.config()
        self.body = canonical_v99_result_bytes(
            evaluate_v99(self.inputs, self.authority, self.config)
        )

    def _write(self, root: pathlib.Path, value: object) -> tuple[pathlib.Path, str]:
        body = (
            json.dumps(
                value, allow_nan=False, separators=(",", ":"), sort_keys=True
            ).encode()
            + b"\n"
        )
        path = root / "result.json"
        path.write_bytes(body)
        return path, hashlib.sha256(body).hexdigest()

    def test_recomputes_canonical_range_evidence_and_decision(self) -> None:
        # Break caught: the reducer trusts producer ranges/aggregates or emits
        # a noncanonical receipt instead of recomputing the full decision.
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "result.json"
            path.write_bytes(self.body)
            digest = hashlib.sha256(self.body).hexdigest()
            validated = validate_v99_result(
                path,
                digest,
                expected_authority=self.authority,
                expected_config=self.config,
            )
            summary = rescore_v99_result(
                path,
                digest,
                expected_authority=self.authority,
                expected_config=self.config,
            )
        self.assertEqual(validated.query_count, 2)
        self.assertEqual(validated.classification, "widths-evaluated")
        self.assertEqual(summary.result_sha256, digest)
        self.assertEqual(len(summary.arms), 5)
        receipt = canonical_v99_rescore_bytes(summary)
        self.assertTrue(receipt.endswith(b"\n"))
        self.assertNotIn(b" ", receipt)
        self.assertEqual(
            json.loads(receipt)["schema"],
            "borsuk-v99-ranked-gap-range-router-rescore-v1",
        )

    def test_rejects_authority_range_sample_projection_and_decision_drift(self) -> None:
        # Break caught: a valid-looking digest reroot lets forged physical or
        # scientific evidence survive independent reduction.
        original = json.loads(self.body)
        mutations = {
            "authority": lambda value: value["authority"].__setitem__(
                "page_map_sha256", "d" * 64
            ),
            "directory": lambda value: value["page_directory"][0].__setitem__(
                "offset", 1
            ),
            "range bytes": lambda value: value["exact_samples"][0]["selected_ranges"][
                0
            ].__setitem__("bytes", 1),
            "page union": lambda value: value["exact_samples"][0].__setitem__(
                "selected_pages", value["exact_samples"][0]["selected_pages"][:-1]
            ),
            "sample recall": lambda value: value["exact_samples"][0].__setitem__(
                "recall100_ppm", 0
            ),
            "aggregate": lambda value: value["exact_aggregate"].__setitem__(
                "average_recall100_ppm", 0
            ),
            "projection": lambda value: value["arms"][0]["projection"].__setitem__(
                "total_bytes", 1
            ),
            "classification": lambda value: value.__setitem__(
                "classification", "range-exact-ceiling-rejected"
            ),
            "winner": lambda value: value.__setitem__("winner", "pq32x8"),
            "bootstrap": lambda value: value.__setitem__(
                "bootstrap_matrix_sha256", "e" * 64
            ),
            "config cap": lambda value: value["config"].__setitem__("maximum_gets", 31),
            "hierarchy IPC": lambda value: value["hierarchy"].__setitem__(
                "ipc_sha256", "z" * 64
            ),
            "range role": lambda value: value["exact_samples"][0]["selected_ranges"][
                0
            ].__setitem__("object_role", "delta"),
            "range first": lambda value: value["exact_samples"][0]["selected_ranges"][
                0
            ].__setitem__("first_page", 1),
            "range last": lambda value: value["exact_samples"][0]["selected_ranges"][
                0
            ].__setitem__("last_page", 0),
            "range offset": lambda value: value["exact_samples"][0]["selected_ranges"][
                0
            ].__setitem__("offset", 1),
            "hit ids": lambda value: value["exact_samples"][0].__setitem__(
                "hit_ids", []
            ),
            "hit10 ids": lambda value: value["exact_samples"][0].__setitem__(
                "hit10_ids", []
            ),
            "work": lambda value: value["exact_samples"][0].__setitem__(
                "scanned_rows", 1
            ),
            "paired interval": lambda value: value["paired_intervals"][0].__setitem__(
                "average_recall100_ppm", [1, 1]
            ),
            "eligibility": lambda value: value["eligibility"][0].__setitem__(
                "eligible", False
            ),
        }
        for role in original["authority"]["identities"]:
            mutations[f"authority identity {role}"] = lambda value, role=role: value[
                "authority"
            ]["identities"][role].__setitem__("sha256", "e" * 64)
        for identity in (
            "summary_books",
            "page_summary_codes",
            "root_summary_codes",
            "page_row_counts",
        ):
            mutations[f"hierarchy identity {identity}"] = (
                lambda value, identity=identity: value["hierarchy"][
                    identity
                ].__setitem__("sha256", "z" * 64)
            )
        for identity in ("codebook_identity", "codes_identity"):
            mutations[f"arm identity {identity}"] = lambda value, identity=identity: (
                value["arms"][0][identity].__setitem__("sha256", "z" * 64)
            )
        for field in (
            "root_groups",
            "page_summary_bytes",
            "root_summary_bytes",
            "page_to_root_bytes",
            "root_child_bytes",
            "row_code_offsets_bytes",
            "root_scores_workspace_bytes",
            "page_scores_workspace_bytes",
            "row_scores_workspace_bytes",
            "shortlist_workspace_bytes",
            "hierarchy_additional_bytes",
            "total_bytes",
            "budget_bytes",
        ):
            mutations[f"projection {field}"] = lambda value, field=field: value["arms"][
                0
            ]["projection"].__setitem__(field, 1)
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            for label, mutate in mutations.items():
                with self.subTest(label=label):
                    changed = copy.deepcopy(original)
                    mutate(changed)
                    path, digest = self._write(root, changed)
                    try:
                        validate_v99_result(
                            path,
                            digest,
                            expected_authority=self.authority,
                            expected_config=self.config,
                        )
                    except ValueError as error:
                        self.assertRegex(str(error), "V99")
                    else:
                        self.fail(f"mutation accepted: {label}")


if __name__ == "__main__":
    unittest.main()
