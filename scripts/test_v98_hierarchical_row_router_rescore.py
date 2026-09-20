import copy
import dataclasses
import hashlib
import json
import pathlib
import tempfile
import unittest

from scripts.test_v98_hierarchical_row_router import (
    V98HierarchyAuthorityTests,
    V98ProducerTests,
)
from scripts.v97_row_width_screen import PageKey
from scripts.v98_hierarchical_row_router import (
    canonical_v98_result_bytes,
    evaluate_v98,
)
from scripts.v98_hierarchical_row_router_rescore import (
    canonical_v98_rescore_bytes,
    rescore_v98_result,
    validate_v98_result,
)


class V98IndependentReducerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)
        self.inputs = V98ProducerTests.inputs(
            dimensions=96,
            base_pages=128,
            delta_pages=9,
            query_count=2,
            truth_key=PageKey("base", 0),
            far_truth=False,
        )
        self.authority = V98ProducerTests.authority(self.inputs)
        self.config = V98HierarchyAuthorityTests.config()
        self.result = evaluate_v98(self.inputs, self.authority, self.config)
        self.body = canonical_v98_result_bytes(self.result)
        self.document = json.loads(self.body)
        self.path = self.root / "result.json"
        self.path.write_bytes(self.body)
        self.sha256 = hashlib.sha256(self.body).hexdigest()

    def _write(self, document: dict[str, object]) -> tuple[pathlib.Path, str]:
        body = (
            json.dumps(
                document,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
            + b"\n"
        )
        path = self.root / f"mutation-{hashlib.sha256(body).hexdigest()[:12]}.json"
        path.write_bytes(body)
        return path, hashlib.sha256(body).hexdigest()

    def _reject(self, mutate) -> None:
        document = copy.deepcopy(self.document)
        mutate(document)
        path, sha256 = self._write(document)
        with self.assertRaises(ValueError):
            validate_v98_result(
                path,
                sha256,
                expected_authority=self.authority,
                expected_config=self.config,
            )

    @staticmethod
    def _set(
        document: dict[str, object], path: tuple[object, ...], value: object
    ) -> None:
        cursor: object = document
        for key in path[:-1]:
            cursor = cursor[key]  # type: ignore[index]
        cursor[path[-1]] = value  # type: ignore[index]

    def test_recomputes_all_decisions_and_emits_canonical_summary(self) -> None:
        # Break caught: the reducer trusts producer aggregates/decisions or
        # emits a different bootstrap matrix for different arm comparisons.
        validated = validate_v98_result(
            self.path,
            self.sha256,
            expected_authority=self.authority,
            expected_config=self.config,
        )
        summary = rescore_v98_result(
            self.path,
            self.sha256,
            expected_authority=self.authority,
            expected_config=self.config,
        )

        self.assertEqual(validated.query_count, 2)
        self.assertEqual(
            summary.schema, "borsuk-v98-hierarchical-row-router-rescore-v1"
        )
        self.assertEqual(summary.result_sha256, self.sha256)
        self.assertEqual(summary.classification, "widths-evaluated")
        self.assertEqual(summary.bootstrap_seed, 7_216)
        self.assertEqual(summary.bootstrap_resamples, 10_000)
        self.assertEqual(
            summary.bootstrap_matrix_sha256,
            "6a3a3e3c3296dcbefaa2d93b6d02bc622023821d4c43c3b51f815f73b4450dfa",
        )
        self.assertEqual(summary.winner, "pq16x8")
        self.assertEqual(
            tuple(item.name for item in summary.arms),
            (
                "pq16x8",
                "pq24x8",
                "pq32x8",
                "pq32x4",
                "summary-only-pq16x8",
            ),
        )
        self.assertEqual(summary.arms[0].average_recall100_ppm, (0, 0))
        self.assertTrue(summary.arms[0].eligible)
        self.assertFalse(summary.arms[2].eligible)
        body = canonical_v98_rescore_bytes(summary)
        self.assertTrue(body.endswith(b"\n"))
        self.assertNotIn(b" ", body)
        self.assertEqual(
            json.loads(body),
            json.loads(json.dumps(dataclasses.asdict(summary))),
        )

    def test_accepts_both_valid_fail_fast_terminal_branches(self) -> None:
        # Break caught: hostile validation accidentally requires width evidence
        # after a registered containment or exact-ceiling stop.
        cases = (
            (
                V98ProducerTests.inputs(
                    dimensions=16,
                    base_pages=1_024,
                    delta_pages=1,
                    query_count=3,
                    truth_key=PageKey("delta", 0),
                    far_truth=False,
                ),
                "hierarchy-containment-rejected",
            ),
            (
                V98ProducerTests.inputs(
                    dimensions=16,
                    base_pages=1_024,
                    delta_pages=1,
                    query_count=1,
                    truth_key=PageKey("base", 1_000),
                    far_truth=True,
                ),
                "hierarchy-exact-ceiling-rejected",
            ),
        )
        for inputs, classification in cases:
            with self.subTest(classification=classification):
                authority = V98ProducerTests.authority(inputs)
                result = evaluate_v98(inputs, authority, self.config)
                body = canonical_v98_result_bytes(result)
                path = self.root / f"{classification}.json"
                path.write_bytes(body)
                summary = rescore_v98_result(
                    path,
                    hashlib.sha256(body).hexdigest(),
                    expected_authority=authority,
                    expected_config=self.config,
                )
                self.assertEqual(summary.classification, classification)
                self.assertEqual(summary.arms, ())
                self.assertIsNone(summary.winner)

    def test_rejects_schema_types_order_and_sample_evidence_drift(self) -> None:
        # Break caught: hostile JSON bypasses strict typed parsing or changes a
        # query's literal truth/page membership while retaining old recalls.
        mutations = (
            lambda value: value.pop("schema"),
            lambda value: value.__setitem__("unexpected", 1),
            lambda value: value.__setitem__("query_count", True),
            lambda value: self._set(
                value, ("containment_samples", 0, "query_ordinal"), 1
            ),
            lambda value: self._set(
                value, ("containment_samples", 0, "truth_ids"), [9_999]
            ),
            lambda value: self._set(
                value, ("containment_samples", 0, "truth_pages", 0, "ordinal"), 127
            ),
            lambda value: self._set(value, ("containment_samples", 0, "hit_ids"), []),
            lambda value: self._set(
                value, ("containment_samples", 0, "recall100_ppm"), 0
            ),
            lambda value: self._set(value, ("exact_samples", 0, "selected_pages"), []),
            lambda value: self._set(value, ("exact_samples", 0, "hit10_ids"), []),
            lambda value: self._set(value, ("exact_samples", 0, "gets"), 33),
            lambda value: self._set(
                value, ("exact_samples", 0, "bytes"), 16 * 1024**2 + 1
            ),
            lambda value: self._set(
                value, ("exact_samples", 0, "root_evaluations"), 65_537
            ),
            lambda value: self._set(
                value, ("exact_samples", 0, "page_evaluations"), 4_097
            ),
            lambda value: self._set(
                value, ("exact_samples", 0, "scanned_rows"), 262_145
            ),
            lambda value: self._set(
                value, ("arms", 0, "samples", 0, "recall10_ppm"), 0
            ),
            lambda value: self._set(
                value,
                ("arms", 0, "samples", 0, "selected_pages", 0, "object_role"),
                "delta",
            ),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                self._reject(mutation)

    def test_rejects_identity_aggregate_projection_and_decision_forgery(self) -> None:
        # Break caught: the reducer shallow-compares producer claims instead of
        # recomputing identities, aggregates, worksheet terms, CIs and winner.
        projection_fields = (
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
        )
        mutations = [
            lambda value: self._set(value, ("hierarchy", "ipc_sha256"), "0" * 64),
            lambda value: self._set(
                value, ("hierarchy", "page_summary_codes", "shape"), [1, 16]
            ),
            lambda value: self._set(
                value, ("arms", 0, "codebook_identity", "sha256"), "0" * 64
            ),
            lambda value: self._set(
                value, ("arms", 0, "codes_identity", "dtype"), "float32"
            ),
            lambda value: self._set(
                value, ("containment_aggregate", "p05_recall100_ppm"), 0
            ),
            lambda value: self._set(
                value, ("exact_aggregate", "average_recall100_ppm"), 0
            ),
            lambda value: self._set(
                value, ("arms", 0, "aggregate", "quality_gate_passed"), False
            ),
            lambda value: value.__setitem__("bootstrap_seed", 7_217),
            lambda value: value.__setitem__("bootstrap_resamples", 9_999),
            lambda value: value.__setitem__("bootstrap_matrix_sha256", "0" * 64),
            lambda value: self._set(
                value, ("paired_intervals", 0, "average_recall100_ppm"), [-1, 0]
            ),
            lambda value: self._set(value, ("eligibility", 0, "eligible"), False),
            lambda value: value.__setitem__("winner", "pq32x4"),
            lambda value: value.__setitem__(
                "classification", "hierarchy-exact-ceiling-rejected"
            ),
        ]
        for field in projection_fields:
            mutations.append(
                lambda value, field=field: self._set(
                    value,
                    ("arms", 0, "projection", field),
                    value["arms"][0]["projection"][field] + 1,
                )
            )
        mutations.extend(
            (
                lambda value: self._set(
                    value, ("arms", 0, "projection", "eligible"), False
                ),
                lambda value: self._set(
                    value, ("arms", 0, "projection", "base", "row_codes_bytes"), 1
                ),
                lambda value: self._set(
                    value, ("arms", 0, "projection", "base", "eligible"), False
                ),
            )
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                self._reject(mutation)


if __name__ == "__main__":
    unittest.main()
