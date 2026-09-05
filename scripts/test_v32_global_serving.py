import copy
import json
import unittest
from dataclasses import asdict

from scripts.v32_global_serving import (
    GlobalPageIdentity,
    GlobalQueryExpectation,
    validate_global_serving_batch,
)


class GlobalServingTests(unittest.TestCase):
    def fixture(self):
        config = {
            "global_leaf_limit": 768,
            "scan_budget": 262144,
            "candidate_depth": 12288,
            "page_count": 16,
            "k": 10,
        }
        pages = tuple(
            GlobalPageIdentity(i, f"{i + 1:064x}", 100, 2, 0) for i in range(16)
        )
        expected = tuple(
            GlobalQueryExpectation(i, f"{i + 100:064x}", tuple(range(16)))
            for i in range(64, 96)
        )
        rows = []
        for query in expected:
            rows.append(
                {
                    "schema_version": 3,
                    "claim_eligible": False,
                    "routing_scope": "global",
                    "global_leaf_limit": 768,
                    "configuration": config,
                    "candidate_replay_sha256": query.candidate_replay_sha256,
                    "requested_pages": [asdict(page) for page in pages],
                    "matches": [
                        {"source_ordinal": i, "squared_distance": i / 10}
                        for i in range(10)
                    ],
                    "timing": {
                        "elapsed_ns": 100,
                        "process_cpu_ns": 50,
                        "peak_rss_bytes": 1000,
                        "routing_elapsed_ns": 10,
                        "page_read_elapsed_ns": 20,
                        "exact_rerank_elapsed_ns": 30,
                        "routing_cpu_ns": 5,
                        "page_read_cpu_ns": 10,
                        "exact_rerank_cpu_ns": 15,
                    },
                    "work": {
                        "decoded_rows": 32,
                        "unique_rows": 32,
                        "encoded_bytes": 1600,
                        "get_count": 16,
                        "routing": {
                            "roots_scored": 1,
                            "leaves_eligible": 2,
                            "leaves_scanned": 2,
                            "query_table_pairs_built": 2,
                            "peak_query_table_pairs_live": 1,
                            "codes_scanned": 64,
                            "candidates_retained": 64,
                            "pages_considered": 16,
                            "selected_pages": 16,
                        },
                    },
                }
            )
        return (
            {
                "schema_version": 3,
                "claim_eligible": False,
                "routing_scope": "global",
                "configuration": config,
                "results": rows,
            },
            expected,
            pages,
        )

    def validate(self, value, expected, pages):
        return validate_global_serving_batch(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n",
            expected=expected,
            pages=pages,
            source_rows=1000000,
        )

    def rejected(self, mutations):
        baseline, expected, pages = self.fixture()
        self.validate(baseline, expected, pages)
        for name, mutate in mutations:
            with self.subTest(name=name):
                value = copy.deepcopy(baseline)
                mutate(value)
                with self.assertRaises(ValueError):
                    self.validate(value, expected, pages)

    def test_coherent_batch_preserves_physical_evidence(self):
        value, expected, pages = self.fixture()
        actual = self.validate(value, expected, pages)
        self.assertEqual(actual, value)
        self.assertEqual(actual["results"][0]["work"]["encoded_bytes"], 1600)
        # Valid float lexemes are byte authority, not Python's rewritten spelling.
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        raw = raw.replace(b'"squared_distance":0.0', b'"squared_distance":0e0')
        self.assertEqual(
            validate_global_serving_batch(
                raw, expected=expected, pages=pages, source_rows=1000000
            ),
            value,
        )

    def test_schema_scope_and_configuration_drift(self):
        self.rejected(
            [
                ("mixed-root", lambda v: v["results"][3].update(schema_version=2)),
                ("claim", lambda v: v.update(claim_eligible=True)),
                ("scope", lambda v: v.update(routing_scope="root-gated")),
                ("extra", lambda v: v.update(extra=1)),
                ("count", lambda v: v["results"].pop()),
                ("budget", lambda v: v["configuration"].update(scan_budget=262145)),
                ("bool-schema", lambda v: v.update(schema_version=True)),
            ]
        )

    def test_registered_replay_and_page_authority_cannot_be_rerooted(self):
        self.rejected(
            [
                (
                    "valid-hash",
                    lambda v: v["results"][0].update(candidate_replay_sha256="f" * 64),
                ),
                (
                    "page-hash",
                    lambda v: v["results"][0]["requested_pages"][0].update(
                        sha256="f" * 64
                    ),
                ),
                ("row-order", lambda v: v["results"].reverse()),
                (
                    "missing-hash",
                    lambda v: v["results"][0].pop("candidate_replay_sha256"),
                ),
            ]
        )

    def test_pages_and_accounting_are_recomputed(self):
        self.rejected(
            [
                ("order", lambda v: v["results"][0]["requested_pages"].reverse()),
                (
                    "duplicate",
                    lambda v: v["results"][0]["requested_pages"].__setitem__(
                        1, v["results"][0]["requested_pages"][0]
                    ),
                ),
                ("bytes", lambda v: v["results"][0]["work"].update(encoded_bytes=1599)),
                ("rows", lambda v: v["results"][0]["work"].update(decoded_rows=31)),
                ("unique", lambda v: v["results"][0]["work"].update(unique_rows=31)),
                ("count", lambda v: v["results"][0]["work"].update(get_count=15)),
                (
                    "bool-page",
                    lambda v: v["results"][0]["requested_pages"][0].update(
                        ordinal=False
                    ),
                ),
            ]
        )

    def test_match_concrete_types_order_and_finiteness(self):
        self.rejected(
            [
                ("order", lambda v: v["results"][0]["matches"].reverse()),
                (
                    "duplicate",
                    lambda v: v["results"][0]["matches"][1].update(source_ordinal=0),
                ),
                (
                    "range",
                    lambda v: v["results"][0]["matches"][0].update(
                        source_ordinal=1000000
                    ),
                ),
                (
                    "nan",
                    lambda v: v["results"][0]["matches"][0].update(
                        squared_distance=float("nan")
                    ),
                ),
                (
                    "negative",
                    lambda v: v["results"][0]["matches"][0].update(squared_distance=-1),
                ),
                (
                    "unrepresentable",
                    lambda v: v["results"][0]["matches"][0].update(
                        squared_distance=10**400
                    ),
                ),
                (
                    "bool",
                    lambda v: v["results"][0]["matches"][0].update(
                        source_ordinal=False
                    ),
                ),
            ]
        )

    def test_timing_and_work_types_sums_and_ceilings(self):
        self.rejected(
            [
                (
                    "retained-underflow",
                    lambda v: v["results"][0]["work"]["routing"].update(
                        candidates_retained=63
                    ),
                ),
                (
                    "extra-page",
                    lambda v: v["results"][0]["work"]["routing"].update(
                        pages_considered=17
                    ),
                ),
                (
                    "no-roots",
                    lambda v: v["results"][0]["work"]["routing"].update(roots_scored=0),
                ),
                (
                    "phase",
                    lambda v: v["results"][0]["timing"].update(
                        page_read_elapsed_ns=100
                    ),
                ),
                (
                    "cpu",
                    lambda v: v["results"][0]["timing"].update(page_read_cpu_ns=50),
                ),
                (
                    "boolean",
                    lambda v: v["results"][0]["timing"].update(elapsed_ns=True),
                ),
                (
                    "scan",
                    lambda v: v["results"][0]["work"]["routing"].update(
                        codes_scanned=262145
                    ),
                ),
                (
                    "candidates",
                    lambda v: v["results"][0]["work"]["routing"].update(
                        candidates_retained=65
                    ),
                ),
                (
                    "leaves",
                    lambda v: v["results"][0]["work"]["routing"].update(
                        leaves_scanned=769
                    ),
                ),
                (
                    "tables",
                    lambda v: v["results"][0]["work"]["routing"].update(
                        peak_query_table_pairs_live=2
                    ),
                ),
            ]
        )
