import copy
import hashlib
import json
import unittest
from dataclasses import asdict

from scripts.v32_global_serving import (
    GlobalPageIdentity,
    GlobalQueryExpectation,
    validate_global_serving_batch,
)


class GlobalReplayAuthorityTests(unittest.TestCase):
    def fixture(self):
        import pyarrow as pa
        import pyarrow.parquet as pq

        from scripts.v32_global_serving import GlobalReplayRegistration

        schema = pa.schema(
            [
                pa.field("page_ordinal", pa.uint32(), nullable=False),
                pa.field("sha256", pa.binary(32), nullable=False),
                pa.field("encoded_bytes", pa.uint32(), nullable=False),
                pa.field("row_count", pa.uint16(), nullable=False),
            ]
        )
        table = pa.Table.from_arrays(
            [
                pa.array(range(16), type=pa.uint32()),
                pa.array(
                    [bytes.fromhex(f"{i + 1:064x}") for i in range(16)],
                    type=pa.binary(32),
                ),
                pa.array([100] * 16, type=pa.uint32()),
                pa.array([2] * 16, type=pa.uint16()),
            ],
            schema=schema,
        )
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        locations = sink.getvalue().to_pybytes()
        manifest = {
            "schema_version": 3,
            "page_key_suffix": ".arrow",
            "layout": {"source_rows": 32, "page_rows": 480},
            "routing": {"candidate_depth": 12288, "page_count": 16, "root_beam": 8},
            "serving": {
                "page_locations": {
                    "role": "v32-page-locations-parquet",
                    "file": "page-locations.parquet",
                    "encoded_bytes": len(locations),
                    "sha256": hashlib.sha256(locations).hexdigest(),
                }
            },
        }
        manifest_raw = self.encode(manifest)
        terminal = {
            "schema_version": 7,
            "claim_eligible": False,
            "routing_scope": "global",
            "layout_algorithm": "v32-global-balanced-cosine-v1",
            "global_leaf_limit": 768,
            "root_beam": 8,
            "leaf_beam": 256,
            "source_rows": 32,
            "query_start": 64,
            "query_count": 32,
            "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
            "query_sha256": "a" * 64,
            "truth_sha256": "b" * 64,
            "truth_receipt_sha256": "c" * 64,
            "control": {
                "queries": [
                    {
                        "query_ordinal": q,
                        "page_selections": {
                            "first_distinct": {
                                "pages": [
                                    {
                                        "ordinal": i,
                                        "sha256": f"{i + 1:064x}",
                                        "encoded_bytes": 100,
                                    }
                                    for i in range(16)
                                ],
                                "selected_page_bytes": 1600,
                            }
                        },
                    }
                    for q in range(64, 96)
                ]
            },
            "virtual_geometric": {
                "queries": [
                    {
                        "query_ordinal": q,
                        "candidate_replay_sha256": f"{q + 100:064x}",
                        "selected_pages": [999],
                    }
                    for q in range(64, 96)
                ]
            },
        }
        terminal_raw = self.encode(terminal)
        registration = GlobalReplayRegistration(
            hashlib.sha256(terminal_raw).hexdigest(),
            len(terminal_raw),
            hashlib.sha256(manifest_raw).hexdigest(),
            len(manifest_raw),
            "a" * 64,
            "b" * 64,
            "c" * 64,
            32,
            64,
        )
        return terminal_raw, manifest_raw, locations, registration

    @staticmethod
    def encode(value):
        return json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"

    def load(self, parts):
        from scripts.v32_global_serving import load_global_replay_authority

        return load_global_replay_authority(*parts)

    def test_authenticated_original_pages_not_virtual_pages(self):
        result = self.load(self.fixture())
        self.assertEqual(result.source_rows, 32)
        self.assertEqual(result.query_start, 64)
        self.assertEqual(len(result.expected), 32)
        self.assertEqual(result.expected[0].page_ordinals, tuple(range(16)))
        self.assertEqual(result.expected[0].candidate_replay_sha256, f"{164:064x}")
        self.assertEqual(sum(p.primary_rows for p in result.pages), 32)

    def test_registered_bytes_are_authenticated_before_projection(self):
        parts = self.fixture()
        for index in range(3):
            with self.subTest(role=index):
                changed = list(parts)
                changed[index] += b" "
                with self.assertRaises(ValueError):
                    self.load(changed)

    def test_query_pairing_and_original_page_bindings(self):
        from dataclasses import replace

        parts = self.fixture()
        for mutate in [
            lambda t: t.update(query_sha256="d" * 64),
            lambda t: t["control"]["queries"].reverse(),
            lambda t: t["virtual_geometric"]["queries"].reverse(),
            lambda t: t["control"]["queries"][0]["page_selections"]["first_distinct"][
                "pages"
            ][0].update(sha256="d" * 64),
            lambda t: t["control"]["queries"][0]["page_selections"][
                "first_distinct"
            ].update(selected_page_bytes=1599),
        ]:
            t = json.loads(parts[0])
            mutate(t)
            raw = self.encode(t)
            # Re-root only terminal digest/length to reach internal cross-bind.
            registered = replace(
                parts[3],
                terminal_sha256=hashlib.sha256(raw).hexdigest(),
                terminal_bytes=len(raw),
            )
            with self.assertRaises(ValueError):
                self.load((raw, parts[1], parts[2], registered))

    def test_page_schema_and_coverage_after_minimal_hash_cascade(self):
        from dataclasses import replace

        import pyarrow as pa
        import pyarrow.parquet as pq

        parts = self.fixture()
        original = pq.read_table(pa.BufferReader(parts[2]))
        changes = [
            (
                "nullable",
                original.cast(
                    pa.schema(
                        [
                            pa.field(f.name, f.type, nullable=True)
                            for f in original.schema
                        ]
                    )
                ),
            ),
            ("missing", original.slice(0, 15)),
            ("order", original.take(pa.array(list(reversed(range(16)))))),
            (
                "coverage",
                original.set_column(
                    3,
                    original.schema.field(3),
                    pa.array([3] + [2] * 15, type=pa.uint16()),
                ),
            ),
        ]
        for name, table in changes:
            with self.subTest(name=name):
                sink = pa.BufferOutputStream()
                pq.write_table(table, sink)
                pages = sink.getvalue().to_pybytes()
                manifest = json.loads(parts[1])
                manifest["serving"]["page_locations"].update(
                    sha256=hashlib.sha256(pages).hexdigest(), encoded_bytes=len(pages)
                )
                manifest_raw = self.encode(manifest)
                terminal = json.loads(parts[0])
                terminal["manifest_sha256"] = hashlib.sha256(manifest_raw).hexdigest()
                terminal_raw = self.encode(terminal)
                # Cascade only page -> manifest -> terminal byte bindings.
                registered = replace(
                    parts[3],
                    manifest_sha256=hashlib.sha256(manifest_raw).hexdigest(),
                    manifest_bytes=len(manifest_raw),
                    terminal_sha256=hashlib.sha256(terminal_raw).hexdigest(),
                    terminal_bytes=len(terminal_raw),
                )
                with self.assertRaises(ValueError):
                    self.load((terminal_raw, manifest_raw, pages, registered))


class GlobalServingTests(unittest.TestCase):
    def test_ladder_projection_pins_bytes_bindings_and_nested_actual_pages(self):
        # Break: serving expectations are derived from measured output, or
        # malformed/wrong-cohort ladder evidence silently becomes authority.
        from scripts.v32_global_serving import (
            GlobalReplayRegistration,
            load_page_ladder_serving_authority,
        )

        _, expected, pages = self.fixture(page_count=64)
        queries = []
        for query in expected:
            queries.append(
                dict(
                    query_ordinal=query.query_ordinal,
                    candidate_replay_sha256=query.candidate_replay_sha256,
                    current={},
                    cells=[
                        dict(
                            requested_pages=n,
                            selected_page_count=n,
                            selected_page_bytes=n * 100,
                            selected_pages=[asdict(p) for p in pages[:n]],
                            contained_truth_count=10,
                            containment_ppm=1000000,
                        )
                        for n in (16, 32, 64)
                    ],
                )
            )
        terminal = dict(
            schema="borsuk-v32-page-budget-ladder-v1",
            status="complete",
            metric="truth-page-containment-not-reranked-recall",
            claim_eligible=False,
            page_body_reads=0,
            source_rows=1000000,
            query_start=64,
            query_count=32,
            manifest_sha256="a" * 64,
            query_sha256="b" * 64,
            truth_sha256="c" * 64,
            truth_receipt_sha256="d" * 64,
            diagnostic=dict(
                schema_version=11,
                claim_eligible=False,
                page_body_reads=0,
                query_start=64,
                queries=queries,
                resources={},
            ),
        )

        def encode(value):
            return (
                json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
                + b"\n"
            )

        def registered(raw):
            return GlobalReplayRegistration(
                hashlib.sha256(raw).hexdigest(),
                len(raw),
                "a" * 64,
                3642,
                "b" * 64,
                "c" * 64,
                "d" * 64,
                1000000,
                64,
            )

        raw = encode(terminal)
        original = registered(raw)
        for cap in (16, 64):
            result = load_page_ladder_serving_authority(raw, original, cap)
            self.assertEqual(result.expected[0].page_ordinals, tuple(range(cap)))
            self.assertEqual(
                result.expected[0].candidate_replay_sha256,
                expected[0].candidate_replay_sha256,
            )
            self.assertEqual(result.pages, pages)
        with self.assertRaises(ValueError):
            load_page_ladder_serving_authority(raw, original, 32)
        for mutation in range(8):
            value = copy.deepcopy(terminal)
            if mutation == 0:
                value["query_sha256"] = "e" * 64
            elif mutation == 1:
                value["query_start"] = 65
            elif mutation == 2:
                value["diagnostic"]["queries"][0]["candidate_replay_sha256"] = "bad"
            elif mutation == 3:
                value["diagnostic"]["queries"][0]["cells"][2][
                    "selected_page_bytes"
                ] += 1
            elif mutation == 4:
                value["diagnostic"]["queries"][0]["cells"][2]["selected_pages"][0][
                    "sha256"
                ] = "e" * 64
            elif mutation == 5:
                value["diagnostic"]["queries"][0]["cells"][1][
                    "selected_pages"
                ].reverse()
            elif mutation == 6:
                value["diagnostic"]["queries"].pop()
            else:
                value["page_body_reads"] = True
            changed = encode(value)
            # Exact-byte authority rejects drift first; re-root just the terminal
            # digest to reach the separate binding/shape/relational gates.
            with self.subTest(mutation=mutation):
                with self.assertRaises(ValueError):
                    load_page_ladder_serving_authority(changed, original, 64)
                with self.assertRaises(ValueError):
                    load_page_ladder_serving_authority(changed, registered(changed), 64)

    def test_serving_page_budget_binds_reference_capture_and_actual_64(self):
        # Break: accepting stale schema3 or confusing the reference16 capture
        # with actual64 reads lets the experiment understate cost or mix arms.
        value, expected, pages = self.fixture(page_count=64)
        self.validate(value, expected, pages)
        for mutation in range(5):
            bad = copy.deepcopy(value)
            if mutation == 0:
                bad["configuration"]["capture_page_count"] = 64
            elif mutation == 1:
                bad["results"][0]["work"]["get_count"] = 16
            elif mutation == 2:
                bad["results"][1]["configuration"] = dict(
                    bad["configuration"], page_count=16
                )
            elif mutation == 3:
                bad["schema_version"] = 3
            else:
                bad["results"][0]["work"]["routing"].update(
                    candidates_retained=16, codes_scanned=16
                )
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.validate(bad, expected, pages)

    def test_serving_page_budget_rejects_oversized_individual_page(self):
        # A coherent registry/receipt rewrite must not turn the aggregate
        # wave allowance into an allowance for one oversized physical page.
        value, expected, pages = self.fixture(page_count=64)
        self.validate(value, expected, pages)
        first = pages[0]
        pages = (
            GlobalPageIdentity(first.ordinal, first.sha256, 1000000, 2, 0),
        ) + pages[1:]
        for row in value["results"]:
            row["requested_pages"][0]["encoded_bytes"] = 1000000
            row["work"]["encoded_bytes"] = 1000000 + 63 * 100
        with self.assertRaisesRegex(ValueError, "page values"):
            self.validate(value, expected, pages)

    def test_summary_preserves_failed_recall_and_empirical_latency(self):
        from scripts.v32_global_serving import summarize_global_serving_batch

        value, expected, pages = self.fixture()
        value["results"][0]["matches"][0]["source_ordinal"] = 10
        value["results"][0]["timing"]["elapsed_ns"] = 1000
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        result = summarize_global_serving_batch(
            raw,
            expected=expected,
            pages=pages,
            source_rows=1000000,
            truth=tuple(tuple(range(10)) for _ in range(32)),
        )
        self.assertEqual(result["status"], "complete")
        self.assertFalse(result["claim_eligible"])
        self.assertFalse(result["quality_passed"])
        self.assertEqual(result["aggregate_recall_ppm"], 996875)
        self.assertEqual(result["minimum_recall_ppm"], 900000)
        self.assertEqual(result["perfect_queries"], 31)
        self.assertEqual(
            result["timing"]["elapsed_ns"],
            {
                "sample_count": 32,
                "p50": 100,
                "p95": 100,
                "maximum": 1000,
                "total": 4100,
            },
        )
        self.assertEqual(result["logical_page_reads"], 512)
        self.assertEqual(result["encoded_bytes"], 51200)
        self.assertIsNone(result["transport_attempts"])
        self.assertEqual(result["batch_sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(result["rows"], value["results"])

    def test_summary_requires_concrete_unique_truth(self):
        from scripts.v32_global_serving import summarize_global_serving_batch

        value, expected, pages = self.fixture()
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        valid = tuple(tuple(range(10)) for _ in range(32))
        self.assertTrue(
            summarize_global_serving_batch(
                raw, expected=expected, pages=pages, source_rows=1000000, truth=valid
            )["quality_passed"]
        )
        for truth in (
            valid[:-1],
            list(valid),
            ((False,) + tuple(range(1, 10)),) + valid[1:],
            ((0,) * 10,) + valid[1:],
            ((1000000,) + tuple(range(1, 10)),) + valid[1:],
        ):
            with self.assertRaises(ValueError):
                summarize_global_serving_batch(
                    raw,
                    expected=expected,
                    pages=pages,
                    source_rows=1000000,
                    truth=truth,
                )

    def fixture(self, page_count=16):
        config = {
            "global_leaf_limit": 768,
            "scan_budget": 262144,
            "candidate_depth": 12288,
            "capture_page_count": 16,
            "page_count": page_count,
            "k": 10,
        }
        pages = tuple(
            GlobalPageIdentity(i, f"{i + 1:064x}", 100, 2, 0) for i in range(page_count)
        )
        expected = tuple(
            GlobalQueryExpectation(i, f"{i + 100:064x}", tuple(range(page_count)))
            for i in range(64, 96)
        )
        rows = []
        for query in expected:
            rows.append(
                {
                    "schema_version": 4,
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
                        "decoded_rows": page_count * 2,
                        "unique_rows": page_count * 2,
                        "encoded_bytes": page_count * 100,
                        "get_count": page_count,
                        "routing": {
                            "roots_scored": 1,
                            "leaves_eligible": 2,
                            "leaves_scanned": 2,
                            "query_table_pairs_built": 2,
                            "peak_query_table_pairs_live": 1,
                            "codes_scanned": 64,
                            "candidates_retained": 64,
                            "pages_considered": page_count,
                            "selected_pages": page_count,
                        },
                    },
                }
            )
        return (
            {
                "schema_version": 4,
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
