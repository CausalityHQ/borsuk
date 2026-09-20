import hashlib
import json
import pathlib
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from scripts.v97_row_width_rescore import validate_screen_result
from scripts.v97_row_width_screen import (
    PQ16X8,
    PQ24X8,
    PQ32X4,
    PQ32X8,
    SUMMARY_ONLY_PQ16X8,
    ObjectIdentity,
    PageKey,
    PqSpec,
    RoutedPage,
    ScreenAuthority,
    ScreenInputs,
    adc_scores,
    encode_pq,
    evaluate_selected_pages,
    evaluate_width_arms,
    fit_pq,
    load_screen_inputs,
    pack_pq4,
    page_block_means,
    page_map_sha256,
    parse_screen_args,
    project_resident_bytes_100m,
    run_screen,
    score_packed_pq4,
    screen_result_document,
    select_budgeted_pages,
    top_k_total_order,
    unpack_pq4,
)


class RowWidthContractTests(unittest.TestCase):
    def test_direct_script_defines_projection_before_invoking_main(self) -> None:
        # Break caught: imported tests pass, but direct paid execution calls
        # main before a helper used while serializing the completed result.
        source = pathlib.Path(__file__).with_name("v97_row_width_screen.py").read_text()
        self.assertLess(
            source.index("def project_resident_bytes_100m"),
            source.index('if __name__ == "__main__"'),
        )

    def test_cli_requires_all_six_registered_object_identities(self) -> None:
        # Break caught: the paid CLI accepts an unbound base/delta artifact or
        # a validation/holdout tuning input outside the registered dev cell.
        arguments = []
        for role in ("source", "queries", "truth", "generation", "base", "delta"):
            arguments.extend(
                [
                    f"--{role}",
                    f"{role}.bin",
                    f"--{role}-uri",
                    f"s3://fixture/{role}",
                    f"--{role}-sha256",
                    "1" * 64,
                    f"--{role}-bytes",
                    "1",
                ]
            )
        arguments.extend(
            [
                "--source-commit",
                "2" * 40,
                "--critique-result-sha256",
                "3" * 64,
                "--output",
                "result.json",
            ]
        )
        parsed = parse_screen_args(arguments)
        self.assertEqual(parsed.query_count, 1_000)
        self.assertEqual(parsed.dimensions, 768)
        self.assertFalse(hasattr(parsed, "validation"))
        with self.assertRaises(SystemExit):
            parse_screen_args(arguments[:-2])

    def test_pq32x4_packs_low_nibble_first_and_scores_like_the_literal_codes(
        self,
    ) -> None:
        # Break caught: the persisted nibble order and the scorer disagree, so
        # the quality screen measures a representation production cannot read.
        codes = np.asarray(
            [list(range(16)) + list(range(16))],
            dtype=np.uint8,
        )
        packed = pack_pq4(codes)

        self.assertEqual(packed.shape, (1, 16))
        self.assertEqual(packed[0, :4].tolist(), [0x10, 0x32, 0x54, 0x76])
        self.assertTrue(np.array_equal(unpack_pq4(packed, 32), codes))

        tables = np.asarray(
            [
                [100.0 * subspace + float(code) for code in range(16)]
                for subspace in range(32)
            ],
            dtype=np.float32,
        )
        self.assertEqual(score_packed_pq4(packed, tables).tolist(), [49_840.0])

    def test_pq32x4_rejects_values_shapes_and_nonfinite_tables(self) -> None:
        # Break caught: malformed packed evidence is truncated, broadcast, or
        # silently assigned a deterministic-looking scientific score.
        with self.assertRaisesRegex(ValueError, "4-bit PQ codes differ"):
            pack_pq4(np.asarray([[16] * 32], dtype=np.uint8))
        with self.assertRaisesRegex(ValueError, "4-bit PQ codes differ"):
            pack_pq4(np.asarray([[1] * 31], dtype=np.uint8))
        with self.assertRaisesRegex(ValueError, "packed 4-bit PQ shape differs"):
            unpack_pq4(np.zeros((1, 15), dtype=np.uint8), 32)
        tables = np.zeros((32, 16), dtype=np.float32)
        tables[0, 0] = np.nan
        with self.assertRaisesRegex(ValueError, "4-bit PQ score table differs"):
            score_packed_pq4(np.zeros((1, 16), dtype=np.uint8), tables)

    def test_resident_projection_recomputes_all_terms_and_budget(self) -> None:
        # Break caught: a wider arm is promoted by omitting summary codes, one
        # codebook, or the fixed mutation/runtime reserves from the 100M total.
        expected = {
            "summary-only-pq16x8": (1_260_816_672, True),
            "pq16x8": (2_861_603_104, True),
            "pq24x8": (3_661_603_104, False),
            "pq32x8": (4_461_603_104, False),
            "pq32x4": (2_860_865_824, True),
        }
        for spec in (SUMMARY_ONLY_PQ16X8, PQ16X8, PQ24X8, PQ32X8, PQ32X4):
            with self.subTest(arm=spec.name):
                projection = project_resident_bytes_100m(spec)
                self.assertEqual(
                    (projection.total_bytes, projection.eligible), expected[spec.name]
                )
                self.assertEqual(projection.rows, 100_000_000)
                self.assertEqual(projection.pages, 390_625)
                self.assertEqual(projection.summary_codes_bytes, 12_500_000)
                self.assertEqual(projection.summary_codebook_bytes, 786_432)
                self.assertEqual(projection.mutation_entries, 1_000_000)
                self.assertEqual(projection.mutation_directory_bytes, 96_000_000)
                self.assertEqual(projection.resident_delta_rows, 100_000)
                self.assertEqual(projection.resident_delta_bytes, 84_000_000)
                self.assertEqual(projection.page_directory_reserve_bytes, 134_217_728)
                self.assertEqual(projection.response_buffers_bytes, 268_435_456)
                self.assertEqual(projection.planner_workspace_bytes, 128_000_000)
                self.assertEqual(projection.runtime_reserve_bytes, 536_870_912)


class RowWidthEvaluationTests(unittest.TestCase):
    def test_loader_authenticates_generation_and_preserves_tier_page_order(self) -> None:
        # Break caught: the paid cell constructs its page map from source-row
        # order, trusts a manifest object identity, or collapses base/delta
        # page ordinals into one namespace.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            dimensions = 96
            source_ids = np.arange(260, dtype=np.int64)
            vectors = np.zeros((260, dimensions), dtype=np.float32)
            vectors[:, 0] = np.arange(260, dtype=np.float32)
            source = root / "source.parquet"
            queries = root / "queries.parquet"
            truth = root / "truth.parquet"
            generation = root / "generation.json"
            base = root / "base.arrow"
            delta = root / "delta.arrow"
            pq.write_table(
                pa.Table.from_arrays(
                    [
                        pa.array(source_ids, type=pa.uint64()),
                        pa.FixedSizeListArray.from_arrays(
                            pa.array(vectors.reshape(-1), type=pa.float32()),
                            dimensions,
                        ),
                    ],
                    schema=pa.schema(
                        [
                            pa.field("feature_row_id", pa.uint64(), nullable=False),
                            pa.field(
                                "embedding",
                                pa.list_(
                                    pa.field("item", pa.float32(), nullable=False),
                                    dimensions,
                                ),
                                nullable=False,
                            ),
                        ]
                    ),
                ),
                source,
            )
            pq.write_table(
                pa.Table.from_arrays(
                    [
                        pa.array([0, 1], type=pa.uint32()),
                        pa.array([10_000, 10_001], type=pa.uint64()),
                        pa.FixedSizeListArray.from_arrays(
                            pa.array(vectors[[0, 258]].reshape(-1), type=pa.float32()),
                            dimensions,
                        ),
                    ],
                    schema=pa.schema(
                        [
                            pa.field("query_ordinal", pa.uint32(), nullable=False),
                            pa.field("feature_row_id", pa.uint64(), nullable=False),
                            pa.field(
                                "embedding",
                                pa.list_(
                                    pa.field("item", pa.float32(), nullable=False),
                                    dimensions,
                                ),
                                nullable=False,
                            ),
                        ]
                    ),
                ),
                queries,
            )
            pq.write_table(
                pa.Table.from_arrays(
                    [
                        pa.array([0, 0, 1, 1], type=pa.uint32()),
                        pa.array([0, 1, 0, 1], type=pa.uint16()),
                        pa.array([0, 1, 258, 259], type=pa.uint64()),
                        pa.array([0.0, 1.0, 0.0, 1.0], type=pa.float64()),
                    ],
                    schema=pa.schema(
                        [
                            pa.field("query_ordinal", pa.uint32(), nullable=False),
                            pa.field("rank", pa.uint16(), nullable=False),
                            pa.field("feature_row_id", pa.uint64(), nullable=False),
                            pa.field("squared_distance", pa.float64(), nullable=False),
                        ]
                    ),
                ),
                truth,
            )

            def write_run(path: pathlib.Path, pages: list[list[int]]) -> list[dict[str, int]]:
                schema = pa.schema(
                    [
                        pa.field("id", pa.int64(), nullable=False),
                        pa.field("sequence", pa.uint64(), nullable=False),
                        pa.field("state", pa.uint8(), nullable=False),
                        pa.field(
                            "code",
                            pa.list_(
                                pa.field("element", pa.uint8(), nullable=False),
                                dimensions,
                            ),
                            nullable=False,
                        ),
                    ]
                )
                body = bytearray()
                roster = []
                for ordinal, ids in enumerate(pages):
                    table = pa.Table.from_arrays(
                        [
                            pa.array(ids, type=pa.int64()),
                            pa.array([1] * len(ids), type=pa.uint64()),
                            pa.array([0] * len(ids), type=pa.uint8()),
                            pa.FixedSizeListArray.from_arrays(
                                pa.array([0] * (len(ids) * dimensions), type=pa.uint8()),
                                dimensions,
                            ),
                        ],
                        schema=schema,
                    )
                    sink = pa.BufferOutputStream()
                    with ipc.new_stream(sink, schema) as writer:
                        writer.write_table(table)
                    page_body = sink.getvalue().to_pybytes()
                    roster.append(
                        {
                            "bytes": len(page_body),
                            "offset": len(body),
                            "page": ordinal,
                            "rows": len(ids),
                        }
                    )
                    body.extend(page_body)
                path.write_bytes(body)
                return roster

            base_pages = [[2 * page, 2 * page + 1] for page in range(128)]
            delta_pages = [[256, 257], [258, 259]]
            base_roster = write_run(base, base_pages)
            delta_roster = write_run(delta, delta_pages)

            def identity(path: pathlib.Path) -> ObjectIdentity:
                body = path.read_bytes()
                return ObjectIdentity(
                    uri=f"s3://fixture/{path.name}",
                    sha256=hashlib.sha256(body).hexdigest(),
                    bytes=len(body),
                )

            base_identity = identity(base)
            delta_identity = identity(delta)
            manifest = {
                "dimensions": dimensions,
                "runs": [
                    {
                        "kind": "base",
                        "object": {
                            "bytes": base_identity.bytes,
                            "sha256": base_identity.sha256,
                            "uri": base_identity.uri,
                        },
                        "pages": base_roster,
                    },
                    {
                        "kind": "delta",
                        "object": {
                            "bytes": delta_identity.bytes,
                            "sha256": delta_identity.sha256,
                            "uri": delta_identity.uri,
                        },
                        "pages": delta_roster,
                    },
                ],
            }
            generation.write_bytes(
                json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            identities = {
                role: identity(path)
                for role, path in {
                    "source": source,
                    "queries": queries,
                    "truth": truth,
                    "generation": generation,
                    "base": base,
                    "delta": delta,
                }.items()
            }

            loaded = load_screen_inputs(
                source=source,
                queries=queries,
                truth=truth,
                generation=generation,
                base=base,
                delta=delta,
                identities=identities,
                dimensions=dimensions,
                neighbors=2,
                query_count=2,
                seed=7216,
            )

            self.assertEqual(loaded.source_ids.tolist(), source_ids.tolist())
            self.assertEqual(loaded.page_by_id[258], PageKey("delta", 1))
            self.assertEqual(loaded.row_order_by_page[PageKey("base", 0)], (0, 1))
            self.assertEqual(len(loaded.pages), 130)
            self.assertEqual(len(page_map_sha256(loaded)), 64)
            output = root / "result.json"
            document = run_screen(
                loaded,
                identities=identities,
                source_commit="1" * 40,
                critique_result_sha256="2" * 64,
                output=output,
            )
            self.assertEqual(json.loads(output.read_bytes()), document)
            self.assertEqual(
                output.read_bytes(),
                json.dumps(document, separators=(",", ":"), sort_keys=True).encode()
                + b"\n",
            )

            changed = dict(identities)
            changed["delta"] = ObjectIdentity(
                uri=delta_identity.uri,
                sha256="0" * 64,
                bytes=delta_identity.bytes,
            )
            with self.assertRaisesRegex(ValueError, "delta identity differs"):
                load_screen_inputs(
                    source=source,
                    queries=queries,
                    truth=truth,
                    generation=generation,
                    base=base,
                    delta=delta,
                    identities=changed,
                    dimensions=dimensions,
                    neighbors=2,
                    query_count=2,
                    seed=7216,
                )

    def test_five_arm_evaluator_uses_one_fence_and_charges_delta_pages(self) -> None:
        # Break caught: the full producer bypasses the bounded page-summary
        # fence, uses a distinct summary-only planner, or grants delta truth
        # rows without selecting and charging their physical page.
        dimensions = 96
        page_keys = [PageKey("base", ordinal) for ordinal in range(128)]
        page_keys.extend((PageKey("delta", 0), PageKey("delta", 1)))
        row_ids = np.arange(260, dtype=np.int64)
        vectors = np.zeros((260, dimensions), dtype=np.float32)
        vectors[:, 0] = np.arange(260, dtype=np.float32)
        vectors[:, 1:] = (
            np.arange(dimensions - 1, dtype=np.float32)[None, :] / 1000.0
        )
        row_order_by_page = {
            key: (2 * ordinal, 2 * ordinal + 1)
            for ordinal, key in enumerate(page_keys[:128])
        }
        row_order_by_page[page_keys[128]] = (256, 257)
        row_order_by_page[page_keys[129]] = (258, 259)
        page_by_id = {
            row_id: key
            for key, rows in row_order_by_page.items()
            for row_id in rows
        }
        pages = {
            key: RoutedPage(key, offset=ordinal * 10, encoded_bytes=10)
            for ordinal, key in enumerate(page_keys)
        }
        inputs = ScreenInputs(
            source_ids=row_ids,
            vectors=vectors,
            queries=np.ascontiguousarray(vectors[[0, 258]]),
            truth_ids=np.asarray([[0, 1], [258, 259]], dtype=np.int64),
            page_by_id=page_by_id,
            pages=pages,
            row_order_by_page=row_order_by_page,
            seed=7216,
            neighbors=2,
            max_gets=32,
            max_bytes=16 * 1024 * 1024,
            summary_page_limit=128,
            shortlist_rows=512,
            training_rows=256,
            training_iterations=1,
        )

        result = evaluate_width_arms(inputs)
        document = screen_result_document(
            result,
            authority=ScreenAuthority(
                source_commit="1" * 40,
                critique_result_sha256="2" * 64,
                page_map_sha256="3" * 64,
                dimensions=dimensions,
                seed=7216,
                identities={
                    role: ObjectIdentity(
                        uri=f"s3://fixture/{role}", sha256="4" * 64, bytes=1
                    )
                    for role in (
                        "source",
                        "queries",
                        "truth",
                        "generation",
                        "base",
                        "delta",
                    )
                },
            ),
            neighbors=2,
            max_gets=32,
            max_bytes=16 * 1024 * 1024,
            bootstrap_seed=7216,
            bootstrap_resamples=10_000,
        )

        self.assertEqual(result.query_count, 2)
        self.assertEqual(
            tuple(result.arms),
            (
                "pq16x8",
                "pq24x8",
                "pq32x8",
                "pq32x4",
                "summary-only-pq16x8",
            ),
        )
        self.assertEqual(len(result.exact_f32_samples), 2)
        self.assertEqual(document["schema"], "borsuk-v97-row-width-screen-v1")
        self.assertEqual(document["queries"], 2)
        self.assertEqual(
            set(document["artifacts"]),
            {"summary-router", "pq16x8", "pq24x8", "pq32x8", "pq32x4"},
        )
        rescored = validate_screen_result(
            document,
            page_by_id={
                row_id: (key.object_role, key.ordinal)
                for row_id, key in page_by_id.items()
            },
            page_bytes={
                (key.object_role, key.ordinal): page.encoded_bytes
                for key, page in pages.items()
            },
            page_ranges={
                (key.object_role, key.ordinal): (page.offset, page.encoded_bytes)
                for key, page in pages.items()
            },
        )
        self.assertEqual(rescored["winner"], document["winner"])
        for name, samples in result.arms.items():
            with self.subTest(arm=name):
                self.assertEqual(len(samples), 2)
                self.assertEqual(samples[1]["query"], 1)
                self.assertEqual(samples[1]["truth_ids"], [258, 259])
                selected = {
                    (page["object_role"], page["ordinal"])
                    for page in samples[1]["selected_pages"]
                }
                if samples[1]["hits"]:
                    self.assertIn(("delta", 1), selected)
                self.assertEqual(
                    samples[1]["bytes"], 10 * samples[1]["gets"]
                )
                self.assertEqual(len(samples[1]["ranges"]), samples[1]["gets"])
                self.assertTrue(all(item["bytes"] == 10 for item in samples[1]["ranges"]))

    def test_four_bit_pq_fit_is_deterministic_and_persists_only_packed_codes(
        self,
    ) -> None:
        # Break caught: training depends on query order, produces unpacked
        # persistent codes, or changes under the same seed and source rows.
        spec = PqSpec("fixture-pq2x4", 2, 4, 1)
        values = np.arange(16, dtype=np.float32)
        vectors = np.column_stack((values, values, -values, -values)).astype(
            np.float32
        )
        first = fit_pq(vectors, spec, seed=19, sample_rows=16, iterations=2)
        second = fit_pq(vectors, spec, seed=19, sample_rows=16, iterations=2)
        self.assertTrue(np.array_equal(first, second))
        self.assertEqual(first.shape, (2, 16, 2))

        packed = encode_pq(vectors, first, spec)
        self.assertEqual(packed.shape, (16, 1))
        scores = adc_scores(vectors[7], first, packed, spec)
        self.assertEqual(int(np.argmin(scores)), 7)
        self.assertEqual(float(scores[7]), 0.0)

    def test_adc_scores_use_fixed_ascending_float32_accumulation(self) -> None:
        # Break caught: NumPy pairwise reduction changes shortlist-boundary
        # ordering relative to Rust's ascending-subspace fold.
        spec = PqSpec("fixture-pq2x4", 2, 4, 1)
        books = np.zeros((2, 16, 2), dtype=np.float32)
        books[0, 1] = [1.0, 0.0]
        books[1, 2] = [0.0, 2.0]
        books[0, 3] = [3.0, 0.0]
        codes = pack_pq4(np.asarray([[1, 2], [3, 0]], dtype=np.uint8))

        scores = adc_scores(
            np.zeros(4, dtype=np.float32), books, codes, spec
        )
        self.assertEqual(scores.tolist(), [5.0, 9.0])

    def test_page_block_means_match_native_partial_page_convention(self) -> None:
        # Break caught: a partial page drops its second summary or splits at a
        # different row, so the harness summary fence is not the native fence.
        base0 = PageKey("base", 0)
        delta0 = PageKey("delta", 0)
        keys, means = page_block_means(
            {
                base0: np.asarray(
                    [[0.0, 2.0], [2.0, 4.0], [4.0, 6.0]], dtype=np.float32
                ),
                delta0: np.asarray([[8.0, 10.0]], dtype=np.float32),
            }
        )
        self.assertEqual(keys, (base0, base0, delta0, delta0))
        self.assertEqual(
            means.tolist(), [[1.0, 3.0], [4.0, 6.0], [8.0, 10.0], [8.0, 10.0]]
        )

    def test_shared_planner_charges_both_tiers_and_never_grants_delta_hits(
        self,
    ) -> None:
        # Break caught: delta rows are counted as resident/free, or ranked pages
        # from two objects are merged into one apparent GET.
        base0 = PageKey("base", 0)
        delta0 = PageKey("delta", 0)
        base1 = PageKey("base", 1)
        delta1 = PageKey("delta", 1)
        pages = {
            base0: RoutedPage(base0, offset=0, encoded_bytes=4),
            base1: RoutedPage(base1, offset=4, encoded_bytes=4),
            delta0: RoutedPage(delta0, offset=0, encoded_bytes=4),
            delta1: RoutedPage(delta1, offset=4, encoded_bytes=4),
        }
        selected = select_budgeted_pages(
            [base0, delta0, base0, base1, delta1],
            pages,
            max_gets=3,
            max_bytes=12,
        )
        self.assertEqual(selected, (base0, delta0, base1))

        sample = evaluate_selected_pages(
            selected_pages=selected,
            truth_ids=np.asarray([10, 20, 30, 40], dtype=np.int64),
            page_by_id={10: base0, 20: delta0, 30: base1, 40: delta1},
            neighbors=4,
        )
        self.assertEqual(sample.hit_ids, (10, 20, 30))
        self.assertEqual(sample.hits, 3)
        self.assertEqual(sample.hits10, 3)
        self.assertEqual(sample.recall100_ppm, 750_000)
        self.assertNotIn(40, sample.hit_ids)

    def test_shared_planner_enforces_exact_get_and_byte_budgets(self) -> None:
        # Break caught: the planner applies the byte cap after selection or
        # silently fetches an unregistered page.
        keys = [PageKey("base", ordinal) for ordinal in range(4)]
        pages = {
            key: RoutedPage(key, offset=ordinal * 6, encoded_bytes=6)
            for ordinal, key in enumerate(keys)
        }
        self.assertEqual(
            select_budgeted_pages(keys, pages, max_gets=4, max_bytes=12),
            tuple(keys[:2]),
        )
        self.assertEqual(
            select_budgeted_pages(keys, pages, max_gets=1, max_bytes=24),
            (keys[0],),
        )
        with self.assertRaisesRegex(ValueError, "ranked page is not registered"):
            select_budgeted_pages(
                [PageKey("delta", 9)], pages, max_gets=1, max_bytes=24
            )

    def test_top_k_resolves_the_partition_boundary_by_distance_then_id(self) -> None:
        # Break caught: argpartition chooses an arbitrary member of a tied
        # shortlist boundary, which is especially frequent for PQ32x4.
        scores = np.asarray([1.0, 0.0, 1.0, 1.0], dtype=np.float32)
        ids = np.asarray([40, 30, 20, 10], dtype=np.int64)
        ranked = top_k_total_order(scores, ids, 2)
        self.assertEqual(ids[ranked].tolist(), [30, 10])
        scores[0] = np.nan
        with self.assertRaisesRegex(ValueError, "rank input differs"):
            top_k_total_order(scores, ids, 2)


if __name__ == "__main__":
    unittest.main()
