import dataclasses
import hashlib
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from scripts.v97_row_width_screen import (
    PQ16X8,
    PQ24X8,
    PQ32X4,
    PQ32X8,
    PageKey,
    RoutedPage,
    ScreenInputs,
    adc_scores,
    pack_pq4,
)
from scripts.v98_hierarchical_row_router import (
    HierarchyConfig,
    RootGroup,
    blockwise_top_rows,
    build_hierarchy,
    hierarchy_ipc_schema,
    read_hierarchy_ipc,
    route_hierarchy,
    score_retained_rows,
    validate_hierarchy,
)


class V98HierarchyAuthorityTests(unittest.TestCase):
    @staticmethod
    def config() -> HierarchyConfig:
        return HierarchyConfig(
            pages_per_root=8,
            maximum_root_groups=65_536,
            maximum_exposed_pages=4_096,
            retained_pages=1_024,
            maximum_scanned_rows=262_144,
            shortlist_rows=2_048,
            maximum_gets=32,
            maximum_bytes=16 * 1024**2,
        )

    @staticmethod
    def inputs(*, singleton: PageKey | None = None) -> ScreenInputs:
        dimensions = 16
        keys = tuple(
            [PageKey("base", ordinal) for ordinal in range(128)]
            + [PageKey("delta", ordinal) for ordinal in range(9)]
        )
        row_order_by_page: dict[PageKey, tuple[int, ...]] = {}
        page_by_id: dict[int, PageKey] = {}
        vectors: list[list[float]] = []
        source_ids: list[int] = []
        next_id = 10_000
        for page_position, key in enumerate(keys):
            row_count = 1 if key == singleton else 2
            page_rows = tuple(range(next_id, next_id + row_count))
            next_id += row_count
            row_order_by_page[key] = page_rows
            for local, row_id in enumerate(page_rows):
                source_ids.append(row_id)
                page_by_id[row_id] = key
                vectors.append(
                    [
                        np.float32(
                            page_position * 0.03125
                            + local * 0.0078125
                            + dimension * 0.0009765625
                        )
                        for dimension in range(dimensions)
                    ]
                )
        pages = {
            key: RoutedPage(
                key=key,
                offset=position * 4_096,
                encoded_bytes=1_024,
            )
            for position, key in enumerate(keys)
        }
        ids = np.asarray(source_ids, dtype=np.int64)
        vector_array = np.asarray(vectors, dtype=np.float32)
        return ScreenInputs(
            source_ids=ids,
            vectors=vector_array,
            queries=np.zeros((1, dimensions), dtype=np.float32),
            truth_ids=ids[:2].reshape(1, 2),
            page_by_id=page_by_id,
            pages=pages,
            row_order_by_page=row_order_by_page,
            seed=7_216,
            neighbors=2,
            max_gets=32,
            max_bytes=16 * 1024**2,
            training_rows=256,
            training_iterations=1,
        )

    def test_builds_same_role_roots_in_total_order_with_exact_code_shapes(self) -> None:
        # Break caught: roots cross the base/delta object boundary, a partial
        # root is lost, or one entity silently receives fewer than two codes.
        inputs = self.inputs()
        artifact = build_hierarchy(inputs, self.config())

        self.assertEqual(len(artifact.roots), 18)
        self.assertEqual(
            tuple(group.role for group in artifact.roots),
            ("base",) * 16 + ("delta",) * 2,
        )
        self.assertTrue(all(len(group.pages) == 8 for group in artifact.roots[:-1]))
        self.assertEqual(artifact.roots[-1].pages, (PageKey("delta", 8),))
        self.assertEqual(
            artifact.page_keys,
            tuple(sorted(inputs.pages)),
        )
        self.assertEqual(artifact.page_summary_codes.shape, (274, 16))
        self.assertEqual(artifact.root_summary_codes.shape, (36, 16))
        self.assertEqual(artifact.summary_books.shape, (16, 256, 1))
        self.assertEqual(artifact.page_summary_codes.dtype, np.uint8)
        self.assertEqual(artifact.root_summary_codes.dtype, np.uint8)
        self.assertTrue(
            np.array_equal(
                artifact.root_summary_codes[-2],
                artifact.root_summary_codes[-1],
            )
        )

    def test_singleton_page_duplicates_its_only_summary_and_ipc_round_trips(self) -> None:
        # Break caught: a one-row page creates an empty-mean NaN, or the
        # cross-language hierarchy bytes lose order, slot, or child metadata.
        singleton = PageKey("delta", 8)
        inputs = self.inputs(singleton=singleton)
        artifact = build_hierarchy(inputs, self.config())
        singleton_position = artifact.page_keys.index(singleton) * 2

        self.assertTrue(
            np.array_equal(
                artifact.page_summary_codes[singleton_position],
                artifact.page_summary_codes[singleton_position + 1],
            )
        )
        self.assertEqual(artifact.ipc_schema, hierarchy_ipc_schema())
        records = read_hierarchy_ipc(artifact.ipc_bytes)
        self.assertEqual(len(records), 2 * (len(artifact.roots) + len(artifact.page_keys)))
        self.assertEqual(records[0].kind, "root")
        self.assertEqual(records[0].role, "base")
        self.assertEqual(records[-1].kind, "page")
        self.assertEqual(records[-1].role, "delta")
        self.assertEqual(records[-1].entity_ordinal, 8)
        self.assertEqual(records[-1].summary_slot, 1)
        self.assertEqual(hashlib.sha256(artifact.ipc_bytes).hexdigest(), artifact.ipc_sha256)
        validate_hierarchy(artifact, inputs, self.config())

    def test_rejects_cap_root_code_identity_and_visible_roster_drift(self) -> None:
        # Break caught: authenticated hierarchy bytes are reused under a
        # different work cap, child roster, code shape, or visible generation.
        inputs = self.inputs()
        config = self.config()
        artifact = build_hierarchy(inputs, config)

        with self.assertRaisesRegex(ValueError, "hierarchy configuration differs"):
            validate_hierarchy(
                artifact,
                inputs,
                dataclasses.replace(config, maximum_exposed_pages=4_095),
            )

        changed_root = dataclasses.replace(artifact.roots[-1], role="base")
        with self.assertRaisesRegex(ValueError, "root group differs"):
            validate_hierarchy(
                dataclasses.replace(
                    artifact,
                    roots=artifact.roots[:-1] + (changed_root,),
                ),
                inputs,
                config,
            )

        changed_child = dataclasses.replace(
            artifact.roots[-1],
            pages=(PageKey("delta", 7),),
        )
        with self.assertRaisesRegex(ValueError, "root group differs"):
            validate_hierarchy(
                dataclasses.replace(
                    artifact,
                    roots=artifact.roots[:-1] + (changed_child,),
                ),
                inputs,
                config,
            )

        with self.assertRaisesRegex(ValueError, "page summary codes differ"):
            validate_hierarchy(
                dataclasses.replace(
                    artifact,
                    page_summary_codes=artifact.page_summary_codes[:, :-1],
                ),
                inputs,
                config,
            )

        changed_identity = dataclasses.replace(
            artifact.page_summary_codes_identity,
            sha256="0" * 64,
        )
        with self.assertRaisesRegex(ValueError, "page summary identity differs"):
            validate_hierarchy(
                dataclasses.replace(
                    artifact,
                    page_summary_codes_identity=changed_identity,
                ),
                inputs,
                config,
            )

        changed_rows = dict(inputs.row_order_by_page)
        changed_rows[PageKey("delta", 8)] = changed_rows[PageKey("delta", 7)]
        with self.assertRaisesRegex(ValueError, "visible row roster differs"):
            validate_hierarchy(
                artifact,
                dataclasses.replace(inputs, row_order_by_page=changed_rows),
                config,
            )

    def test_rejects_child_offsets_counts_and_nullable_ipc_schema(self) -> None:
        # Break caught: consumers accept ambiguous child slices or nullable
        # Arrow authority that a Rust reader can interpret differently.
        inputs = self.inputs()
        config = self.config()
        artifact = build_hierarchy(inputs, config)
        shifted = dataclasses.replace(
            artifact.roots[1],
            child_offset=artifact.roots[1].child_offset + 1,
        )
        with self.assertRaisesRegex(ValueError, "root group differs"):
            validate_hierarchy(
                dataclasses.replace(
                    artifact,
                    roots=(artifact.roots[0], shifted) + artifact.roots[2:],
                ),
                inputs,
                config,
            )
        shortened = dataclasses.replace(artifact.roots[0], child_count=7)
        with self.assertRaisesRegex(ValueError, "root group differs"):
            validate_hierarchy(
                dataclasses.replace(
                    artifact,
                    roots=(shortened,) + artifact.roots[1:],
                ),
                inputs,
                config,
            )

        schema = hierarchy_ipc_schema()
        nullable_schema = pa.schema(
            [
                pa.field(field.name, field.type, nullable=True)
                if field.name == "summary_slot"
                else field
                for field in schema
            ]
        )
        empty = pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in nullable_schema],
            schema=nullable_schema,
        )
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, nullable_schema) as writer:
            writer.write_table(empty)
        with self.assertRaisesRegex(ValueError, "hierarchy IPC schema differs"):
            read_hierarchy_ipc(sink.getvalue().to_pybytes())


class V98BoundedRoutingTests(unittest.TestCase):
    @staticmethod
    def literal_zero_scoring_artifact():
        inputs = V98HierarchyAuthorityTests.inputs()
        config = V98HierarchyAuthorityTests.config()
        artifact = build_hierarchy(inputs, config)
        return (
            inputs,
            config,
            dataclasses.replace(
                artifact,
                summary_books=np.zeros((16, 256, 1), dtype=np.float32),
                page_summary_codes=np.zeros_like(artifact.page_summary_codes),
                root_summary_codes=np.zeros_like(artifact.root_summary_codes),
            ),
        )

    def test_route_uses_total_order_and_actual_partial_page_row_counts(self) -> None:
        # Break caught: equal summary scores use unstable argpartition order or
        # count every partial page as 256 rows instead of its visible rows.
        inputs, config, artifact = self.literal_zero_scoring_artifact()
        fence = route_hierarchy(
            np.zeros(16, dtype=np.float32), artifact, config
        )

        self.assertEqual(fence.root_evaluations, 18)
        self.assertEqual(fence.page_evaluations, 137)
        self.assertEqual(fence.exposed_pages, tuple(sorted(inputs.pages)))
        self.assertEqual(fence.retained_pages, tuple(sorted(inputs.pages)))
        self.assertEqual(fence.scanned_rows, 274)

    def test_route_stops_before_the_root_that_would_exceed_4096_children(self) -> None:
        # Break caught: the hierarchy admits a whole next root after reaching
        # the child cap, making 100M page and row work data-dependent.
        _, config, seed_artifact = self.literal_zero_scoring_artifact()
        page_keys = tuple(PageKey("base", ordinal) for ordinal in range(4_104))
        roots = tuple(
            RootGroup(
                role="base",
                ordinal=ordinal,
                pages=page_keys[ordinal * 8 : ordinal * 8 + 8],
                child_offset=ordinal * 8,
                child_count=8,
            )
            for ordinal in range(513)
        )
        root_codes = np.zeros((1_026, 16), dtype=np.uint8)
        root_codes[-2:] = 1
        artifact = dataclasses.replace(
            seed_artifact,
            roots=roots,
            page_keys=page_keys,
            page_row_counts=tuple(1 for _ in page_keys),
            root_summary_codes=root_codes,
            page_summary_codes=np.zeros((8_208, 16), dtype=np.uint8),
        )
        fence = route_hierarchy(
            np.zeros(16, dtype=np.float32), artifact, config
        )

        self.assertEqual(fence.root_evaluations, 513)
        self.assertEqual(len(fence.exposed_pages), 4_096)
        self.assertNotIn(PageKey("base", 4_096), fence.exposed_pages)
        self.assertEqual(fence.page_evaluations, 4_096)
        self.assertEqual(fence.retained_pages, page_keys[:1_024])
        self.assertEqual(fence.scanned_rows, 1_024)

    def test_blockwise_top_rows_matches_literal_total_order_at_boundaries(self) -> None:
        # Break caught: bounded heap replacement loses a lower row ID at a tie
        # or assumes IDs are dense/ordered like score-array positions.
        cases = (
            (
                np.asarray([3.0, 1.0, 1.0, 2.0], dtype=np.float32),
                np.asarray([90, 70, 10, 30], dtype=np.int64),
            ),
            (
                np.asarray([np.finfo(np.float32).tiny, 0.0, -0.0], dtype=np.float32),
                np.asarray([8, 9, 7], dtype=np.int64),
            ),
            (
                np.random.default_rng(7_216).normal(size=257).astype(np.float32),
                np.arange(20_000, 20_257, dtype=np.int64)[::-1],
            ),
        )
        for scores, row_ids in cases:
            for count in (1, min(17, scores.size), scores.size):
                expected = tuple(
                    row_id
                    for _, row_id in sorted(
                        zip(scores.tolist(), row_ids.tolist(), strict=True)
                    )[:count]
                )
                self.assertEqual(
                    blockwise_top_rows(scores, row_ids, count, block_rows=13),
                    expected,
                )

    def test_retained_row_scoring_matches_full_adc_sort_for_every_width(self) -> None:
        # Break caught: one width uses the wrong persistent code shape or the
        # bounded heap diverges from exact `(distance,row_id)` ordering.
        generator = np.random.default_rng(1_337)
        query = generator.normal(size=96).astype(np.float32)
        row_ids = np.arange(50_000, 50_037, dtype=np.int64)[::-1]
        for spec in (PQ16X8, PQ24X8, PQ32X8, PQ32X4):
            centroid_count = 1 << spec.centroid_bits
            books = generator.normal(
                size=(spec.subspaces, centroid_count, 96 // spec.subspaces)
            ).astype(np.float32)
            literal_codes = generator.integers(
                0,
                centroid_count,
                size=(row_ids.size, spec.subspaces),
                dtype=np.uint8,
            )
            stored_codes = (
                pack_pq4(literal_codes)
                if spec.centroid_bits == 4
                else literal_codes
            )
            scores = adc_scores(query, books, stored_codes, spec)
            expected = tuple(
                row_id
                for _, row_id in sorted(
                    zip(scores.tolist(), row_ids.tolist(), strict=True)
                )[:19]
            )
            self.assertEqual(
                score_retained_rows(
                    query,
                    row_ids,
                    stored_codes,
                    books,
                    spec,
                    maximum_rows=262_144,
                    shortlist_rows=19,
                    block_rows=7,
                ),
                expected,
            )

    def test_routing_and_row_scoring_reject_malformed_or_over_cap_inputs(self) -> None:
        # Break caught: nonfinite scores, duplicate identities, malformed codes,
        # or excessive scans enter deterministic-looking evidence.
        with self.assertRaisesRegex(ValueError, "bounded row-score input differs"):
            blockwise_top_rows(
                np.asarray([0.0, np.nan], dtype=np.float32),
                np.asarray([1, 2], dtype=np.int64),
                1,
            )
        with self.assertRaisesRegex(ValueError, "bounded row-score input differs"):
            blockwise_top_rows(
                np.asarray([0.0, 1.0], dtype=np.float32),
                np.asarray([1, 1], dtype=np.int64),
                1,
            )
        query = np.zeros(96, dtype=np.float32)
        books = np.zeros((16, 256, 6), dtype=np.float32)
        with self.assertRaisesRegex(ValueError, "retained row-score input differs"):
            score_retained_rows(
                query,
                np.arange(3, dtype=np.int64),
                np.zeros((3, 15), dtype=np.uint8),
                books,
                PQ16X8,
                maximum_rows=2,
                shortlist_rows=1,
            )


if __name__ == "__main__":
    unittest.main()
