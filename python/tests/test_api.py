import os
import tempfile
import unittest
import uuid
from array import array
from pathlib import Path

import borsuk


class PythonApiTests(unittest.TestCase):
    def test_vector_distance_exposes_dense_metric_catalog(self) -> None:
        self.assertAlmostEqual(
            borsuk.vector_distance("minkowski:3", [0.0, 0.0], [1.0, 2.0]),
            9.0 ** (1.0 / 3.0),
            places=6,
        )
        self.assertAlmostEqual(
            borsuk.vector_distance("cosine", [1.0, 0.0], [1.0, 0.0]),
            0.0,
            places=6,
        )
        self.assertAlmostEqual(
            borsuk.vector_distance(
                "gower",
                [1.0, 2.0, 0.0, 4.0],
                [1.0, 4.0, 3.0, 0.0],
            ),
            2.25,
            places=6,
        )
        self.assertAlmostEqual(
            borsuk.vector_distance(
                "rogers-tanimoto",
                [1.0, 0.0, 1.0, 0.0],
                [1.0, 1.0, 0.0, 0.0],
            ),
            2.0 / 3.0,
            places=6,
        )
        self.assertAlmostEqual(
            borsuk.vector_distance(
                "sokal-sneath",
                [1.0, 0.0, 1.0, 0.0],
                [1.0, 1.0, 0.0, 0.0],
            ),
            0.8,
            places=6,
        )
        self.assertAlmostEqual(
            borsuk.vector_distance("jensen-shannon", [0.5, 0.5], [0.25, 0.75]),
            0.18390779,
            places=6,
        )
        self.assertAlmostEqual(
            borsuk.vector_distance("bhattacharyya", [0.5, 0.5], [0.25, 0.75]),
            0.03466823,
            places=6,
        )
        self.assertAlmostEqual(
            borsuk.vector_distance("earth-mover", [1.0, 0.0, 0.0], [0.0, 0.0, 1.0]),
            2.0,
            places=6,
        )
        self.assertAlmostEqual(
            borsuk.vector_distance("dtw", [0.0, 0.0, 1.0, 1.0], [0.0, 1.0, 1.0, 1.0]),
            0.0,
            places=6,
        )
        self.assertAlmostEqual(
            borsuk.vector_distance("ruzicka", [1.0, 2.0, 0.0], [2.0, 1.0, 3.0]),
            5.0 / 7.0,
            places=6,
        )
        self.assertAlmostEqual(
            borsuk.vector_distance("squared-chord", [1.0, 4.0], [4.0, 1.0]),
            2.0,
            places=6,
        )
        self.assertAlmostEqual(
            borsuk.vector_distance("wave-hedges", [1.0, 2.0, 0.0], [2.0, 1.0, 3.0]),
            2.0,
            places=6,
        )

        with self.assertRaises(ValueError):
            borsuk.vector_distance("euclidean", [1.0], [1.0, 2.0])

    def test_string_distance_exposes_edit_and_similarity_metrics(self) -> None:
        self.assertEqual(
            borsuk.string_distance("damerau-levenshtein", "abcd", "acbd"),
            1.0,
        )
        self.assertEqual(
            borsuk.string_distance("optimal-string-alignment", "abcd", "acbd"),
            1.0,
        )
        self.assertEqual(borsuk.string_distance("hamming", "rust", "dust"), 1.0)
        self.assertAlmostEqual(
            borsuk.string_distance("normalized-levenshtein", "kitten", "sitting"),
            0.42857143,
            places=6,
        )
        self.assertAlmostEqual(
            borsuk.string_distance("normalized-damerau-levenshtein", "abcd", "acbd"),
            0.25,
            places=6,
        )
        self.assertAlmostEqual(
            borsuk.string_distance("sorensen-dice", "night", "nacht"),
            0.75,
            places=6,
        )

        jaro_winkler = borsuk.string_distance("jaro-winkler", "segment", "segments")
        self.assertGreater(jaro_winkler, 0.0)
        self.assertLess(jaro_winkler, 0.2)

        with self.assertRaises(ValueError):
            borsuk.string_distance("not-a-string-metric", "a", "b")

    def test_recall_at_k_measures_top_k_overlap(self) -> None:
        self.assertAlmostEqual(
            borsuk.recall_at_k(
                ["doc-a", "doc-b", "doc-c", "doc-d"],
                ["doc-c", "doc-x", "doc-a", "doc-a"],
                3,
            ),
            2.0 / 3.0,
            places=6,
        )
        self.assertAlmostEqual(
            borsuk.recall_at_k(
                ["doc-a", "doc-b", "doc-c"],
                ["doc-c", "doc-b"],
                10,
            ),
            2.0 / 3.0,
            places=6,
        )
        with self.assertRaisesRegex(ValueError, "k must be greater than zero"):
            borsuk.recall_at_k(["doc-a"], ["doc-a"], 0)

    def test_create_add_search_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = f"file://{tmp}"
            index = borsuk.create(
                uri=uri,
                metric="euclidean",
                dim=2,
                segment_size=1,
            )

            index.add(["a", "b"], [[0.0, 0.0], [1.0, 0.0]])
            hits = index.search([0.2, 0.0], k=2)

            self.assertEqual([hit.id for hit in hits], ["a", "b"])

    def test_add_buffer_accepts_contiguous_float32_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=2,
                segment_size=2,
            )

            index.add_buffer(
                ["a", "b", "c"],
                array("f", [0.0, 0.0, 1.0, 0.0, 9.0, 0.0]),
                payload_refs=["objects/a.parquet", None, "objects/c.parquet"],
            )
            hits = index.search([0.8, 0.0], k=2)

            self.assertEqual([hit.id for hit in hits], ["b", "a"])
            self.assertEqual([hit.payload_ref for hit in hits], [None, "objects/a.parquet"])

    def test_exact_search_does_not_prune_equal_distance_ties(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(["z-tie", "a-tie"], [[1.0, 0.0], [-1.0, 0.0]])
            report = index.search_with_report([0.0, 0.0], k=1)

            self.assertEqual([hit.id for hit in report.hits], ["a-tie"])
            self.assertEqual(report.segments_searched, 2)
            self.assertEqual(report.segments_skipped, 0)

    def test_payload_refs_round_trip_in_hits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = f"file://{tmp}"
            index = borsuk.create(
                uri=uri,
                metric="euclidean",
                dimensions=2,
                segment_size=2,
            )

            index.add(
                ["a", "b"],
                [[0.0, 0.0], [1.0, 0.0]],
                payload_refs=["objects/a.parquet", "objects/b.parquet"],
            )

            reopened = borsuk.open(uri)
            hits = reopened.search([0.1, 0.0], k=2)

            self.assertEqual(
                [hit.payload_ref for hit in hits],
                ["objects/a.parquet", "objects/b.parquet"],
            )

    def test_payload_refs_can_be_missing_per_record(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = f"file://{tmp}"
            index = borsuk.create(
                uri=uri,
                metric="euclidean",
                dimensions=2,
                segment_size=2,
            )

            index.add(
                ["with-ref", "without-ref"],
                [[0.0, 0.0], [1.0, 0.0]],
                payload_refs=["objects/with.parquet", None],
            )

            hits = borsuk.open(uri).search([0.1, 0.0], k=2)

            self.assertEqual([hit.payload_ref for hit in hits], ["objects/with.parquet", None])

    def test_open_with_cache_reads_fresh_current_after_external_publish(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as cache:
            uri = f"file://{tmp}"
            cached = borsuk.create(
                uri=uri,
                metric="euclidean",
                dimensions=2,
                segment_size=2,
                cache_dir=cache,
            )
            self.assertEqual(cached.stats().manifest_version, 1)

            writer = borsuk.open(uri)
            writer.add(["fresh"], [[0.0, 0.0]])
            self.assertEqual(writer.stats().manifest_version, 2)

            reopened = borsuk.open(uri, cache_dir=cache)

            self.assertEqual(reopened.stats().manifest_version, 2)
            self.assertEqual(reopened.stats().records, 1)
            self.assertEqual(reopened.search([0.0, 0.0], k=1)[0].id, "fresh")

    def test_search_batch_preserves_query_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                ["left", "middle", "right"],
                [[0.0, 0.0], [5.0, 0.0], [10.0, 0.0]],
            )
            results = index.search_batch([[0.1, 0.0], [9.9, 0.0]], k=1)

            self.assertEqual([[hit.id for hit in hits] for hits in results], [["left"], ["right"]])

    def test_search_batch_buffer_accepts_contiguous_float32_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                ["left", "middle", "right"],
                [[0.0, 0.0], [5.0, 0.0], [10.0, 0.0]],
            )
            results = index.search_batch_buffer(
                array("f", [0.1, 0.0, 9.9, 0.0]),
                k=1,
            )

            self.assertEqual([[hit.id for hit in hits] for hits in results], [["left"], ["right"]])

    def test_search_batch_with_report_preserves_query_order_and_counters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                ["left", "middle", "right"],
                [[0.0, 0.0], [5.0, 0.0], [10.0, 0.0]],
            )
            reports = index.search_batch_with_report([[0.1, 0.0], [9.9, 0.0]], k=1)

            self.assertEqual([report.hits[0].id for report in reports], ["left", "right"])
            self.assertEqual([report.segments_total for report in reports], [3, 3])
            self.assertGreater(reports[0].bytes_read, 0)
            self.assertGreater(reports[1].bytes_read, 0)
            self.assertGreater(reports[0].resident_bytes_estimate, 0)
            self.assertGreater(reports[1].resident_bytes_estimate, 0)

    def test_stats_expose_manifest_and_resident_budget_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = f"file://{tmp}"
            index = borsuk.create(
                uri=uri,
                metric="euclidean",
                dimensions=2,
                segment_size=2,
                ram_budget="1MB",
            )

            index.add(
                ["a", "b", "c"],
                [[0.0, 0.0], [1.0, 0.0], [10.0, 0.0]],
            )
            stats = index.stats()

            self.assertEqual(stats.metric, "euclidean")
            self.assertEqual(stats.dimensions, 2)
            self.assertEqual(stats.segment_max_vectors, 2)
            self.assertEqual(stats.ram_budget_bytes, 1_000_000)
            self.assertEqual(stats.manifest_version, 2)
            self.assertEqual(stats.segments, 2)
            self.assertEqual(stats.records, 3)
            self.assertGreater(stats.segment_bytes, 0)
            self.assertGreater(stats.graph_bytes, 0)
            self.assertGreater(stats.resident_bytes_estimate, 0)

            reopened = borsuk.open(uri, ram_budget="500KB")
            self.assertEqual(reopened.stats().ram_budget_bytes, 500_000)

    def test_create_enforces_ram_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError, "RAM budget exceeded"):
                borsuk.create(
                    uri=f"file://{tmp}",
                    metric="euclidean",
                    dimensions=2,
                    segment_size=1,
                    ram_budget="1B",
                )

    def test_runtime_errors_use_typed_exception(self) -> None:
        self.assertIsNot(borsuk.BorsukError, RuntimeError)

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(borsuk.BorsukError, "RAM budget exceeded"):
                borsuk.create(
                    uri=f"file://{tmp}",
                    metric="euclidean",
                    dimensions=2,
                    segment_size=1,
                    ram_budget="1B",
                )

    def test_open_enforces_runtime_ram_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = f"file://{tmp}"
            borsuk.create(uri=uri, metric="euclidean", dimensions=2, segment_size=1)

            with self.assertRaisesRegex(RuntimeError, "RAM budget exceeded"):
                borsuk.open(uri, ram_budget="1B")

    def test_search_with_report_exposes_query_counters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                ["near", "mid", "far"],
                [[0.0, 0.0], [10.0, 0.0], [20.0, 0.0]],
            )
            report = index.search_with_report([0.0, 0.0], k=1)

            self.assertEqual(report.hits[0].id, "near")
            self.assertEqual(report.segments_total, 3)
            self.assertEqual(report.segments_searched, 1)
            self.assertEqual(report.segments_skipped, 2)
            self.assertGreater(report.bytes_read, 0)
            self.assertEqual(report.object_cache_hits, 0)
            self.assertGreater(report.object_cache_misses, 0)
            self.assertGreater(report.resident_bytes_estimate, 0)
            self.assertGreaterEqual(report.elapsed_ms, 0)

    def test_approx_search_limits_exact_scoring_inside_segment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=1,
                segment_size=4,
            )

            index.add(
                ["near", "next", "far-a", "far-b"],
                [[0.0], [0.2], [10.0], [20.0]],
            )
            report = index.search_with_report(
                [0.05],
                k=1,
                mode="approx",
                max_candidates_per_segment=2,
            )

            self.assertEqual(report.hits[0].id, "near")
            self.assertEqual(report.records_considered, 4)
            self.assertEqual(report.records_scored, 2)

    def test_approx_search_enforces_candidate_budget_when_k_is_larger(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=1,
                segment_size=4,
            )

            index.add(
                ["near", "next", "far-a", "far-b"],
                [[0.0], [0.2], [10.0], [20.0]],
            )
            report = index.search_with_report(
                [0.05],
                k=3,
                mode="approx",
                max_candidates_per_segment=2,
            )

            self.assertEqual(len(report.hits), 2)
            self.assertEqual(report.records_considered, 4)
            self.assertEqual(report.records_scored, 2)

    def test_approx_search_obeys_byte_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                ["near", "mid", "far"],
                [[0.0, 0.0], [10.0, 0.0], [20.0, 0.0]],
            )
            report = index.search_with_report(
                [0.0, 0.0],
                k=3,
                mode="approx",
                max_bytes=1,
            )

            self.assertEqual([hit.id for hit in report.hits], ["near"])
            self.assertEqual(report.segments_searched, 1)
            self.assertEqual(report.segments_skipped, 2)
            self.assertGreater(report.bytes_read, 1)

    def test_approx_search_accepts_byte_budget_string(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                ["near", "mid", "far"],
                [[0.0, 0.0], [10.0, 0.0], [20.0, 0.0]],
            )
            report = index.search_with_report(
                [0.0, 0.0],
                k=3,
                mode="approx",
                max_bytes="1B",
            )

            self.assertEqual([hit.id for hit in report.hits], ["near"])
            self.assertEqual(report.segments_searched, 1)
            self.assertEqual(report.segments_skipped, 2)

    def test_approx_search_rejects_invalid_budgets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )
            index.add(["near"], [[0.0, 0.0]])

            for kwargs, expected in [
                ({"eps": -0.1}, "eps must be non-negative when set"),
                ({"max_segments": 0}, "max_segments must be greater than zero when set"),
                ({"max_bytes": 0}, "max_bytes must be greater than zero when set"),
                ({"max_latency_ms": 0}, "max_latency_ms must be greater than zero when set"),
                (
                    {"max_candidates_per_segment": 0},
                    "max_candidates_per_segment must be greater than zero when set",
                ),
            ]:
                with self.subTest(kwargs=kwargs):
                    with self.assertRaisesRegex(RuntimeError, expected):
                        index.search_with_report([0.0, 0.0], k=1, mode="approx", **kwargs)

    def test_approx_search_expands_segment_graph_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=2,
                segment_size=4,
            )

            index.add(
                ["entry", "true-neighbor", "routing-decoy", "far"],
                [[0.0, 0.0], [0.0, 0.1], [0.1, -0.1], [100.0, 100.0]],
            )
            report = index.search_with_report(
                [0.04, 0.07],
                k=1,
                mode="approx",
                max_candidates_per_segment=2,
            )

            self.assertEqual(report.hits[0].id, "true-neighbor")
            self.assertEqual(report.records_considered, 4)
            self.assertEqual(report.records_scored, 2)
            self.assertGreater(report.graph_bytes_read, 0)
            self.assertEqual(report.graph_candidates_added, 1)

    def test_approx_search_walks_segment_graph_beyond_first_hop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=2,
                segment_size=10,
            )

            index.add(
                [
                    "aa-entry",
                    "bb-hop",
                    "cc-decoy-0",
                    "cc-decoy-1",
                    "cc-decoy-2",
                    "cc-decoy-3",
                    "cc-decoy-4",
                    "cc-decoy-5",
                    "cc-decoy-6",
                    "zz-target",
                ],
                [
                    [0.0, 0.0],
                    [1.0, 1.0],
                    [-1.0, -1.0],
                    [-1.1, -1.1],
                    [-1.2, -1.2],
                    [-1.3, -1.3],
                    [-1.4, -1.4],
                    [-1.5, -1.5],
                    [-1.6, -1.6],
                    [2.0, 2.0],
                ],
            )
            report = index.search_with_report(
                [2.0, 2.0],
                k=1,
                mode="approx",
                max_candidates_per_segment=3,
            )

            self.assertEqual(report.hits[0].id, "zz-target")
            self.assertEqual(report.records_considered, 10)
            self.assertEqual(report.records_scored, 3)
            self.assertEqual(report.graph_candidates_added, 2)

    def test_cache_dir_populates_segment_and_graph_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as cache:
            writer = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=2,
                segment_size=4,
            )

            writer.add(
                ["entry", "true-neighbor", "routing-decoy", "far"],
                [[0.0, 0.0], [0.0, 0.1], [0.1, -0.1], [100.0, 100.0]],
            )
            index = borsuk.open(f"file://{tmp}", cache_dir=cache)
            report = index.search_with_report(
                [0.04, 0.07],
                k=1,
                mode="approx",
                max_candidates_per_segment=2,
            )

            self.assertEqual(report.hits[0].id, "true-neighbor")
            self.assertGreater(report.graph_bytes_read, 0)
            self.assertEqual(report.object_cache_hits, 0)
            self.assertEqual(report.object_cache_misses, 2)
            self.assertTrue(list((Path(cache) / "segments").rglob("*.parquet")))
            self.assertTrue(list((Path(cache) / "graphs").rglob("*.parquet")))

    def test_s3_compatible_storage_round_trips_when_configured(self) -> None:
        base_uri = os.environ.get("BORSUK_S3_TEST_URI")
        if not base_uri:
            self.skipTest("BORSUK_S3_TEST_URI is not set")

        uri = f"{base_uri.rstrip('/')}/python-{uuid.uuid4()}"
        with tempfile.TemporaryDirectory() as cache:
            index = borsuk.create(
                uri=uri,
                metric="euclidean",
                dimensions=2,
                segment_size=2,
            )

            index.add(
                ["entry", "true-neighbor", "routing-decoy", "far"],
                [[0.0, 0.0], [0.0, 0.1], [0.1, -0.1], [100.0, 100.0]],
                payload_refs=[
                    "objects/entry.parquet",
                    "objects/true-neighbor.parquet",
                    "objects/routing-decoy.parquet",
                    "objects/far.parquet",
                ],
            )
            reopened = borsuk.open(uri, cache_dir=cache)
            report = reopened.search_with_report(
                [0.04, 0.07],
                k=1,
                mode="approx",
                max_candidates_per_segment=2,
            )

            self.assertEqual(report.hits[0].id, "true-neighbor")
            self.assertEqual(report.hits[0].payload_ref, "objects/true-neighbor.parquet")
            self.assertGreater(report.graph_bytes_read, 0)
            self.assertGreater(report.object_cache_misses, 0)
            self.assertTrue(list((Path(cache) / "segments").rglob("*.parquet")))
            self.assertTrue(list((Path(cache) / "graphs").rglob("*.parquet")))

            compaction = reopened.compact(
                source_level=0,
                target_level=1,
                max_segments=2,
                min_segments=2,
                target_segment_max_vectors=4,
            )
            self.assertTrue(compaction.compacted)
            self.assertEqual(compaction.segments_written, 1)

            gc = reopened.gc_obsolete_segments()
            self.assertTrue(gc.dry_run)
            self.assertGreater(len(gc.candidates), 0)

    def test_compact_rewrites_segments_and_reports_counters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                ["a", "b", "c", "d"],
                [[0.0, 0.0], [1.0, 0.0], [8.0, 0.0], [9.0, 0.0]],
            )

            before = index.search_with_report([8.5, 0.0], k=2)
            self.assertEqual(before.segments_total, 4)

            report = index.compact(
                source_level=0,
                target_level=1,
                max_segments=4,
                min_segments=2,
                target_segment_max_vectors=2,
            )

            self.assertTrue(report.compacted)
            self.assertEqual(report.segments_read, 4)
            self.assertEqual(report.segments_written, 2)
            self.assertEqual(report.records_rewritten, 4)
            self.assertGreater(report.bytes_read, 0)
            self.assertGreater(report.bytes_written, 0)
            self.assertEqual(report.object_cache_hits, 0)
            self.assertEqual(report.object_cache_misses, 4)

            after = index.search_with_report([8.5, 0.0], k=2)
            self.assertEqual(after.segments_total, 2)
            self.assertEqual([hit.id for hit in after.hits], ["c", "d"])

    def test_gc_obsolete_segments_dry_runs_and_deletes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                ["a", "b", "c", "d"],
                [[0.0, 0.0], [1.0, 0.0], [8.0, 0.0], [9.0, 0.0]],
            )
            index.compact(target_segment_max_vectors=2)

            dry_run = index.gc_obsolete_segments()
            self.assertTrue(dry_run.dry_run)
            self.assertEqual(dry_run.objects_scanned, 12)
            self.assertEqual(dry_run.objects_deleted, 0)
            self.assertEqual(len(dry_run.candidates), 8)
            self.assertGreater(dry_run.bytes_reclaimable, 0)

            deleted = index.gc_obsolete_segments(dry_run=False)
            self.assertFalse(deleted.dry_run)
            self.assertEqual(deleted.objects_deleted, 8)
            self.assertEqual(deleted.candidates, dry_run.candidates)
            self.assertEqual(deleted.bytes_reclaimed, dry_run.bytes_reclaimable)

            hits = index.search([8.5, 0.0], k=2)
            self.assertEqual([hit.id for hit in hits], ["c", "d"])

    def test_gc_obsolete_segments_removes_cached_inactive_objects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as cache:
            index = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=2,
                segment_size=1,
                cache_dir=cache,
            )

            index.add(
                ["a", "b", "c", "d"],
                [[0.0, 0.0], [1.0, 0.0], [8.0, 0.0], [9.0, 0.0]],
            )
            index.compact(
                source_level=0,
                target_level=1,
                max_segments=4,
                min_segments=2,
                target_segment_max_vectors=2,
            )

            self.assertEqual(
                len(list((Path(cache) / "segments" / "L0").rglob("*.parquet"))),
                4,
            )
            self.assertEqual(
                len(list((Path(cache) / "graphs" / "L0").rglob("*.parquet"))),
                4,
            )

            deleted = index.gc_obsolete_segments(dry_run=False)

            self.assertEqual(deleted.objects_deleted, 8)
            self.assertFalse(list((Path(cache) / "segments" / "L0").rglob("*.parquet")))
            self.assertFalse(list((Path(cache) / "graphs" / "L0").rglob("*.parquet")))
            self.assertEqual(
                len(list((Path(cache) / "segments" / "L1").rglob("*.parquet"))),
                2,
            )
            self.assertEqual(
                len(list((Path(cache) / "graphs" / "L1").rglob("*.parquet"))),
                2,
            )

    def test_add_rejects_mismatched_lengths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(uri=f"file://{tmp}", metric="euclidean", dim=1)
            with self.assertRaises(ValueError):
                index.add(["a"], [[0.0], [1.0]])


if __name__ == "__main__":
    unittest.main()
