import tempfile
import unittest
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

        with self.assertRaises(ValueError):
            borsuk.vector_distance("euclidean", [1.0], [1.0, 2.0])

    def test_string_distance_exposes_edit_and_similarity_metrics(self) -> None:
        self.assertEqual(
            borsuk.string_distance("damerau-levenshtein", "abcd", "acbd"),
            1.0,
        )
        self.assertEqual(borsuk.string_distance("hamming", "rust", "dust"), 1.0)

        jaro_winkler = borsuk.string_distance("jaro-winkler", "segment", "segments")
        self.assertGreater(jaro_winkler, 0.0)
        self.assertLess(jaro_winkler, 0.2)

        with self.assertRaises(ValueError):
            borsuk.string_distance("not-a-string-metric", "a", "b")

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
            index = borsuk.create(
                uri=f"file://{tmp}",
                metric="euclidean",
                dimensions=2,
                segment_size=4,
                cache_dir=cache,
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
            self.assertGreater(report.graph_bytes_read, 0)
            self.assertTrue(list((Path(cache) / "segments").rglob("*.parquet")))
            self.assertTrue(list((Path(cache) / "graphs").rglob("*.parquet")))

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

    def test_add_rejects_mismatched_lengths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(uri=f"file://{tmp}", metric="euclidean", dim=1)
            with self.assertRaises(ValueError):
                index.add(["a"], [[0.0], [1.0]])


if __name__ == "__main__":
    unittest.main()
