import os
import tempfile
import unittest
import uuid
from array import array
from collections.abc import Sequence
from pathlib import Path
from typing import get_args, get_type_hints

import borsuk


def local_uri(path: str) -> str:
    return Path(path).as_uri()


def deterministic_vector(seed: int, dimensions: int) -> list[float]:
    return [
        float(seed) if dimension == 0 else float(dimension) / float(dimensions)
        for dimension in range(dimensions)
    ]


class PythonApiTests(unittest.TestCase):
    def test_metric_name_catalogs_expose_canonical_names(self) -> None:
        self.assertEqual(borsuk.VectorMetricName.COSINE.value, "cosine")
        self.assertEqual(borsuk.SearchMode.APPROX.value, "approx")
        self.assertEqual(borsuk.LeafModeName.FLAT_SCAN.value, "flat-scan")
        self.assertEqual(borsuk.LeafModeName.SQ_SCAN.value, "sq-scan")
        self.assertEqual(borsuk.LeafModeName.PQ_SCAN.value, "pq-scan")
        self.assertEqual(borsuk.LeafModeName.VAMANA_PQ.value, "vamana-pq")
        self.assertEqual(borsuk.LeafModeName.HYBRID.value, "hybrid")
        minkowski = borsuk.minkowski_metric(3)
        self.assertEqual(minkowski, "minkowski:3")
        self.assertEqual(borsuk.MinkowskiMetric("minkowski:3"), minkowski)
        self.assertEqual(
            borsuk.vector_distance(
                borsuk.VectorMetricName.COSINE,
                [1.0, 0.0],
                [1.0, 0.0],
            ),
            0.0,
        )

        vector_names = borsuk.vector_metric_names()
        self.assertIn("euclidean", vector_names)
        self.assertIn("cosine", vector_names)
        self.assertIn("gower", vector_names)
        self.assertIn("jensen-shannon", vector_names)
        self.assertIn("dynamic-time-warping", vector_names)
        self.assertIn("clark", vector_names)
        self.assertNotIn("l2", vector_names)
        for name in vector_names:
            borsuk.vector_distance(name, [1.0, 2.0, 3.0], [2.0, 3.0, 4.0])
        self.assertAlmostEqual(
            borsuk.vector_distance(minkowski, [0.0, 0.0], [1.0, 2.0]),
            9.0 ** (1.0 / 3.0),
            places=6,
        )
        with self.assertRaisesRegex(ValueError, "Minkowski power must be greater than or equal to 1"):
            borsuk.minkowski_metric(0.5)

        leaf_names = borsuk.leaf_mode_names()
        self.assertEqual(leaf_names, ["flat-scan", "sq-scan", "pq-scan", "graph", "vamana-pq", "hybrid"])

    def test_runtime_annotations_include_minkowski_metric(self) -> None:
        create_metric = get_type_hints(borsuk.create)["metric"]
        vector_distance_metric = get_type_hints(borsuk.vector_distance)["metric"]

        self.assertIn(borsuk.MinkowskiMetric, get_args(create_metric))
        self.assertIn(borsuk.MinkowskiMetric, get_args(vector_distance_metric))

    def test_runtime_config_type_aliases_are_exported(self) -> None:
        for name in [
            "CanonicalVectorMetric",
            "VectorMetricAlias",
            "VectorMetric",
            "SearchModeName",
            "CanonicalLeafMode",
            "LeafModeAlias",
            "LeafMode",
            "RecordId",
        ]:
            with self.subTest(name=name):
                self.assertIn(name, borsuk.__all__)
                self.assertTrue(hasattr(borsuk, name))

        self.assertIn(borsuk.VectorMetricName, get_args(borsuk.VectorMetric))
        self.assertIn(borsuk.MinkowskiMetric, get_args(borsuk.VectorMetric))
        self.assertIn(int, get_args(borsuk.RecordId))
        self.assertEqual(get_args(borsuk.SearchModeName), ("exact", "approx"))
        self.assertIn(borsuk.LeafModeName, get_args(borsuk.LeafMode))

    def test_vector_distance_runtime_annotations_accept_sequences(self) -> None:
        hints = get_type_hints(borsuk.vector_distance)

        self.assertEqual(hints["left"], Sequence[float])
        self.assertEqual(hints["right"], Sequence[float])
        self.assertEqual(
            borsuk.vector_distance("euclidean", (0.0, 0.0), (1.0, 0.0)),
            1.0,
        )

    def test_open_has_runtime_annotations(self) -> None:
        hints = get_type_hints(borsuk.open)

        self.assertEqual(hints["uri"], str)
        self.assertEqual(hints["cache_dir"], str | None)
        self.assertEqual(hints["ram_budget"], int | str | None)
        self.assertEqual(hints["resident_routing"], bool)
        self.assertIs(hints["return"], borsuk.Index)

    def test_metric_helper_functions_have_runtime_annotations(self) -> None:
        leaf_mode_hints = get_type_hints(borsuk.leaf_mode_names)
        vector_metric_hints = get_type_hints(borsuk.vector_metric_names)
        recall_hints = get_type_hints(borsuk.recall_at_k)
        tie_recall_hints = get_type_hints(borsuk.tie_aware_recall_at_k)

        self.assertEqual(leaf_mode_hints["return"], list[borsuk.CanonicalLeafMode])
        self.assertEqual(vector_metric_hints["return"], list[borsuk.CanonicalVectorMetric])
        self.assertEqual(recall_hints["exact_ids"], Sequence[borsuk.RecordId])
        self.assertEqual(recall_hints["actual_ids"], Sequence[borsuk.RecordId])
        self.assertEqual(recall_hints["k"], int)
        self.assertEqual(recall_hints["return"], float)
        self.assertEqual(tie_recall_hints["exact_distances"], Sequence[float])
        self.assertEqual(tie_recall_hints["actual_distances"], Sequence[float])
        self.assertEqual(tie_recall_hints["k"], int)
        self.assertEqual(tie_recall_hints["return"], float)

    def test_tie_aware_recall_counts_equal_distance_hits_without_ids(self) -> None:
        self.assertEqual(
            borsuk.tie_aware_recall_at_k([0.0, 0.0], [0.0, 0.0], 2),
            1.0,
        )
        self.assertAlmostEqual(
            borsuk.tie_aware_recall_at_k([0.0, 0.0, 0.2], [0.0, 0.2, 0.3], 3),
            2.0 / 3.0,
            places=6,
        )
        with self.assertRaisesRegex(ValueError, "k must be greater than zero"):
            borsuk.tie_aware_recall_at_k([0.0], [0.0], 0)
        with self.assertRaisesRegex(ValueError, "k must be an integer"):
            borsuk.tie_aware_recall_at_k([0.0], [0.0], 1.5)  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "k must be an integer"):
            borsuk.tie_aware_recall_at_k([0.0], [0.0], True)  # type: ignore[arg-type]

    def test_result_classes_have_runtime_annotations(self) -> None:
        hit_hints = get_type_hints(borsuk.Hit)
        stats_hints = get_type_hints(borsuk.IndexStats)
        report_hints = get_type_hints(borsuk.SearchReport)
        compaction_hints = get_type_hints(borsuk.CompactionReport)
        gc_hints = get_type_hints(borsuk.GarbageCollectionReport)
        rebuild_hints = get_type_hints(borsuk.RebuildReport)

        self.assertIn("id", hit_hints)
        self.assertEqual(hit_hints["id"], str)
        self.assertEqual(hit_hints["id_bytes"], bytes)
        self.assertEqual(hit_hints["distance"], float)
        self.assertEqual(stats_hints["metric"], borsuk.CanonicalVectorMetric | borsuk.MinkowskiMetric)
        self.assertEqual(stats_hints["dimensions"], int)
        self.assertEqual(stats_hints["ram_budget_bytes"], int | None)
        self.assertEqual(stats_hints["routing_max_level"], int)
        self.assertEqual(stats_hints["routing_page_fanout"], int)
        self.assertEqual(stats_hints["routing_leaf_pages"], int)
        self.assertEqual(stats_hints["routing_pages"], int)
        self.assertEqual(report_hints["hits"], list[borsuk.Hit])
        self.assertEqual(report_hints["leaf_mode"], borsuk.CanonicalLeafMode)
        self.assertEqual(report_hints["termination_reason"], borsuk.SearchTerminationReason)
        self.assertEqual(report_hints["routing_page_indexes_read"], int)
        self.assertEqual(report_hints["routing_pages_read"], int)
        self.assertEqual(report_hints["graph_bytes_read"], int)
        self.assertEqual(report_hints["graph_candidates_added"], int)
        self.assertEqual(compaction_hints["compacted"], bool)
        self.assertEqual(compaction_hints["manifest_version"], int)
        self.assertEqual(gc_hints["dry_run"], bool)
        self.assertEqual(gc_hints["objects_scanned"], int)
        self.assertEqual(gc_hints["objects_deleted"], int)
        self.assertEqual(gc_hints["routing_objects_deleted"], int)
        self.assertEqual(gc_hints["tables_deleted"], int)
        self.assertEqual(gc_hints["routing_page_indexes_read"], int)
        self.assertEqual(gc_hints["routing_pages_read"], int)
        self.assertEqual(gc_hints["bytes_read"], int)
        self.assertEqual(gc_hints["object_cache_hits"], int)
        self.assertEqual(gc_hints["object_cache_misses"], int)
        self.assertEqual(gc_hints["candidates"], list[str])
        self.assertIs(rebuild_hints["compaction"], borsuk.CompactionReport)
        self.assertIs(rebuild_hints["garbage_collection"], borsuk.GarbageCollectionReport)

    def test_index_core_methods_have_runtime_annotations(self) -> None:
        add_hints = get_type_hints(borsuk.Index.add)
        search_ids_hints = get_type_hints(borsuk.Index.search_ids)
        search_id_bytes_hints = get_type_hints(borsuk.Index.search_id_bytes)
        search_vectors_hints = get_type_hints(borsuk.Index.search_vectors)
        get_vector_hints = get_type_hints(borsuk.Index.get_vector)

        self.assertEqual(add_hints["vectors"], Sequence[Sequence[float]])
        self.assertEqual(add_hints["ids"], Sequence[borsuk.RecordId] | None)
        self.assertEqual(add_hints["return"], list[borsuk.RecordId])
        self.assertEqual(search_ids_hints["query"], Sequence[float])
        self.assertEqual(search_ids_hints["return"], list[str])
        self.assertEqual(search_id_bytes_hints["query"], Sequence[float])
        self.assertEqual(search_id_bytes_hints["return"], list[bytes])
        self.assertEqual(search_vectors_hints["query"], Sequence[float])
        self.assertEqual(search_vectors_hints["return"], list[list[float]])
        self.assertEqual(get_vector_hints["id"], borsuk.RecordId)
        self.assertEqual(get_vector_hints["return"], list[float] | None)

    def test_index_batch_report_buffer_and_admin_methods_have_runtime_annotations(self) -> None:
        add_buffer_hints = get_type_hints(borsuk.Index.add_buffer)
        search_ids_batch_hints = get_type_hints(borsuk.Index.search_ids_batch)
        search_id_bytes_batch_hints = get_type_hints(borsuk.Index.search_id_bytes_batch)
        search_vectors_batch_hints = get_type_hints(borsuk.Index.search_vectors_batch)
        search_with_report_hints = get_type_hints(borsuk.Index.search_with_report)
        search_batch_with_report_hints = get_type_hints(borsuk.Index.search_batch_with_report)
        stats_hints = get_type_hints(borsuk.Index.stats)
        compact_hints = get_type_hints(borsuk.Index.compact)
        rebuild_hints = get_type_hints(borsuk.Index.rebuild)
        gc_hints = get_type_hints(borsuk.Index.gc_obsolete_segments)

        self.assertIn("vectors", add_buffer_hints)
        self.assertEqual(add_buffer_hints["vectors"], borsuk.Float32Buffer)
        self.assertEqual(add_buffer_hints["ids"], Sequence[borsuk.RecordId] | None)
        self.assertEqual(add_buffer_hints["return"], list[borsuk.RecordId])
        self.assertEqual(search_ids_batch_hints["queries"], Sequence[Sequence[float]])
        self.assertEqual(search_ids_batch_hints["return"], list[list[str]])
        self.assertEqual(search_id_bytes_batch_hints["queries"], Sequence[Sequence[float]])
        self.assertEqual(search_id_bytes_batch_hints["return"], list[list[bytes]])
        self.assertEqual(search_vectors_batch_hints["queries"], Sequence[Sequence[float]])
        self.assertEqual(search_vectors_batch_hints["return"], list[list[list[float]]])
        self.assertEqual(search_with_report_hints["query"], Sequence[float])
        self.assertEqual(search_with_report_hints["routing_page_overfetch"], int | None)
        self.assertIs(search_with_report_hints["return"], borsuk.SearchReport)
        self.assertEqual(search_batch_with_report_hints["queries"], Sequence[Sequence[float]])
        self.assertEqual(search_batch_with_report_hints["return"], list[borsuk.SearchReport])
        self.assertIs(stats_hints["return"], borsuk.IndexStats)
        self.assertIs(compact_hints["return"], borsuk.CompactionReport)
        self.assertIs(rebuild_hints["return"], borsuk.RebuildReport)
        self.assertEqual(gc_hints["min_age_seconds"], float)
        self.assertIs(gc_hints["return"], borsuk.GarbageCollectionReport)

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
        self.assertAlmostEqual(
            borsuk.recall_at_k(
                [b"\x00\x9f\xff\x07", 300, "doc-c"],
                [300, b"\x00\x9f\xff\x07"],
                3,
            ),
            2.0 / 3.0,
            places=6,
        )
        with self.assertRaisesRegex(ValueError, "k must be greater than zero"):
            borsuk.recall_at_k(["doc-a"], ["doc-a"], 0)
        with self.assertRaisesRegex(ValueError, "k must be an integer"):
            borsuk.recall_at_k(["doc-a"], ["doc-a"], 1.5)  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "k must be an integer"):
            borsuk.recall_at_k(["doc-a"], ["doc-a"], True)  # type: ignore[arg-type]

    def test_create_add_search_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = local_uri(tmp)
            index = borsuk.create(
                uri=uri,
                metric="euclidean",
                dim=2,
                segment_size=1,
            )

            index.add([[0.0, 0.0], [1.0, 0.0]], ids=["a", "b"])
            ids = index.search_ids([0.2, 0.0], k=2)

            self.assertEqual(ids, ["a", "b"])

    def test_create_rejects_conflicting_segment_size_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(
                ValueError,
                "segment_size and segment_max_vectors disagree",
            ):
                borsuk.create(
                    uri=local_uri(tmp),
                    metric="euclidean",
                    dimensions=2,
                    segment_size=1,
                    segment_max_vectors=2,
                )

    def test_create_rejects_non_integer_layout_options(self) -> None:
        for kwargs, expected in [
            ({"dim": 2.5}, "dim must be an integer when set"),
            ({"dimensions": 2.5}, "dimensions must be an integer when set"),
            ({"segment_size": 1.5}, "segment_size must be an integer when set"),
            ({"segment_max_vectors": float("nan")}, "segment_max_vectors must be an integer when set"),
            ({"routing_page_fanout": True}, "routing_page_fanout must be an integer when set"),
        ]:
            with self.subTest(kwargs=kwargs), tempfile.TemporaryDirectory() as tmp:
                options = {
                    "uri": local_uri(tmp),
                    "metric": "euclidean",
                    "dimensions": 2,
                    "segment_size": 2,
                    **kwargs,
                }
                with self.assertRaisesRegex(ValueError, expected):
                    borsuk.create(**options)

    def test_add_accepts_vectors_with_optional_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=2,
            )

            generated_ids = index.add([[0.0, 0.0], [1.0, 0.0]])
            explicit_ids = index.add([[9.0, 0.0]], ids=["far"])

            self.assertEqual(generated_ids, ["0", "1"])
            self.assertEqual(explicit_ids, ["far"])
            self.assertEqual(index.search_ids([0.1, 0.0], k=2), ["0", "1"])

    def test_public_api_has_id_and_vector_searches_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=2,
            )

            self.assertFalse(hasattr(index, "search"))
            self.assertFalse(hasattr(index, "search_buffer"))
            self.assertFalse(hasattr(index, "search_batch"))
            self.assertFalse(hasattr(index, "search_batch_buffer"))
            self.assertTrue(callable(index.search_ids))
            self.assertTrue(callable(index.search_vectors))
            self.assertTrue(callable(index.search_ids_buffer))
            self.assertTrue(callable(index.search_vectors_buffer))
            self.assertTrue(callable(index.search_ids_batch))
            self.assertTrue(callable(index.search_vectors_batch))
            self.assertTrue(callable(index.search_ids_batch_buffer))
            self.assertTrue(callable(index.search_vectors_batch_buffer))
            self.assertTrue(callable(index.get_vector))

    def test_add_rejects_duplicate_ids_and_generated_ids_skip_collisions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=2,
            )

            with self.assertRaisesRegex(borsuk.BorsukError, "duplicate record id"):
                index.add([[0.0, 0.0], [1.0, 0.0]], ids=["dup", "dup"])

            index.add([[0.0, 0.0]], ids=["1"])
            self.assertEqual(index.add([[2.0, 0.0], [3.0, 0.0]]), ["2", "3"])

            with self.assertRaisesRegex(borsuk.BorsukError, "duplicate record id"):
                index.add([[4.0, 0.0]], ids=["2"])

    def test_search_vectors_and_get_vector_return_stored_vectors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = local_uri(tmp)
            index = borsuk.create(
                uri=uri,
                metric="euclidean",
                dimensions=2,
                segment_size=2,
            )

            index.add([[0.0, 0.0], [1.0, 0.0], [9.0, 0.0]], ids=["a", "b", "far"])

            self.assertEqual(index.search_ids([0.8, 0.0], k=2), ["b", "a"])
            self.assertEqual(index.search_vectors([0.8, 0.0], k=2), [[1.0, 0.0], [0.0, 0.0]])
            self.assertEqual(index.get_vector("b"), [1.0, 0.0])
            self.assertIsNone(index.get_vector("missing"))
            self.assertEqual(borsuk.open(uri).get_vector("far"), [9.0, 0.0])

            with self.assertRaisesRegex(borsuk.BorsukError, "record ids must not be empty"):
                index.get_vector("")
            with self.assertRaisesRegex(borsuk.BorsukError, "record ids must not be empty"):
                index.get_vector(" \t ")

    def test_binary_ids_can_be_added_searched_and_loaded_without_utf8_decoding(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = local_uri(tmp)
            index = borsuk.create(
                uri=uri,
                metric="euclidean",
                dimensions=2,
                segment_size=2,
            )
            record_id = bytes([0, 159, 255, 7])

            self.assertEqual(index.add([[0.0, 0.0]], ids=[record_id]), [record_id])
            self.assertEqual(index.search_id_bytes([0.0, 0.0], k=1), [record_id])
            self.assertEqual(index.get_vector(record_id), [0.0, 0.0])
            self.assertEqual(index.search_vectors([0.0, 0.0], k=1), [[0.0, 0.0]])
            report = index.search_with_report([0.0, 0.0], k=1)
            self.assertEqual(report.hits[0].id, "0x009fff07")
            self.assertEqual(report.hits[0].id_bytes, record_id)
            self.assertEqual(borsuk.open(uri).search_id_bytes([0.0, 0.0], k=1), [record_id])
            with self.assertRaisesRegex(borsuk.BorsukError, "valid UTF-8"):
                index.search_ids([0.0, 0.0], k=1)

    def test_integer_ids_use_compact_binary_encoding(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = local_uri(tmp)
            index = borsuk.create(
                uri=uri,
                metric="euclidean",
                dimensions=2,
                segment_size=2,
            )

            self.assertEqual(index.add([[0.0, 0.0]], ids=[300]), [300])
            self.assertEqual(index.search_id_bytes([0.0, 0.0], k=1), [bytes([0xAC, 0x02])])
            self.assertEqual(index.get_vector(300), [0.0, 0.0])
            self.assertEqual(borsuk.open(uri).get_vector(300), [0.0, 0.0])

            with self.assertRaisesRegex(ValueError, "integer record ids must be non-negative"):
                index.add([[1.0, 0.0]], ids=[-1])

    def test_search_buffer_variants_accept_contiguous_float32_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add([[0.0, 0.0], [1.0, 0.0], [9.0, 0.0]], ids=["a", "b", "c"])

            self.assertEqual(index.search_ids_buffer(array("f", [0.8, 0.0]), k=2), ["b", "a"])
            self.assertEqual(
                index.search_vectors_buffer(array("f", [0.8, 0.0]), k=2),
                [[1.0, 0.0], [0.0, 0.0]],
            )

    def test_add_buffer_accepts_contiguous_float32_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=2,
            )

            index.add_buffer(
                array("f", [0.0, 0.0, 1.0, 0.0, 9.0, 0.0]),
                ids=["a", "b", "c"],
            )
            ids = index.search_ids([0.8, 0.0], k=2)

            self.assertEqual(ids, ["b", "a"])

    def test_exact_search_does_not_prune_equal_distance_ties(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add([[1.0, 0.0], [-1.0, 0.0]], ids=["z-tie", "a-tie"])
            report = index.search_with_report([0.0, 0.0], k=1)

            self.assertEqual([hit.id for hit in report.hits], ["a-tie"])
            self.assertEqual(report.segments_searched, 2)
            self.assertEqual(report.segments_skipped, 0)

    def test_open_with_cache_reads_fresh_current_after_external_publish(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as cache:
            uri = local_uri(tmp)
            cached = borsuk.create(
                uri=uri,
                metric="euclidean",
                dimensions=2,
                segment_size=2,
                cache_dir=cache,
            )
            self.assertEqual(cached.stats().manifest_version, 1)

            writer = borsuk.open(uri)
            writer.add([[0.0, 0.0]], ids=["fresh"])
            self.assertEqual(writer.stats().manifest_version, 2)

            reopened = borsuk.open(uri, cache_dir=cache)

            self.assertEqual(reopened.stats().manifest_version, 2)
            self.assertEqual(reopened.stats().records, 1)
            self.assertEqual(reopened.search_ids([0.0, 0.0], k=1)[0], "fresh")

    def test_search_batch_variants_preserve_query_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                [[0.0, 0.0], [5.0, 0.0], [10.0, 0.0]],
                ids=["left", "middle", "right"],
            )

            self.assertEqual(
                index.search_ids_batch([[0.1, 0.0], [9.9, 0.0]], k=1),
                [["left"], ["right"]],
            )
            self.assertEqual(
                index.search_vectors_batch([[0.1, 0.0], [9.9, 0.0]], k=1),
                [[[0.0, 0.0]], [[10.0, 0.0]]],
            )

    def test_search_batch_buffer_variants_accept_contiguous_float32_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                [[0.0, 0.0], [5.0, 0.0], [10.0, 0.0]],
                ids=["left", "middle", "right"],
            )

            self.assertEqual(
                index.search_ids_batch_buffer(array("f", [0.1, 0.0, 9.9, 0.0]), k=1),
                [["left"], ["right"]],
            )
            self.assertEqual(
                index.search_vectors_batch_buffer(array("f", [0.1, 0.0, 9.9, 0.0]), k=1),
                [[[0.0, 0.0]], [[10.0, 0.0]]],
            )

    def test_search_batch_with_report_preserves_query_order_and_counters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                [[0.0, 0.0], [5.0, 0.0], [10.0, 0.0]],
                ids=["left", "middle", "right"],
            )
            reports = index.search_batch_with_report([[0.1, 0.0], [9.9, 0.0]], k=1)

            self.assertEqual([report.hits[0].id for report in reports], ["left", "right"])
            self.assertEqual([report.segments_total for report in reports], [3, 3])
            self.assertGreater(reports[0].bytes_read, 0)
            self.assertGreater(reports[1].bytes_read, 0)
            self.assertGreater(reports[0].resident_bytes_estimate, 0)
            self.assertGreater(reports[1].resident_bytes_estimate, 0)

    def test_search_batch_with_report_buffer_accepts_contiguous_float32_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                [[0.0, 0.0], [5.0, 0.0], [10.0, 0.0]],
                ids=["left", "middle", "right"],
            )
            reports = index.search_batch_with_report_buffer(
                array("f", [0.1, 0.0, 9.9, 0.0]),
                k=1,
            )

            self.assertEqual([report.hits[0].id for report in reports], ["left", "right"])
            self.assertEqual([report.segments_total for report in reports], [3, 3])
            self.assertGreater(reports[0].bytes_read, 0)
            self.assertGreater(reports[1].bytes_read, 0)

    def test_stats_expose_manifest_and_resident_budget_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = local_uri(tmp)
            index = borsuk.create(
                uri=uri,
                metric="euclidean",
                dimensions=2,
                segment_size=2,
                ram_budget="1MB",
            )

            index.add(
                [[0.0, 0.0], [1.0, 0.0], [10.0, 0.0]],
                ids=["a", "b", "c"],
            )
            stats = index.stats()

            self.assertEqual(stats.metric, "euclidean")
            self.assertEqual(stats.dimensions, 2)
            self.assertEqual(stats.segment_max_vectors, 2)
            self.assertEqual(stats.ram_budget_bytes, 1_000_000)
            self.assertEqual(stats.manifest_version, 2)
            self.assertEqual(stats.routing_max_level, 0)
            self.assertEqual(stats.routing_page_fanout, 128)
            self.assertEqual(stats.routing_leaf_pages, 1)
            self.assertEqual(stats.routing_pages, 1)
            self.assertEqual(stats.segments, 2)
            self.assertEqual(stats.records, 3)
            self.assertGreater(stats.segment_bytes, 0)
            self.assertGreater(stats.graph_bytes, 0)
            self.assertGreater(stats.resident_bytes_estimate, 0)

            reopened = borsuk.open(uri, ram_budget="500KB")
            self.assertEqual(reopened.stats().ram_budget_bytes, 500_000)

    def test_create_and_open_accept_numeric_ram_budget_byte_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = local_uri(tmp)
            index = borsuk.create(
                uri=uri,
                metric="euclidean",
                dimensions=2,
                segment_size=2,
                ram_budget=1_000_000,
            )

            self.assertEqual(index.stats().ram_budget_bytes, 1_000_000)

            reopened = borsuk.open(uri, ram_budget=500_000)
            self.assertEqual(reopened.stats().ram_budget_bytes, 500_000)

    def test_stats_expose_computed_routing_max_level(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                [[float(value), 0.0] for value in range(130)],
                ids=[f"v{value}" for value in range(130)],
            )

            stats = index.stats()
            self.assertEqual(stats.routing_page_fanout, 128)
            self.assertEqual(stats.routing_max_level, 1)
            self.assertEqual(stats.routing_leaf_pages, 2)
            self.assertEqual(stats.routing_pages, 3)

    def test_create_supports_routing_page_fanout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = local_uri(tmp)
            index = borsuk.create(
                uri=uri,
                metric="euclidean",
                dimensions=2,
                segment_size=1,
                routing_page_fanout=4,
            )

            index.add(
                [[float(value), 0.0] for value in range(17)],
                ids=[f"v{value}" for value in range(17)],
            )

            stats = index.stats()
            self.assertEqual(stats.routing_page_fanout, 4)
            self.assertEqual(stats.routing_max_level, 2)
            self.assertEqual(stats.routing_leaf_pages, 5)
            self.assertEqual(stats.routing_pages, 8)

            reopened = borsuk.open(uri, resident_routing=False)
            reopened_stats = reopened.stats()
            self.assertEqual(reopened_stats.routing_page_fanout, 4)
            self.assertEqual(reopened_stats.routing_max_level, 2)
            self.assertEqual(reopened_stats.routing_leaf_pages, 5)
            self.assertEqual(reopened_stats.routing_pages, 8)

    def test_approx_search_drills_through_deep_paged_routing_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = local_uri(tmp)
            index = borsuk.create(
                uri=uri,
                metric="euclidean",
                dimensions=2,
                segment_size=1,
                routing_page_fanout=4,
            )

            vectors = [[1000.0 + float(value), 0.0] for value in range(64)]
            vectors.append([0.0, 0.0])
            ids = [f"far-{value}" for value in range(64)]
            ids.append("near")
            index.add(vectors, ids=ids)
            stats = index.stats()
            self.assertEqual(stats.routing_page_fanout, 4)
            self.assertEqual(stats.routing_max_level, 3)

            reopened = borsuk.open(uri, resident_routing=False)
            Path(
                tmp,
                "routing",
                "layers",
                f"{stats.manifest_version:020}",
                "L0",
                "pages.parquet",
            ).write_bytes(b"corrupt global L0 routing page index that deep search must not read")

            report = reopened.search_with_report(
                [0.0, 0.0],
                k=1,
                mode=borsuk.SearchMode.APPROX,
                leaf_mode=borsuk.LeafModeName.PQ_SCAN,
                max_segments=1,
                routing_page_overfetch=1,
            )

            self.assertEqual(report.hits[0].id, "near")
            self.assertEqual(report.segments_total, 65)
            self.assertEqual(report.segments_searched, 1)
            self.assertEqual(report.routing_page_indexes_read, 1)
            self.assertEqual(report.routing_pages_read, 4)

    def test_create_enforces_ram_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError, "RAM budget exceeded"):
                borsuk.create(
                    uri=local_uri(tmp),
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
                    uri=local_uri(tmp),
                    metric="euclidean",
                    dimensions=2,
                    segment_size=1,
                    ram_budget="1B",
                )

    def test_concurrent_publish_errors_expose_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = local_uri(tmp)
            winner = borsuk.create(
                uri=uri,
                metric="euclidean",
                dimensions=2,
                segment_size=2,
            )
            loser = borsuk.open(uri)

            winner.add([[0.0, 0.0]], ids=["winner"])
            with self.assertRaises(borsuk.BorsukError) as raised:
                loser.add([[9.0, 0.0]], ids=["loser"])

            self.assertEqual(raised.exception.code, "concurrent_modification")

    def test_open_enforces_runtime_ram_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = local_uri(tmp)
            borsuk.create(uri=uri, metric="euclidean", dimensions=2, segment_size=1)

            with self.assertRaisesRegex(RuntimeError, "RAM budget exceeded"):
                borsuk.open(uri, ram_budget="1B")

    def test_open_rejects_non_boolean_resident_routing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = local_uri(tmp)
            borsuk.create(uri=uri, metric="euclidean", dimensions=2, segment_size=1)

            with self.assertRaisesRegex(ValueError, "resident_routing must be a boolean when set"):
                borsuk.open(uri, resident_routing=1)  # type: ignore[arg-type]

    def test_open_can_use_paged_routing_without_resident_segment_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = local_uri(tmp)
            index = borsuk.create(
                uri=uri,
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )
            index.add(
                [[float(value), 0.0] for value in range(130)],
                ids=[f"v{value}" for value in range(130)],
            )
            full_resident_bytes = index.stats().resident_bytes_estimate

            reopened = borsuk.open(
                uri,
                ram_budget=f"{full_resident_bytes - 1}B",
                resident_routing=False,
            )

            stats = reopened.stats()
            self.assertEqual(stats.segments, 130)
            self.assertEqual(stats.records, 130)
            self.assertLess(stats.resident_bytes_estimate, full_resident_bytes)
            report = reopened.search_with_report(
                [129.0, 0.0],
                k=1,
                mode=borsuk.SearchMode.APPROX,
                leaf_mode=borsuk.LeafModeName.PQ_SCAN,
                max_segments=1,
            )
            self.assertEqual(report.hits[0].id, "v129")
            self.assertEqual(report.segments_total, 130)
            self.assertEqual(report.segments_searched, 1)
            self.assertLess(report.resident_bytes_estimate, full_resident_bytes)

    def test_stats_propagates_corrupt_paged_routing_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uri = local_uri(tmp)
            index = borsuk.create(
                uri=uri,
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )
            index.add([[0.0, 0.0]], ids=["v0"])
            version = index.stats().manifest_version
            reopened = borsuk.open(uri, resident_routing=False)
            Path(tmp, "routing", "layers", f"{version:020}", "L0", "pages.parquet").write_bytes(
                b"corrupt paged stats routing metadata"
            )

            with self.assertRaisesRegex(RuntimeError, "(?i)parquet|routing layer page index"):
                reopened.stats()

    def test_search_with_report_exposes_query_counters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                [[0.0, 0.0], [10.0, 0.0], [20.0, 0.0]],
                ids=["near", "mid", "far"],
            )
            report = index.search_with_report([0.0, 0.0], k=1)

            self.assertEqual(report.hits[0].id, "near")
            self.assertEqual(report.leaf_mode, "flat-scan")
            self.assertEqual(report.termination_reason, "exact-pruned")
            self.assertEqual(report.segments_total, 3)
            self.assertEqual(report.segments_searched, 1)
            self.assertEqual(report.segments_skipped, 2)
            self.assertEqual(report.routing_page_indexes_read, 1)
            self.assertEqual(report.routing_pages_read, 1)
            self.assertGreater(report.bytes_read, 0)
            self.assertEqual(report.object_cache_hits, 0)
            self.assertGreater(report.object_cache_misses, 0)
            self.assertGreater(report.resident_bytes_estimate, 0)
            self.assertGreaterEqual(report.elapsed_ms, 0)

    def test_search_with_report_exposes_recall_guarantee_and_guaranteed_option(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add([[0.0, 0.0], [1.0, 0.0]], ids=["near", "far"])

            exact_report = index.search_with_report([0.0, 0.0], k=1)
            complete_report = index.search_with_report(
                [0.0, 0.0],
                k=2,
                mode="approx",
            )

            self.assertEqual(exact_report.recall_guarantee, "exact")
            self.assertEqual(complete_report.recall_guarantee, "budget-complete")

            with self.assertRaises(borsuk.BorsukError) as raised:
                index.search_with_report(
                    [0.0, 0.0],
                    k=1,
                    mode="approx",
                    max_segments=1,
                    guaranteed_recall=True,
                )
            self.assertEqual(raised.exception.code, "recall_guarantee_violated")

    def test_search_with_report_buffer_accepts_contiguous_float32_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                [[0.0, 0.0], [10.0, 0.0], [20.0, 0.0]],
                ids=["near", "mid", "far"],
            )
            report = index.search_with_report_buffer(array("f", [0.0, 0.0]), k=1)

            self.assertEqual(report.hits[0].id, "near")
            self.assertEqual(report.segments_total, 3)
            self.assertEqual(report.segments_searched, 1)
            self.assertEqual(report.segments_skipped, 2)
            self.assertGreater(report.bytes_read, 0)
            self.assertGreater(report.object_cache_misses, 0)

    def test_approx_search_limits_exact_scoring_inside_segment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=1,
                segment_size=4,
            )

            index.add(
                [[0.0], [0.2], [10.0], [20.0]],
                ids=["near", "next", "far-a", "far-b"],
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
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=1,
                segment_size=4,
            )

            index.add(
                [[0.0], [0.2], [10.0], [20.0]],
                ids=["near", "next", "far-a", "far-b"],
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

    def test_approx_flat_scan_leaf_mode_skips_segment_graph(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=1,
                segment_size=4,
            )

            index.add(
                [[0.0], [0.2], [10.0], [20.0]],
                ids=["near", "next", "far-a", "far-b"],
            )
            report = index.search_with_report(
                [0.05],
                k=1,
                mode=borsuk.SearchMode.APPROX,
                leaf_mode=borsuk.LeafModeName.FLAT_SCAN,
                max_candidates_per_segment=2,
            )

            self.assertEqual(report.leaf_mode, "flat-scan")
            self.assertEqual(report.hits[0].id, "near")
            self.assertEqual(report.records_considered, 4)
            self.assertEqual(report.records_scored, 2)
            self.assertEqual(report.graph_bytes_read, 0)
            self.assertEqual(report.graph_candidates_added, 0)

    def test_approx_sq_scan_leaf_mode_uses_routing_codes_and_skips_segment_graph(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=4,
            )

            index.add(
                [[0.0, 0.0], [0.2, 0.0], [0.0, 0.1], [100.0, 100.0]],
                ids=["entry", "routing-neighbor", "graph-neighbor", "far"],
            )
            report = index.search_with_report(
                [0.19, 0.0],
                k=1,
                mode=borsuk.SearchMode.APPROX,
                leaf_mode=borsuk.LeafModeName.SQ_SCAN,
                max_candidates_per_segment=2,
            )

            self.assertEqual(report.leaf_mode, "sq-scan")
            self.assertEqual(report.hits[0].id, "routing-neighbor")
            self.assertEqual(report.records_considered, 4)
            self.assertEqual(report.records_scored, 2)
            self.assertEqual(report.graph_bytes_read, 0)
            self.assertEqual(report.graph_candidates_added, 0)

    def test_approx_pq_scan_leaf_mode_uses_compressed_scan_and_skips_segment_graph(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=4,
            )

            index.add(
                [[0.0, 0.0], [0.2, 0.0], [0.0, 0.1], [100.0, 100.0]],
                ids=["entry", "routing-neighbor", "graph-neighbor", "far"],
            )
            report = index.search_with_report(
                [0.19, 0.0],
                k=1,
                mode=borsuk.SearchMode.APPROX,
                leaf_mode=borsuk.LeafModeName.PQ_SCAN,
                max_candidates_per_segment=2,
            )

            self.assertEqual(report.leaf_mode, "pq-scan")
            self.assertEqual(report.hits[0].id, "routing-neighbor")
            self.assertEqual(report.records_considered, 4)
            self.assertEqual(report.records_scored, 2)
            self.assertEqual(report.graph_bytes_read, 0)
            self.assertEqual(report.graph_candidates_added, 0)

    def test_approx_vamana_pq_leaf_mode_uses_segment_graph_and_reports_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=4,
            )

            index.add(
                [[0.0, 0.0], [0.0, 0.1], [0.1, -0.1], [100.0, 100.0]],
                ids=["entry", "true-neighbor", "routing-decoy", "far"],
            )
            report = index.search_with_report(
                [0.04, 0.07],
                k=1,
                mode=borsuk.SearchMode.APPROX,
                leaf_mode=borsuk.LeafModeName.VAMANA_PQ,
                max_candidates_per_segment=2,
            )

            self.assertEqual(report.leaf_mode, "vamana-pq")
            self.assertEqual(report.hits[0].id, "true-neighbor")
            self.assertEqual(report.records_considered, 4)
            self.assertEqual(report.records_scored, 2)
            self.assertGreater(report.graph_bytes_read, 0)
            self.assertEqual(report.graph_candidates_added, 1)

    def test_approx_hybrid_leaf_mode_uses_stored_segment_graph_mode_and_reports_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=4,
            )

            index.add(
                [[0.0, 0.0], [0.0, 0.1], [0.1, -0.1], [100.0, 100.0]],
                ids=["entry", "true-neighbor", "routing-decoy", "far"],
            )
            report = index.search_with_report(
                [0.04, 0.07],
                k=1,
                mode=borsuk.SearchMode.APPROX,
                leaf_mode=borsuk.LeafModeName.HYBRID,
                max_candidates_per_segment=2,
            )

            self.assertEqual(report.leaf_mode, "hybrid")
            self.assertEqual(report.hits[0].id, "true-neighbor")
            self.assertGreater(report.graph_bytes_read, 0)
            self.assertEqual(report.graph_candidates_added, 1)

    def test_local_package_search_reports_stay_subsecond(self) -> None:
        dimensions = 16
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric=borsuk.VectorMetricName.EUCLIDEAN,
                dimensions=dimensions,
                segment_size=128,
            )
            vectors = [deterministic_vector(seed, dimensions) for seed in range(1024)]
            ids = [f"doc-{seed}" for seed in range(1024)]
            index.add(vectors, ids=ids)
            query = deterministic_vector(42, dimensions)

            exact_report = index.search_with_report(query, k=10)
            approx_report = index.search_with_report(
                query,
                k=10,
                mode=borsuk.SearchMode.APPROX,
                leaf_mode=borsuk.LeafModeName.HYBRID,
                max_candidates_per_segment=32,
            )

            self.assertEqual(exact_report.hits[0].id, "doc-42")
            self.assertLess(exact_report.elapsed_ms, 1000)
            self.assertLess(approx_report.elapsed_ms, 1000)
            self.assertEqual(approx_report.leaf_mode, "hybrid")
            self.assertGreater(approx_report.bytes_read, 0)
            self.assertGreater(approx_report.graph_bytes_read, 0)
            self.assertLess(approx_report.records_scored, approx_report.records_considered)
            self.assertGreater(approx_report.resident_bytes_estimate, 0)

    def test_approx_search_obeys_byte_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                [[0.0, 0.0], [10.0, 0.0], [20.0, 0.0]],
                ids=["near", "mid", "far"],
            )
            report = index.search_with_report(
                [0.0, 0.0],
                k=3,
                mode="approx",
                max_bytes=1,
            )

            self.assertEqual([hit.id for hit in report.hits], [])
            self.assertEqual(report.segments_searched, 0)
            self.assertEqual(report.segments_skipped, 3)
            self.assertGreater(report.bytes_read, 1)
            self.assertEqual(report.termination_reason, "max-bytes")

    def test_approx_search_accepts_byte_budget_string(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                [[0.0, 0.0], [10.0, 0.0], [20.0, 0.0]],
                ids=["near", "mid", "far"],
            )
            report = index.search_with_report(
                [0.0, 0.0],
                k=1,
                mode="approx",
                max_bytes="1MiB",
            )

            self.assertEqual([hit.id for hit in report.hits], ["near"])
            self.assertEqual(report.segments_searched, 3)
            self.assertEqual(report.segments_skipped, 0)
            self.assertEqual(report.termination_reason, "complete")

    def test_approx_search_rejects_invalid_budgets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )
            index.add([[0.0, 0.0]], ids=["near"])

            for kwargs, expected in [
                ({"eps": -0.1}, "eps must be finite and non-negative when set"),
                ({"eps": float("nan")}, "eps must be finite and non-negative when set"),
                ({"max_segments": 0}, "max_segments must be greater than zero when set"),
                ({"max_bytes": 0}, "max_bytes must be greater than zero when set"),
                ({"max_latency_ms": 0}, "max_latency_ms must be greater than zero when set"),
                (
                    {"routing_page_overfetch": 0},
                    "routing_page_overfetch must be greater than zero when set",
                ),
                (
                    {"max_candidates_per_segment": 0},
                    "max_candidates_per_segment must be greater than zero when set",
                ),
            ]:
                with self.subTest(kwargs=kwargs):
                    with self.assertRaisesRegex(RuntimeError, expected):
                        index.search_with_report([0.0, 0.0], k=1, mode="approx", **kwargs)

            for kwargs, expected in [
                ({"max_segments": 1.5}, "max_segments must be an integer when set"),
                ({"max_bytes": 1.5}, "max_bytes must be an integer when set"),
                ({"max_latency_ms": float("nan")}, "max_latency_ms must be an integer when set"),
                (
                    {"routing_page_overfetch": True},
                    "routing_page_overfetch must be an integer when set",
                ),
                (
                    {"max_candidates_per_segment": 1.5},
                    "max_candidates_per_segment must be an integer when set",
                ),
            ]:
                with self.subTest(kwargs=kwargs):
                    with self.assertRaisesRegex(ValueError, expected):
                        index.search_with_report([0.0, 0.0], k=1, mode="approx", **kwargs)

    def test_search_rejects_invalid_mode_option_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )
            index.add([[0.0, 0.0]], ids=["near"])

            with self.assertRaisesRegex(ValueError, "mode must be a string when set"):
                index.search_with_report(
                    [0.0, 0.0],
                    k=1,
                    mode=True,  # type: ignore[arg-type]
                )
            with self.assertRaisesRegex(ValueError, "leaf_mode must be a string when set"):
                index.search_with_report(
                    [0.0, 0.0],
                    k=1,
                    mode="approx",
                    leaf_mode=True,  # type: ignore[arg-type]
                )
            with self.assertRaisesRegex(ValueError, "unknown search mode `not-a-mode`"):
                index.search_with_report(
                    [0.0, 0.0],
                    k=1,
                    mode="not-a-mode",  # type: ignore[arg-type]
                )
            with self.assertRaisesRegex(ValueError, "unknown leaf mode `not-a-leaf`"):
                index.search_with_report(
                    [0.0, 0.0],
                    k=1,
                    mode="approx",
                    leaf_mode="not-a-leaf",  # type: ignore[arg-type]
                )

    def test_search_rejects_zero_k(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )
            index.add([[0.0, 0.0]], ids=["near"])

            with self.assertRaisesRegex(RuntimeError, "k must be greater than zero"):
                index.search_ids([0.0, 0.0], k=0)
            with self.assertRaisesRegex(ValueError, "k must be an integer"):
                index.search_ids([0.0, 0.0], k=1.5)  # type: ignore[arg-type]
            with self.assertRaisesRegex(ValueError, "k must be an integer"):
                index.search_ids([0.0, 0.0], k=True)  # type: ignore[arg-type]
            with self.assertRaisesRegex(RuntimeError, "k must be greater than zero"):
                index.search_with_report([0.0, 0.0], k=0, mode="approx")
            with self.assertRaisesRegex(ValueError, "k must be an integer"):
                index.search_with_report(
                    [0.0, 0.0],
                    k=1.5,  # type: ignore[arg-type]
                    mode="approx",
                )

    def test_approx_search_expands_segment_graph_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=4,
            )

            index.add(
                [[0.0, 0.0], [0.0, 0.1], [0.1, -0.1], [100.0, 100.0]],
                ids=["entry", "true-neighbor", "routing-decoy", "far"],
            )
            report = index.search_with_report(
                [0.04, 0.07],
                k=1,
                mode="approx",
                max_candidates_per_segment=2,
            )

            self.assertEqual(report.hits[0].id, "true-neighbor")
            self.assertEqual(report.leaf_mode, "graph")
            self.assertEqual(report.records_considered, 4)
            self.assertEqual(report.records_scored, 2)
            self.assertGreater(report.graph_bytes_read, 0)
            self.assertEqual(report.graph_candidates_added, 1)

    def test_approx_search_walks_segment_graph_beyond_first_hop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=10,
            )

            index.add(
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
                ids=[
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
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=4,
            )

            writer.add(
                [[0.0, 0.0], [0.0, 0.1], [0.1, -0.1], [100.0, 100.0]],
                ids=["entry", "true-neighbor", "routing-decoy", "far"],
            )
            index = borsuk.open(local_uri(tmp), cache_dir=cache)
            report = index.search_with_report(
                [0.04, 0.07],
                k=1,
                mode="approx",
                max_candidates_per_segment=2,
            )

            self.assertEqual(report.hits[0].id, "true-neighbor")
            self.assertGreater(report.graph_bytes_read, 0)
            self.assertEqual(report.object_cache_hits, 0)
            self.assertEqual(report.object_cache_misses, 4)
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
                [[0.0, 0.0], [0.0, 0.1], [0.1, -0.1], [100.0, 100.0]],
                ids=["entry", "true-neighbor", "routing-decoy", "far"],
            )
            reopened = borsuk.open(uri, cache_dir=cache)
            report = reopened.search_with_report(
                [0.04, 0.07],
                k=1,
                mode="approx",
                max_candidates_per_segment=2,
            )

            self.assertEqual(report.hits[0].id, "true-neighbor")
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

            gc = reopened.gc_obsolete_segments(min_age_seconds=0)
            self.assertTrue(gc.dry_run)
            self.assertGreater(len(gc.candidates), 0)

    def test_compact_rewrites_segments_and_reports_counters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                [[0.0, 0.0], [1.0, 0.0], [8.0, 0.0], [9.0, 0.0]],
                ids=["a", "b", "c", "d"],
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
            self.assertEqual(report.routing_page_indexes_read, 1)
            self.assertEqual(report.routing_pages_read, 1)
            self.assertGreaterEqual(report.routing_page_indexes_written, 1)
            self.assertGreaterEqual(report.routing_pages_written, 1)
            self.assertEqual(report.graph_payloads_read, 0)
            self.assertEqual(report.graph_bytes_read, 0)
            self.assertEqual(report.object_cache_hits, 0)
            self.assertEqual(report.object_cache_misses, 6)

            after = index.search_with_report([8.5, 0.0], k=2)
            self.assertEqual(after.segments_total, 2)
            self.assertEqual([hit.id for hit in after.hits], ["c", "d"])

    def test_compact_default_uses_bounded_source_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )
            index.add(
                [[float(value), 0.0] for value in range(34)],
                ids=[f"v{value}" for value in range(34)],
            )

            report = index.compact(min_segments=1, target_segment_max_vectors=1)

            self.assertTrue(report.compacted)
            self.assertEqual(report.segments_read, 32)
            self.assertEqual(report.records_rewritten, 32)
            self.assertEqual(index.stats().segments, 34)
            self.assertEqual(index.get_vector("v33"), [33.0, 0.0])

    def test_compact_rejects_impossible_batch_thresholds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            with self.assertRaisesRegex(
                RuntimeError,
                "min_segments must be less than or equal to max_segments when max_segments is set",
            ):
                index.compact(max_segments=1, min_segments=2)

    def test_compact_rejects_non_integer_options(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            for kwargs, expected in [
                ({"source_level": 0.5}, "source_level must be an integer when set"),
                ({"target_level": 1.5}, "target_level must be an integer when set"),
                ({"max_segments": 1.5}, "max_segments must be an integer when set"),
                ({"min_segments": float("nan")}, "min_segments must be an integer when set"),
                (
                    {"target_segment_max_vectors": True},
                    "target_segment_max_vectors must be an integer when set",
                ),
            ]:
                with self.subTest(kwargs=kwargs):
                    with self.assertRaisesRegex(ValueError, expected):
                        index.compact(**kwargs)

    def test_compact_rejects_non_boolean_all_matching(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            with self.assertRaisesRegex(ValueError, "all_matching must be a boolean when set"):
                index.compact(all_matching=1)  # type: ignore[arg-type]

    def test_rebuild_compacts_all_matching_segments_and_deletes_obsolete_objects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )
            index.add(
                [[0.0, 0.0], [1.0, 0.0], [8.0, 0.0], [9.0, 0.0]],
                ids=["a", "b", "c", "d"],
            )

            report = index.rebuild(
                source_level=0,
                target_level=1,
                min_segments=1,
                target_segment_max_vectors=2,
                delete_obsolete=True,
            )

            self.assertTrue(report.compaction.compacted)
            self.assertEqual(report.compaction.segments_read, 4)
            self.assertEqual(report.compaction.segments_written, 2)
            self.assertFalse(report.garbage_collection.dry_run)
            self.assertEqual(report.garbage_collection.objects_deleted, 17)
            self.assertEqual(report.garbage_collection.routing_objects_deleted, 3)
            self.assertEqual(report.garbage_collection.tables_deleted, 6)
            self.assertEqual(len(report.garbage_collection.candidates), 17)
            self.assertEqual(index.search_ids([8.5, 0.0], k=2), ["c", "d"])

    def test_rebuild_rejects_non_integer_options(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            for kwargs, expected in [
                ({"source_level": 0.5}, "source_level must be an integer when set"),
                ({"target_level": 1.5}, "target_level must be an integer when set"),
                ({"min_segments": float("nan")}, "min_segments must be an integer when set"),
                (
                    {"target_segment_max_vectors": True},
                    "target_segment_max_vectors must be an integer when set",
                ),
            ]:
                with self.subTest(kwargs=kwargs):
                    with self.assertRaisesRegex(ValueError, expected):
                        index.rebuild(**kwargs)

    def test_rebuild_rejects_non_boolean_delete_obsolete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            with self.assertRaisesRegex(ValueError, "delete_obsolete must be a boolean when set"):
                index.rebuild(delete_obsolete=1)  # type: ignore[arg-type]

    def test_gc_obsolete_segments_dry_runs_and_deletes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            index.add(
                [[0.0, 0.0], [1.0, 0.0], [8.0, 0.0], [9.0, 0.0]],
                ids=["a", "b", "c", "d"],
            )
            index.compact(target_segment_max_vectors=2)

            dry_run = index.gc_obsolete_segments(min_age_seconds=0)
            self.assertTrue(dry_run.dry_run)
            self.assertEqual(dry_run.objects_scanned, 26)
            self.assertEqual(dry_run.objects_deleted, 0)
            self.assertEqual(dry_run.routing_objects_deleted, 0)
            self.assertEqual(dry_run.tables_deleted, 0)
            self.assertEqual(dry_run.routing_page_indexes_read, 1)
            self.assertEqual(dry_run.routing_pages_read, 1)
            self.assertGreater(dry_run.bytes_read, 0)
            self.assertEqual(dry_run.object_cache_hits, 0)
            self.assertEqual(dry_run.object_cache_misses, 2)
            self.assertEqual(len(dry_run.candidates), 17)
            self.assertGreater(dry_run.bytes_reclaimable, 0)

            # Repo-policy anchor for the delete path: gc_obsolete_segments(dry_run=False).
            deleted = index.gc_obsolete_segments(dry_run=False, min_age_seconds=0)
            self.assertFalse(deleted.dry_run)
            self.assertEqual(deleted.objects_deleted, 17)
            self.assertEqual(deleted.routing_objects_deleted, 3)
            self.assertEqual(deleted.tables_deleted, 6)
            self.assertEqual(deleted.routing_page_indexes_read, 1)
            self.assertEqual(deleted.routing_pages_read, 1)
            self.assertGreater(deleted.bytes_read, 0)
            self.assertEqual(deleted.object_cache_hits, 0)
            self.assertEqual(deleted.object_cache_misses, 2)
            self.assertEqual(deleted.candidates, dry_run.candidates)
            self.assertEqual(deleted.bytes_reclaimed, dry_run.bytes_reclaimable)

            self.assertEqual(index.search_ids([8.5, 0.0], k=2), ["c", "d"])

    def test_gc_obsolete_segments_rejects_non_boolean_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
            )

            with self.assertRaisesRegex(ValueError, "dry_run must be a boolean when set"):
                index.gc_obsolete_segments(dry_run=1)  # type: ignore[arg-type]
            with self.assertRaisesRegex(
                ValueError, "min_age_seconds must be a non-negative finite number"
            ):
                index.gc_obsolete_segments(min_age_seconds=-1)

    def test_gc_obsolete_segments_removes_cached_inactive_objects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as cache:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric="euclidean",
                dimensions=2,
                segment_size=1,
                cache_dir=cache,
            )

            index.add(
                [[0.0, 0.0], [1.0, 0.0], [8.0, 0.0], [9.0, 0.0]],
                ids=["a", "b", "c", "d"],
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

            deleted = index.gc_obsolete_segments(dry_run=False, min_age_seconds=0)

            self.assertEqual(deleted.objects_deleted, 17)
            self.assertEqual(deleted.routing_objects_deleted, 3)
            self.assertEqual(deleted.tables_deleted, 6)
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
            index = borsuk.create(uri=local_uri(tmp), metric="euclidean", dim=1)
            with self.assertRaises(ValueError):
                index.add([[0.0], [1.0]], ids=["a"])


if __name__ == "__main__":
    unittest.main()
