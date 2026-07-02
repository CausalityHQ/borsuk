"""Local-file BORSUK example using the native Python API."""

from __future__ import annotations

import tempfile
from array import array
from pathlib import Path

import borsuk


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="borsuk-py-index-") as root:
        index = borsuk.create(
            uri=Path(root).as_uri(),
            metric=borsuk.VectorMetricName.COSINE,
            dimensions=3,
            segment_size=2,
        )

        index.add(
            [
                [1.0, 0.0, 0.0],
                [0.9, 0.1, 0.0],
                [0.0, 1.0, 0.0],
            ],
            ids=["alpha", "beta", "gamma"],
        )
        stats = index.stats()
        assert stats.metric == "cosine"
        assert stats.dimensions == 3
        assert stats.segments == 2
        assert stats.records == 3
        assert stats.segment_bytes > 0
        assert stats.graph_bytes > 0

        report = index.search_with_report(
            [1.0, 0.0, 0.0],
            k=2,
            mode=borsuk.SearchMode.APPROX,
            leaf_mode=borsuk.LeafModeName.GRAPH,
            max_candidates_per_segment=2,
        )
        ids = [hit.id for hit in report.hits]
        assert ids == ["alpha", "beta"], ids
        assert report.leaf_mode == "graph"
        assert report.graph_bytes_read > 0
        vamana_pq_report = index.search_with_report(
            [1.0, 0.0, 0.0],
            k=2,
            mode=borsuk.SearchMode.APPROX,
            leaf_mode=borsuk.LeafModeName.VAMANA_PQ,
            max_candidates_per_segment=2,
        )
        assert [hit.id for hit in vamana_pq_report.hits] == ids
        assert vamana_pq_report.leaf_mode == "vamana-pq"
        assert vamana_pq_report.graph_bytes_read > 0
        hybrid_report = index.search_with_report(
            [1.0, 0.0, 0.0],
            k=2,
            mode=borsuk.SearchMode.APPROX,
            leaf_mode=borsuk.LeafModeName.HYBRID,
            max_candidates_per_segment=2,
        )
        assert [hit.id for hit in hybrid_report.hits] == ids
        assert hybrid_report.leaf_mode == "hybrid"
        assert hybrid_report.graph_bytes_read > 0
        pq_report = index.search_with_report(
            [1.0, 0.0, 0.0],
            k=2,
            mode=borsuk.SearchMode.APPROX,
            leaf_mode=borsuk.LeafModeName.PQ_SCAN,
            max_candidates_per_segment=2,
        )
        assert [hit.id for hit in pq_report.hits] == ids
        assert pq_report.leaf_mode == "pq-scan"
        assert pq_report.graph_bytes_read == 0
        sq_report = index.search_with_report(
            [1.0, 0.0, 0.0],
            k=2,
            mode=borsuk.SearchMode.APPROX,
            leaf_mode=borsuk.LeafModeName.SQ_SCAN,
            max_candidates_per_segment=2,
        )
        assert [hit.id for hit in sq_report.hits] == ids
        assert sq_report.leaf_mode == "sq-scan"
        assert sq_report.graph_bytes_read == 0
        buffer_ids = index.search_ids_buffer(array("f", [1.0, 0.0, 0.0]), k=2)
        assert buffer_ids == ids
        buffer_report = index.search_with_report_buffer(
            array("f", [1.0, 0.0, 0.0]),
            k=2,
            mode=borsuk.SearchMode.APPROX,
            max_candidates_per_segment=2,
        )
        assert [hit.id for hit in buffer_report.hits] == ids
        assert buffer_report.bytes_read > 0
        assert index.search_ids([1.0, 0.0, 0.0], k=2) == ids
        vectors = index.search_vectors([1.0, 0.0, 0.0], k=2)
        assert [[round(value, 6) for value in vector] for vector in vectors] == [
            [1.0, 0.0, 0.0],
            [0.9, 0.1, 0.0],
        ]
        beta = index.get_vector("beta")
        assert beta is not None
        assert [round(value, 6) for value in beta] == [0.9, 0.1, 0.0]
        assert report.bytes_read > 0
        batch = index.search_ids_batch(
            [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]],
            k=1,
        )
        assert batch == [["alpha"], ["gamma"]]
        buffer_batch = index.search_ids_batch_buffer(
            array("f", [1.0, 0.0, 0.0, 0.0, 1.0, 0.0]),
            k=1,
        )
        assert buffer_batch == [["alpha"], ["gamma"]]
        batch_reports = index.search_batch_with_report(
            [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]],
            k=1,
        )
        assert [batch_report.hits[0].id for batch_report in batch_reports] == [
            "alpha",
            "gamma",
        ]
        assert all(batch_report.bytes_read > 0 for batch_report in batch_reports)
        buffer_batch_reports = index.search_batch_with_report_buffer(
            array("f", [1.0, 0.0, 0.0, 0.0, 1.0, 0.0]),
            k=1,
        )
        assert [batch_report.hits[0].id for batch_report in buffer_batch_reports] == [
            "alpha",
            "gamma",
        ]
        assert all(batch_report.bytes_read > 0 for batch_report in buffer_batch_reports)

        assert "cosine" in borsuk.vector_metric_names()
        assert borsuk.LeafModeName.SQ_SCAN.value in borsuk.leaf_mode_names()
        assert borsuk.LeafModeName.PQ_SCAN.value in borsuk.leaf_mode_names()
        assert borsuk.LeafModeName.GRAPH.value in borsuk.leaf_mode_names()
        assert borsuk.LeafModeName.VAMANA_PQ.value in borsuk.leaf_mode_names()
        assert borsuk.LeafModeName.HYBRID.value in borsuk.leaf_mode_names()
        cosine = borsuk.vector_distance(borsuk.VectorMetricName.COSINE, [1.0, 0.0], [1.0, 0.0])
        recall = borsuk.recall_at_k(["alpha", "beta"], ids, 2)
        assert cosine == 0.0
        assert recall == 1.0

        print(
            f"hits={ids} bytes_read={report.bytes_read} "
            f"pq_hits={[hit.id for hit in pq_report.hits]} "
            f"hybrid_hits={[hit.id for hit in hybrid_report.hits]} "
            f"recall_at_2={recall} "
            f"object_cache_hits={report.object_cache_hits} "
            f"object_cache_misses={report.object_cache_misses} "
            f"records_scored={report.records_scored} "
            f"resident_bytes_estimate={report.resident_bytes_estimate} "
            f"segment_bytes={stats.segment_bytes}"
        )


if __name__ == "__main__":
    main()
