"""Local-file BORSUK example using the native Python API."""

from __future__ import annotations

import tempfile
from array import array

import borsuk


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="borsuk-py-index-") as root:
        index = borsuk.create(
            uri=f"file://{root}",
            metric="cosine",
            dimensions=3,
            segment_size=2,
        )

        index.add(
            ["alpha", "beta", "gamma"],
            [
                [1.0, 0.0, 0.0],
                [0.9, 0.1, 0.0],
                [0.0, 1.0, 0.0],
            ],
            payload_refs=[
                "objects/alpha.parquet",
                None,
                "objects/gamma.parquet",
            ],
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
            mode="approx",
            max_candidates_per_segment=2,
        )
        ids = [hit.id for hit in report.hits]
        assert ids == ["alpha", "beta"], ids
        buffer_report = index.search_with_report_buffer(
            array("f", [1.0, 0.0, 0.0]),
            k=2,
            mode="approx",
            max_candidates_per_segment=2,
        )
        assert [hit.id for hit in buffer_report.hits] == ids
        assert buffer_report.bytes_read > 0
        assert [hit.payload_ref for hit in report.hits] == [
            "objects/alpha.parquet",
            None,
        ]
        assert report.bytes_read > 0
        batch = index.search_batch(
            [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]],
            k=1,
        )
        assert [[hit.id for hit in hits] for hits in batch] == [["alpha"], ["gamma"]]
        buffer_batch = index.search_batch_buffer(
            array("f", [1.0, 0.0, 0.0, 0.0, 1.0, 0.0]),
            k=1,
        )
        assert [[hit.id for hit in hits] for hits in buffer_batch] == [["alpha"], ["gamma"]]
        batch_reports = index.search_batch_with_report(
            [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]],
            k=1,
        )
        assert [batch_report.hits[0].id for batch_report in batch_reports] == [
            "alpha",
            "gamma",
        ]
        assert all(batch_report.bytes_read > 0 for batch_report in batch_reports)

        cosine = borsuk.vector_distance("cosine", [1.0, 0.0], [1.0, 0.0])
        edit = borsuk.string_distance("jaro-winkler", "segment", "segments")
        recall = borsuk.recall_at_k(["alpha", "beta"], ids, 2)
        assert cosine == 0.0
        assert 0.0 < edit < 0.2
        assert recall == 1.0

        print(
            f"hits={ids} bytes_read={report.bytes_read} "
            f"payload_ref={report.hits[0].payload_ref} "
            f"recall_at_2={recall} "
            f"object_cache_hits={report.object_cache_hits} "
            f"object_cache_misses={report.object_cache_misses} "
            f"records_scored={report.records_scored} "
            f"resident_bytes_estimate={report.resident_bytes_estimate} "
            f"segment_bytes={stats.segment_bytes}"
        )


if __name__ == "__main__":
    main()
