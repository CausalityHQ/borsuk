"""S3-compatible BORSUK example using the native Python API.

Set BORSUK_S3_TEST_URI to an S3-compatible bucket/prefix, for example:

    BORSUK_S3_TEST_URI=s3://borsuk-test/indexes python examples/s3_index.py
"""

from __future__ import annotations

import os
import tempfile
import uuid

import borsuk


def main() -> None:
    base_uri = os.environ.get("BORSUK_S3_TEST_URI")
    if not base_uri:
        raise SystemExit("set BORSUK_S3_TEST_URI=s3://bucket/prefix before running this example")

    uri = f"{base_uri.rstrip('/')}/python-example-{uuid.uuid4()}"
    with tempfile.TemporaryDirectory(prefix="borsuk-py-s3-cache-") as cache:
        index = borsuk.create(
            uri=uri,
            metric=borsuk.VectorMetricName.EUCLIDEAN,
            dimensions=2,
            segment_size=3,
        )

        index.add(
            [[0.0, 0.0], [0.0, 0.1], [0.1, -0.1], [100.0, 100.0], [110.0, 100.0], [100.0, 110.0]],
            ids=["entry", "true-neighbor", "routing-decoy", "far", "far2", "far3"],
        )

        reopened = borsuk.open(uri, cache_dir=cache)
        report = reopened.search_with_report(
            [0.04, 0.07],
            k=1,
            mode=borsuk.SearchMode.APPROX,
            leaf_mode=borsuk.LeafModeName.GRAPH,
            max_candidates_per_segment=2,
        )
        assert report.hits[0].id == "true-neighbor"
        vector = reopened.get_vector("true-neighbor")
        assert vector is not None
        assert [round(value, 6) for value in vector] == [0.0, 0.1]
        assert report.bytes_read > 0
        assert report.graph_bytes_read > 0
        assert report.object_cache_misses > 0

        compaction = reopened.compact(
            source_level=0,
            target_level=1,
            max_segments=2,
            min_segments=2,
            target_segment_max_vectors=6,
        )
        assert compaction.compacted

        gc = reopened.gc_obsolete_segments(min_age_seconds=0)
        assert gc.dry_run
        assert gc.candidates

        print(
            f"uri={uri} hit={report.hits[0].id} "
            f"bytes_read={report.bytes_read} graph_bytes_read={report.graph_bytes_read} "
            f"object_cache_misses={report.object_cache_misses} "
            f"compacted={compaction.compacted} gc_candidates={len(gc.candidates)}"
        )


if __name__ == "__main__":
    main()
