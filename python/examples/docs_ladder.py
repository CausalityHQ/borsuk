"""The example ladder shown on the docs site, from a first search to production.

Every snippet the website renders is extracted verbatim from the ``docs:`` marker
regions below, and this example runs in CI, so the code on the page always works.
Keep the marker regions self-contained and copy-pasteable; put throwaway setup
(temp directories, cleanup) outside the markers.
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import borsuk


def rung_hello() -> None:
    root = tempfile.mkdtemp(prefix="borsuk-ladder-hello-")
    try:
        # docs:hello:start
        # Create an index. It lives entirely as files under `uri` — a local path
        # here, or an `s3://…` URI for object storage. Nothing else to run.
        index = borsuk.create(
            uri=Path(root).as_uri(),
            metric=borsuk.VectorMetricName.EUCLIDEAN,
            dimensions=3,
            segment_size=4096,
        )

        # Add a few vectors with your own ids.
        index.add(
            [[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.0, 5.0, 0.0]],
            ids=["alpha", "beta", "gamma"],
        )

        # Ask for the 2 nearest neighbours. `k` with exact mode returns the true top-k.
        ids = index.search_ids([0.1, 0.0, 0.0], k=2)
        assert ids == ["alpha", "beta"]
        print("nearest:", ids)
        # docs:hello:end
    finally:
        shutil.rmtree(root, ignore_errors=True)


def rung_report() -> None:
    root = tempfile.mkdtemp(prefix="borsuk-ladder-report-")
    try:
        index = borsuk.create(
            uri=Path(root).as_uri(),
            metric=borsuk.VectorMetricName.EUCLIDEAN,
            dimensions=3,
            segment_size=4096,
        )
        index.add(
            [[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.0, 5.0, 0.0]],
            ids=["alpha", "beta", "gamma"],
        )

        # docs:report:start
        # `search_with_report` returns the hits plus everything the query touched:
        # bytes read, segments searched, and the object-store requests it issued.
        report = index.search_with_report(
            [0.1, 0.0, 0.0], k=2, mode=borsuk.SearchMode.EXACT
        )
        print(
            f"hits={[hit.id for hit in report.hits]} "
            f"bytes_read={report.bytes_read} "
            f"segments_searched={report.segments_searched} "
            f"requests={report.requests.total} "
            f"(gets={report.requests.gets}, heads={report.requests.heads})"
        )
        # docs:report:end
    finally:
        shutil.rmtree(root, ignore_errors=True)


def rung_filter() -> None:
    root = tempfile.mkdtemp(prefix="borsuk-ladder-filter-")
    try:
        index = borsuk.create(
            uri=Path(root).as_uri(),
            metric=borsuk.VectorMetricName.EUCLIDEAN,
            dimensions=2,
            segment_size=4096,
        )
        # docs:filter:start
        # Attach schemaless metadata to any vector, then constrain a search with a
        # Pinecone-style operator dict. The filter is applied *before* ranking, so
        # a selective filter is fast and exact — whole segments that cannot match
        # are skipped unread.
        index.add(
            [[0.0, 0.0], [0.1, 0.0], [0.2, 0.0]],
            ids=["a", "b", "c"],
            metadata=[{"genre": "comedy"}, {"genre": "drama"}, {"genre": "comedy"}],
        )
        report = index.search_with_report(
            [0.0, 0.0],
            k=5,
            filter={"genre": {"$eq": "comedy"}},
            include_metadata=True,
        )
        ids = [hit.id for hit in report.hits]
        assert ids == ["a", "c"]
        print("filtered (genre=comedy):", ids)
        # docs:filter:end
    finally:
        shutil.rmtree(root, ignore_errors=True)


def rung_upsert() -> None:
    root = tempfile.mkdtemp(prefix="borsuk-ladder-upsert-")
    try:
        index = borsuk.create(
            uri=Path(root).as_uri(),
            metric=borsuk.VectorMetricName.EUCLIDEAN,
            dimensions=2,
            segment_size=4096,
        )
        # docs:upsert:start
        # `add` is insert-only; `upsert` inserts-or-replaces by id in one atomic
        # publish. Reads immediately see only the new version, and there is only
        # ever one live copy of an id — the superseded one is reclaimed by
        # compaction.
        index.add([[0.0, 0.0], [1.0, 0.0]], ids=["a", "b"])
        index.upsert([[0.0, 9.0]], ids=["a"])  # move "a" away from the origin

        near_origin = index.search_ids([0.0, 0.0], k=3)
        assert near_origin[0] == "b"  # "a" is now far from the origin
        assert near_origin.count("a") == 1
        print("after upsert, nearest origin:", near_origin)
        # docs:upsert:end
    finally:
        shutil.rmtree(root, ignore_errors=True)


def rung_hybrid() -> None:
    root = tempfile.mkdtemp(prefix="borsuk-ladder-hybrid-")
    try:
        index = borsuk.create(
            uri=Path(root).as_uri(),
            metric=borsuk.VectorMetricName.EUCLIDEAN,
            dimensions=2,
            text=True,
        )
        # docs:hybrid:start
        # Turn on `text` to index BM25 alongside the vectors, then fuse both legs
        # in one query. Reciprocal-rank fusion (the default) needs no tuning;
        # switch to weighted fusion when you want to lean on one leg.
        index.add(
            [[0.0, 0.0], [1.0, 0.0], [0.0, 1.0]],
            ids=["a", "b", "c"],
            text=["red apple", "green apple pie", "blue sky"],
        )
        hits = index.search_hybrid(vectors={"": [0.0, 0.0]}, text="apple", k=3)
        assert hits
        print("hybrid (dense + text):", hits)
        # docs:hybrid:end
    finally:
        shutil.rmtree(root, ignore_errors=True)


def rung_tuning() -> None:
    root = tempfile.mkdtemp(prefix="borsuk-ladder-tuning-")
    try:
        index = borsuk.create(
            uri=Path(root).as_uri(),
            metric=borsuk.VectorMetricName.EUCLIDEAN,
            dimensions=3,
            segment_size=2,
        )
        index.add(
            [[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.0, 5.0, 0.0], [9.0, 0.0, 0.0]],
            ids=["alpha", "beta", "gamma", "delta"],
        )

        # docs:tuning:start
        # Approximate search spends three explicit budgets instead of hidden magic:
        # how many segments to read, how much routing metadata to look ahead, and
        # how many rows to exact-score per segment. Tighten budgets while watching
        # the report — smaller budgets read less but can lower recall.
        query = [0.1, 0.0, 0.0]
        cheap = index.search_with_report(
            query,
            k=2,
            mode=borsuk.SearchMode.APPROX,
            leaf_mode=borsuk.LeafModeName.PQ_SCAN,
            max_segments=1,
            max_candidates_per_segment=2,
        )
        thorough = index.search_with_report(
            query,
            k=2,
            mode=borsuk.SearchMode.APPROX,
            leaf_mode=borsuk.LeafModeName.PQ_SCAN,
            max_segments=8,
            routing_page_overfetch=8,
        )
        print(
            f"cheap: {cheap.segments_searched} segments, {cheap.bytes_read} bytes | "
            f"thorough: {thorough.segments_searched} segments, {thorough.bytes_read} bytes"
        )
        # docs:tuning:end
    finally:
        shutil.rmtree(root, ignore_errors=True)


def rung_production() -> None:
    root = tempfile.mkdtemp(prefix="borsuk-ladder-production-")
    cache = tempfile.mkdtemp(prefix="borsuk-ladder-cache-")
    try:
        uri = Path(root).as_uri()
        index = borsuk.create(
            uri=uri,
            metric=borsuk.VectorMetricName.EUCLIDEAN,
            dimensions=3,
            segment_size=4096,
        )
        index.add(
            [[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.0, 5.0, 0.0]],
            ids=["alpha", "beta", "gamma"],
        )

        # docs:production:start
        # Open for serving. Paged routing (the default) keeps resident memory near
        # zero; a local `cache_dir` keeps fetched objects on fast disk. Every report
        # carries the object-store requests it issued, so you can chart
        # requests-per-query straight from production traffic.
        index = borsuk.open(uri, cache_dir=cache)
        report = index.search_with_report(
            [0.1, 0.0, 0.0],
            k=2,
            mode=borsuk.SearchMode.APPROX,
            leaf_mode=borsuk.LeafModeName.PQ_SCAN,
        )
        print(
            f"requests/query: {report.requests.total} "
            f"(gets={report.requests.gets}, heads={report.requests.heads}, "
            f"lists={report.requests.lists})"
        )
        # docs:production:end
    finally:
        shutil.rmtree(root, ignore_errors=True)
        shutil.rmtree(cache, ignore_errors=True)


def main() -> None:
    rung_hello()
    rung_report()
    rung_filter()
    rung_upsert()
    rung_hybrid()
    rung_tuning()
    rung_production()
    print("docs ladder ok")


if __name__ == "__main__":
    main()
