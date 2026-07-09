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
    rung_tuning()
    rung_production()
    print("docs ladder ok")


if __name__ == "__main__":
    main()
