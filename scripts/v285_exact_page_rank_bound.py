"""Exact-row page-ranking bound with the frozen V284 physical scheduler."""

import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np

from scripts.v284_page_primary_dev64 import covered_pages

RAW_SHA = "0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e"
LAYOUT_SHA = "303f31ab8a182a0aaa304c4ef551a046be41071ac24e67a793882eb74c5b532e"
REQUESTS_SHA = "1de0122f73d1b72e54498640b9701ce6d156b513629596447580c85fac302ba4"
TRUTH_SHA = "f6630d0edf06539752c3fbf129ae01e58d3a3cf7b6aefa4decaa9c979e8ba355"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def evaluate(raw, layout, requests, truth):
    assert [digest(p) for p in (raw, layout, requests, truth)] == [
        RAW_SHA, LAYOUT_SHA, REQUESTS_SHA, TRUTH_SHA]
    vectors = np.memmap(raw, dtype="<f4", mode="r", shape=(100_000, 768))
    order = np.load(layout, allow_pickle=False)
    assert order.shape == (100_000,) and np.array_equal(np.sort(order), np.arange(100_000))
    queries = [json.loads(line) for line in requests.read_text().splitlines()]
    assert len(queries) == 64 and [q["query_ordinal"] for q in queries] == list(range(64))
    q = np.asarray([row["query"] for row in queries], dtype=np.float32)
    assert q.shape == (64, 768) and np.isfinite(q).all()
    query_norms = np.linalg.norm(q, axis=1)
    assert (query_norms > 0).all()
    q /= query_norms[:, None]
    scores = np.full((64, 391), -np.inf, dtype=np.float32)
    for first in range(0, 100_000, 4096):
        stop = min(first + 4096, 100_000)
        rows = np.asarray(vectors[order[first:stop]], dtype=np.float32)
        norms = np.linalg.norm(rows, axis=1)
        assert np.isfinite(rows).all() and (norms > 0).all()
        rows /= norms[:, None]
        similarities = q @ rows.T
        for page_first in range(first, stop, 256):
            page_stop = min(page_first + 256, stop)
            scores[:, page_first // 256] = similarities[:, page_first - first:page_stop - first].max(axis=1)
    gt = np.fromfile(truth, dtype="<u4").reshape(64, 100)
    assert (gt < 100_000).all()
    page_of = np.empty(100_000, dtype=np.int32)
    page_of[order] = np.arange(100_000) // 256
    hits, gets, pages, charged = [], [], [], []
    for score, neighbors in zip(scores, gt):
        ranking = np.lexsort((np.arange(391), -score))
        chosen = set()
        for page in ranking:
            tentative = chosen | {int(page)}
            cover, count = covered_pages(tentative)
            if len(cover) <= 84 and count <= 32:
                chosen = tentative
        cover, count = covered_pages(chosen)
        hits.append(int(np.isin(page_of[neighbors], list(cover)).sum()))
        gets.append(count)
        pages.append(len(cover))
        charged.append(sum(min(256, 100_000 - 256 * page) * 780 for page in cover))
    p05 = sorted(hits)[math.ceil(.05 * 64) - 1]
    return {"schema": "borsuk-v285-exact-page-rank-bound-v1",
            "dataset": "CoHere first100k D768 cosine k100", "split": "development0-63",
            "queries": 64, "mean_fetched_gt_hits": sum(hits) / 64,
            "p05_fetched_gt_hits": p05, "max_gets": max(gets),
            "max_planned_bytes": max(charged), "mean_fetched_pages": sum(pages) / 64,
            "descriptor_worth_testing": sum(hits) / 64 >= 98.7 and p05 >= 95}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("raw", "layout", "requests", "truth", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    result = evaluate(args.raw, args.layout, args.requests, args.truth)
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
