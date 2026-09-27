"""Global PQ64 page-score bounds, with the frozen V284 physical scheduler."""

import argparse
import json
import math
import tarfile
from pathlib import Path

import numpy as np

from scripts.v284_page_primary_dev64 import (
    ARCHIVE_SHA, LAYOUT_SHA, REQUESTS_SHA, ROOT_SHA_FILE_SHA, TRUTH_SHA,
    covered_pages, sha,
)


def evaluate(archive, root_sha, layout, requests, truth):
    assert sha(archive.read_bytes()) == ARCHIVE_SHA
    assert sha(root_sha.read_bytes()) == ROOT_SHA_FILE_SHA
    assert sha(layout.read_bytes()) == LAYOUT_SHA
    assert sha(requests.read_bytes()) == REQUESTS_SHA
    assert sha(truth.read_bytes()) == TRUTH_SHA
    with tarfile.open(archive, "r:gz") as source:
        root_raw = source.extractfile("generation/manifest.json").read()
        assert sha(root_raw) == root_sha.read_text().strip()
        root = json.loads(root_raw)
        manifest_raw = source.extractfile("generation/router/manifest.json").read()
        assert sha(manifest_raw) == root["router_manifest_sha256"]
        manifest = json.loads(manifest_raw)
        assert manifest["geometry"]["rows"] == 100_000
        assert manifest["geometry"]["dimensions"] == 768
        planes = {}
        for name in ("books", "codes"):
            body = source.extractfile(f"generation/router/{name}.bin").read()
            assert sha(body) == manifest["sections"][name]["sha256"]
            assert len(body) == manifest["sections"][name]["bytes"]
            planes[name] = body
    books = np.frombuffer(planes["books"], dtype="<f4").reshape(64, 256, 12)
    codes = np.frombuffer(planes["codes"], dtype="u1").reshape(100_000, 64)
    assert np.isfinite(books).all()
    norm_words = np.einsum("swd,swd->sw", books, books)
    row_norm2 = np.zeros(100_000, dtype=np.float32)
    for space in range(64):
        row_norm2 += norm_words[space, codes[:, space]]
    assert np.isfinite(row_norm2).all() and (row_norm2 > 0).all()
    order = np.load(layout, allow_pickle=False)
    assert order.shape == (100_000,) and np.array_equal(np.sort(order), np.arange(100_000))
    page_of = np.empty(100_000, dtype=np.int32)
    page_of[order] = np.arange(100_000) // 256
    queries = [json.loads(line) for line in requests.read_text().splitlines()]
    gt = np.fromfile(truth, dtype="<u4").reshape(64, 100)
    assert len(queries) == 64 and [q["query_ordinal"] for q in queries] == list(range(64))
    assert (gt < 100_000).all()
    outcomes = {"euclidean": [], "cosine": []}
    get_max = byte_max = 0
    for request, neighbors in zip(queries, gt):
        query = np.asarray(request["query"], dtype=np.float32)
        assert query.shape == (768,) and np.isfinite(query).all()
        qnorm2 = float(query @ query)
        assert qnorm2 > 0
        dots = np.einsum("sd,swd->sw", query.reshape(64, 12), books)
        row_dots = np.zeros(100_000, dtype=np.float32)
        for space in range(64):
            row_dots += dots[space, codes[:, space]]
        scores = {
            "euclidean": -(qnorm2 + row_norm2 - 2 * row_dots),
            "cosine": row_dots / (np.sqrt(row_norm2) * math.sqrt(qnorm2)),
        }
        for arm, row_scores in scores.items():
            page_scores = np.maximum.reduceat(row_scores, np.arange(0, 100_000, 256))
            ranking = np.lexsort((np.arange(len(page_scores)), -page_scores))
            chosen = set()
            for page in ranking:
                tentative = chosen | {int(page)}
                cover, count = covered_pages(tentative)
                if len(cover) <= 84 and count <= 32:
                    chosen = tentative
            cover, count = covered_pages(chosen)
            outcomes[arm].append(int(np.isin(page_of[neighbors], list(cover)).sum()))
            get_max = max(get_max, count)
            byte_max = max(byte_max, sum(min(256, 100_000 - 256 * p) * 780 for p in cover))
    metrics = {}
    for arm, hits in outcomes.items():
        p05 = sorted(hits)[math.ceil(.05 * 64) - 1]
        metrics[arm] = {"mean_fetched_gt_hits": sum(hits) / 64,
                        "p05_fetched_gt_hits": p05,
                        "advance": sum(hits) / 64 >= 98.9 and p05 >= 96}
    return {"schema": "borsuk-v287-global-pq-page-bound-v1",
            "dataset": "CoHere first100k D768 cosine k100", "split": "development0-63",
            "queries": 64, "max_gets": get_max, "max_planned_bytes": byte_max,
            "arms": metrics}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("archive", "root-sha", "layout", "requests", "truth", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    result = evaluate(args.archive, args.root_sha, args.layout, args.requests, args.truth)
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
