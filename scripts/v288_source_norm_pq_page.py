"""Source-norm PQ64 page-ranking check on frozen V283 CoHere development0-63."""

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
from scripts.v285_exact_page_rank_bound import RAW_SHA


def evaluate(raw, archive, root_sha, layout, requests, truth):
    assert [sha(p.read_bytes()) for p in (raw, archive, root_sha, layout, requests, truth)] == [
        RAW_SHA, ARCHIVE_SHA, ROOT_SHA_FILE_SHA, LAYOUT_SHA, REQUESTS_SHA, TRUTH_SHA]
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
    vectors = np.memmap(raw, dtype="<f4", mode="r", shape=(100_000, 768))
    order = np.load(layout, allow_pickle=False)
    assert order.shape == (100_000,) and np.array_equal(np.sort(order), np.arange(100_000))
    norms = np.empty(100_000, dtype="<f2")
    for start in range(0, 100_000, 4096):
        stop = min(start + 4096, 100_000)
        block = np.asarray(vectors[order[start:stop]], dtype=np.float32)
        assert np.isfinite(block).all()
        norms[start:stop] = np.sqrt(np.einsum("ij,ij->i", block, block)).astype("<f2")
    assert np.isfinite(norms).all() and (norms > 0).all()
    inv_norms = 1.0 / norms.astype(np.float32)
    page_of = np.empty(100_000, dtype=np.int32)
    page_of[order] = np.arange(100_000) // 256
    queries = [json.loads(line) for line in requests.read_text().splitlines()]
    gt = np.fromfile(truth, dtype="<u4").reshape(64, 100)
    assert len(queries) == 64 and [q["query_ordinal"] for q in queries] == list(range(64))
    assert (gt < 100_000).all()
    hits, gets, charged = [], [], []
    for request, neighbors in zip(queries, gt):
        query = np.asarray(request["query"], dtype=np.float32)
        assert query.shape == (768,) and np.isfinite(query).all()
        dots = np.einsum("sd,swd->sw", query.reshape(64, 12), books)
        row_dots = np.zeros(100_000, dtype=np.float32)
        for space in range(64):
            row_dots += dots[space, codes[:, space]]
        row_scores = row_dots * inv_norms
        page_scores = np.maximum.reduceat(row_scores, np.arange(0, 100_000, 256))
        ranking = np.lexsort((np.arange(len(page_scores)), -page_scores))
        chosen = set()
        for page in ranking:
            tentative = chosen | {int(page)}
            cover, count = covered_pages(tentative)
            if len(cover) <= 84 and count <= 32:
                chosen = tentative
        cover, count = covered_pages(chosen)
        hits.append(int(np.isin(page_of[neighbors], list(cover)).sum()))
        gets.append(count)
        charged.append(sum(min(256, 100_000 - 256 * p) * 780 for p in cover))
    p05 = sorted(hits)[math.ceil(.05 * 64) - 1]
    return {"schema": "borsuk-v288-source-norm-pq-page-v1",
            "dataset": "CoHere first100k D768 cosine k100", "split": "development0-63",
            "queries": 64, "norm_bytes_per_row": 2, "mean_fetched_gt_hits": sum(hits) / 64,
            "p05_fetched_gt_hits": p05, "max_gets": max(gets),
            "max_planned_bytes": max(charged),
            "advance": sum(hits) / 64 >= 98.9 and p05 >= 96 and
            max(gets) <= 32 and max(charged) <= 16_777_216}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("raw", "archive", "root-sha", "layout", "requests", "truth", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    result = evaluate(args.raw, args.archive, args.root_sha, args.layout,
                      args.requests, args.truth)
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
