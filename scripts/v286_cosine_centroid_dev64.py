"""Frozen cosine-scored unit-centroid page coverage on V283 development."""

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
        manifest_raw = source.extractfile("generation/manifest.json").read()
        assert sha(manifest_raw) == root_sha.read_text().strip()
        manifest = json.loads(manifest_raw)
        raw = source.extractfile("generation/centroids.bin").read()
        assert sha(raw) == manifest["centroids_sha256"]
    assert raw[:8] == b"BORSUCP1"
    rows = int.from_bytes(raw[8:16], "little")
    dimensions, unit_rows, page_rows = [int.from_bytes(raw[i:i + 4], "little")
                                       for i in (16, 20, 24)]
    assert (rows, dimensions, unit_rows, page_rows) == (100_000, 768, 32, 256)
    centers = np.frombuffer(raw, dtype="<f2", offset=32).astype(np.float32).reshape(-1, 768)
    center_norms = np.linalg.norm(centers, axis=1)
    assert centers.shape == (3125, 768) and np.isfinite(centers).all()
    assert (center_norms > 0).all()
    centers /= center_norms[:, None]
    order = np.load(layout, allow_pickle=False)
    assert order.shape == (100_000,) and np.array_equal(np.sort(order), np.arange(100_000))
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
        query_norm = np.linalg.norm(query)
        assert query_norm > 0
        scores = centers @ (query / query_norm)
        page_scores = np.maximum.reduceat(scores, np.arange(0, len(centers), 8))
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
        charged.append(sum(min(256, 100_000 - 256 * page) * 780 for page in cover))
    p05 = sorted(hits)[math.ceil(64 * .05) - 1]
    return {"schema": "borsuk-v286-cosine-centroid-dev64-v1",
            "dataset": "CoHere first100k D768 cosine k100", "split": "development0-63",
            "queries": 64, "mean_fetched_gt_hits": sum(hits) / 64,
            "p05_fetched_gt_hits": p05, "max_gets": max(gets),
            "max_planned_bytes": max(charged),
            "advance_to_rust": sum(hits) / 64 >= 98.7 and p05 >= 95
            and max(gets) <= 32 and max(charged) <= 16_777_216}


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
