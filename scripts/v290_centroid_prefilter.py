"""Necessary GT coverage of a fixed sqrt(page-count) centroid prefilter."""

import argparse
import json
import math
import tarfile
from pathlib import Path

import numpy as np

from scripts.v284_page_primary_dev64 import (
    ARCHIVE_SHA, LAYOUT_SHA, REQUESTS_SHA, ROOT_SHA_FILE_SHA, TRUTH_SHA, sha,
)


def evaluate(archive, root_sha, layout, requests, truth):
    assert [sha(path.read_bytes()) for path in (archive, root_sha, layout, requests, truth)] == [
        ARCHIVE_SHA, ROOT_SHA_FILE_SHA, LAYOUT_SHA, REQUESTS_SHA, TRUTH_SHA]
    with tarfile.open(archive, "r:gz") as source:
        root_raw = source.extractfile("generation/manifest.json").read()
        assert sha(root_raw) == root_sha.read_text().strip()
        root = json.loads(root_raw)
        raw = source.extractfile("generation/centroids.bin").read()
        assert sha(raw) == root["centroids_sha256"]
    assert raw[:8] == b"BORSUCP1"
    assert int.from_bytes(raw[8:16], "little") == 100_000
    assert [int.from_bytes(raw[i:i+4], "little") for i in (16, 20, 24)] == [768, 32, 256]
    centers = np.frombuffer(raw, dtype="<f2", offset=32).astype(np.float32).reshape(391, 8, 768)
    assert np.isfinite(centers).all()
    order = np.load(layout, allow_pickle=False)
    assert order.shape == (100_000,) and np.array_equal(np.sort(order), np.arange(100_000))
    page_of = np.empty(100_000, dtype=np.int32)
    page_of[order] = np.arange(100_000) // 256
    req = [json.loads(line) for line in requests.read_text().splitlines()]
    gt = np.fromfile(truth, dtype="<u4").reshape(64, 100)
    assert len(req) == 64 and [q["query_ordinal"] for q in req] == list(range(64))
    assert (gt < 100_000).all()
    candidate_count = math.ceil(8 * math.sqrt(391))
    norms = np.einsum("ijk,ijk->ij", centers, centers)
    hits = []
    for request, neighbors in zip(req, gt):
        q = np.asarray(request["query"], dtype=np.float32)
        assert q.shape == (768,) and np.isfinite(q).all()
        distances = norms - 2 * (centers.reshape(-1, 768) @ q).reshape(391, 8)
        pages = np.lexsort((np.arange(391), distances.min(axis=1)))[:candidate_count]
        hits.append(int(np.isin(page_of[neighbors], pages).sum()))
    p05 = sorted(hits)[math.ceil(.05 * 64) - 1]
    return {"schema": "borsuk-v290-centroid-prefilter-v1",
            "dataset": "CoHere first100k D768 cosine k100", "split": "development0-63",
            "queries": 64, "candidate_pages": candidate_count,
            "mean_candidate_gt_hits": sum(hits) / 64, "p05_candidate_gt_hits": p05,
            "advance": sum(hits) / 64 >= 98.9 and p05 >= 96}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("archive", "root-sha", "layout", "requests", "truth", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    result = evaluate(args.archive, args.root_sha, args.layout, args.requests,
                      args.truth)
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
