"""One frozen page-primary coverage check on V283 CoHere development queries."""

import argparse
import hashlib
import json
import math
import tarfile
from pathlib import Path

import numpy as np

LAYOUT_SHA = "303f31ab8a182a0aaa304c4ef551a046be41071ac24e67a793882eb74c5b532e"
REQUESTS_SHA = "1de0122f73d1b72e54498640b9701ce6d156b513629596447580c85fac302ba4"
TRUTH_SHA = "f6630d0edf06539752c3fbf129ae01e58d3a3cf7b6aefa4decaa9c979e8ba355"
ARCHIVE_SHA = "42b0ddef9488f2ad30ba6aa22b40f2472e8a37c36427db3d27bd618073d70510"
ROOT_SHA_FILE_SHA = "7158e40299caba8a520caf79866262982e6a932b593d8a389caeb494ad9545d7"


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def covered_pages(selected):
    """Bridge the cheapest gaps until at most 32 physical ranges remain."""
    selected = sorted(selected)
    gaps = [(selected[i + 1] - selected[i] - 1, selected[i], i)
            for i in range(len(selected) - 1) if selected[i + 1] > selected[i] + 1]
    bridged = {i for _, _, i in sorted(gaps)[:max(0, len(gaps) + 1 - 32)]}
    cover = set(selected)
    for i in bridged:
        cover.update(range(selected[i] + 1, selected[i + 1]))
    gets = 1 + len(gaps) - len(bridged)
    return cover, gets


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
    assert centers.shape == (3125, 768) and np.isfinite(centers).all()
    order = np.load(layout, allow_pickle=False)
    assert order.shape == (100_000,) and np.array_equal(np.sort(order), np.arange(100_000))
    page_of = np.empty(100_000, dtype=np.int32)
    page_of[order] = np.arange(100_000) // 256
    queries = [json.loads(line) for line in requests.read_text().splitlines()]
    gt = np.fromfile(truth, dtype="<u4").reshape(64, 100)
    assert len(queries) == 64 and [q["query_ordinal"] for q in queries] == list(range(64))
    assert (gt < 100_000).all()
    norms = np.einsum("ij,ij->i", centers, centers)
    hits, gets, pages, selected, charged = [], [], [], [], []
    for request, neighbors in zip(queries, gt):
        query = np.asarray(request["query"], dtype=np.float32)
        assert query.shape == (768,) and np.isfinite(query).all()
        distances = norms - 2 * (centers @ query)
        page_scores = np.minimum.reduceat(distances, np.arange(0, len(centers), 8))
        ranking = np.lexsort((np.arange(len(page_scores)), page_scores))
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
        selected.append(len(chosen))
        charged.append(sum(min(256, 100_000 - 256 * page) * 780 for page in cover))
    return {"schema": "borsuk-v284-page-primary-dev64-v1", "dataset": "CoHere first100k D768 cosine k100",
            "split": "development0-63", "queries": 64, "layout_sha256": LAYOUT_SHA,
            "mean_fetched_gt_hits": sum(hits) / 64,
            "p05_fetched_gt_hits": sorted(hits)[math.ceil(64 * .05) - 1],
            "max_gets": max(gets), "max_planned_bytes": max(charged),
            "mean_selected_pages": sum(selected) / 64,
            "mean_fetched_pages": sum(pages) / 64,
            "advance_to_sublinear_router": sum(hits) / 64 >= 98.7
            and sorted(hits)[math.ceil(64 * .05) - 1] >= 95}


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
