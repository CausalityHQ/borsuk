"""Frozen V283 page coverage using source-only rotated two-bit row codes."""

import argparse
import json
import math
from pathlib import Path

import numpy as np

from scripts.native_rotated_two_bit_codes import (
    BATCH_ROWS, PACKED_BYTES, _fit_records, decode_levels, rotate_rows,
)
from scripts.v284_page_primary_dev64 import covered_pages, sha
from scripts.v285_exact_page_rank_bound import (
    LAYOUT_SHA, RAW_SHA, REQUESTS_SHA, TRUTH_SHA,
)

SEED = 20260923


def evaluate(raw, layout, requests, truth):
    assert [sha(path.read_bytes()) for path in (raw, layout, requests, truth)] == [
        RAW_SHA, LAYOUT_SHA, REQUESTS_SHA, TRUTH_SHA]
    vectors = np.memmap(raw, dtype="<f4", mode="r", shape=(100_000, 768))
    order = np.load(layout, allow_pickle=False)
    assert order.shape == (100_000,) and np.array_equal(np.sort(order), np.arange(100_000))
    mean = np.asarray(np.mean(vectors, axis=0, dtype=np.float64), np.float32)
    records = np.empty((100_000, 200), dtype=np.uint8)
    for first in range(0, 100_000, BATCH_ROWS):
        last = min(first + BATCH_ROWS, 100_000)
        rows = np.asarray(vectors[order[first:last]], dtype=np.float64)
        assert np.isfinite(rows).all()
        records[first:last] = _fit_records(rows - mean.astype(np.float64), rotation_seed=SEED)
    requests_json = [json.loads(line) for line in requests.read_text().splitlines()]
    assert len(requests_json) == 64 and [q["query_ordinal"] for q in requests_json] == list(range(64))
    queries = np.asarray([q["query"] for q in requests_json], dtype=np.float32)
    assert queries.shape == (64, 768) and np.isfinite(queries).all()
    query_norms = np.linalg.norm(queries, axis=1)
    assert (query_norms > 0).all()
    q_rot = rotate_rows(queries.astype(np.float64), rotation_seed=SEED).astype(np.float32)
    m_rot = rotate_rows(mean[None, :].astype(np.float64), rotation_seed=SEED)[0].astype(np.float32)
    mean_dot = queries @ mean
    mean_norm2 = float(mean @ mean)
    scores = np.full((64, 391), -np.inf, dtype=np.float32)
    for first in range(0, 100_000, BATCH_ROWS):
        last = min(first + BATCH_ROWS, 100_000)
        part = records[first:last]
        levels = decode_levels(part[:, :PACKED_BYTES]).astype(np.float32)
        scale = np.frombuffer(part[:, PACKED_BYTES:PACKED_BYTES + 4].tobytes(), dtype="<f4")
        assert np.isfinite(scale).all() and (scale > 0).all()
        coded_norm2 = np.einsum("ij,ij->i", levels, levels) * scale * scale
        norm2 = mean_norm2 + 2 * scale * (levels @ m_rot) + coded_norm2
        assert np.isfinite(norm2).all() and (norm2 > 0).all()
        approximate_dot = (q_rot @ levels.T) * scale[None, :] + mean_dot[:, None]
        cosine = approximate_dot / (np.sqrt(norm2)[None, :] * query_norms[:, None])
        for page_first in range(first, last, 256):
            page_last = min(page_first + 256, last)
            scores[:, page_first // 256] = cosine[:, page_first - first:page_last - first].max(axis=1)
    gt = np.fromfile(truth, dtype="<u4").reshape(64, 100)
    assert (gt < 100_000).all()
    page_of = np.empty(100_000, dtype=np.int32)
    page_of[order] = np.arange(100_000) // 256
    hits, gets, charged = [], [], []
    for page_scores, neighbors in zip(scores, gt):
        ranking = np.lexsort((np.arange(391), -page_scores))
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
    p05 = sorted(hits)[math.ceil(.05 * 64) - 1]
    return {"schema": "borsuk-v289-two-bit-page-rank-v1",
            "dataset": "CoHere first100k D768 cosine k100", "split": "development0-63",
            "queries": 64, "code_bytes_per_row": 200,
            "mean_fetched_gt_hits": sum(hits) / 64, "p05_fetched_gt_hits": p05,
            "max_gets": max(gets), "max_planned_bytes": max(charged),
            "advance": sum(hits) / 64 >= 98.9 and p05 >= 96 and
            max(gets) <= 32 and max(charged) <= 16_777_216}


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
