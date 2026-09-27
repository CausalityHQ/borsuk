"""Diagnostic upper bound: optimal GT-containing page ranges under V282 caps."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np


def best_coverage(hits: list[int], *, max_gets: int, max_pages: int) -> int:
    """Max hits in at most `max_gets` contiguous ranges and `max_pages` pages."""
    if max_gets < 1 or max_pages < 1 or any(x < 0 for x in hits):
        raise ValueError("invalid page budget or hit count")
    outside = np.full((max_gets + 1, max_pages + 1), -1, dtype=np.int16)
    inside = outside.copy()
    outside[0, 0] = 0
    for hit in hits:
        next_outside = np.maximum(outside, inside)
        next_inside = np.full_like(inside, -1)
        # Starting a new range spends one GET; continuing does not.
        next_inside[0, 1:] = np.where(inside[0, :-1] >= 0,
                                      inside[0, :-1] + hit, -1)
        next_inside[1:, 1:] = np.maximum(
            np.where(inside[1:, :-1] >= 0, inside[1:, :-1] + hit, -1),
            np.where(outside[:-1, :-1] >= 0, outside[:-1, :-1] + hit, -1),
        )
        outside, inside = next_outside, next_inside
    return int(max(outside.max(), inside.max()))


def page_oracle(layout: Path, truth: Path, *, layout_sha: str, truth_sha: str) -> dict:
    if hashlib.sha256(layout.read_bytes()).hexdigest() != layout_sha:
        raise ValueError("layout digest differs")
    if hashlib.sha256(truth.read_bytes()).hexdigest() != truth_sha:
        raise ValueError("truth digest differs")
    order = np.load(layout, allow_pickle=False)
    if (order.shape != (100_000,) or not np.issubdtype(order.dtype, np.integer)
            or not np.array_equal(np.sort(order), np.arange(100_000))):
        raise ValueError("layout is not a 100k permutation")
    gt = np.fromfile(truth, dtype="<u4")
    if gt.size != 100_000 or (gt >= 100_000).any():
        raise ValueError("GT100 roster or IDs differ")
    gt = gt.reshape(1000, 100)
    pages = np.empty(100_000, dtype=np.int32)
    pages[order] = np.arange(100_000) // 256
    scores = []
    for query in gt[:64]:
        hits = np.bincount(pages[query], minlength=391).tolist()
        scores.append(best_coverage(hits, max_gets=32, max_pages=84))
    result = {
        "schema": "borsuk-v283-page-oracle-v1", "queries": 64,
        "split": "CoHere first100k development ordinals0-63",
        "layout_sha256": layout_sha, "truth_sha256": truth_sha,
        "max_gets": 32, "max_bytes": 16_777_216,
        "max_pages": 84, "mean_fetched_gt_hits_upper_bound": sum(scores) / len(scores),
        "p05_fetched_gt_hits_upper_bound": sorted(scores)[math.ceil(0.05 * len(scores)) - 1],
        "advance_to_router": sum(scores) / len(scores) >= 98.7
        and sorted(scores)[math.ceil(0.05 * len(scores)) - 1] >= 95,
    }
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layout", type=Path, required=True)
    parser.add_argument("--truth", type=Path, required=True)
    parser.add_argument("--layout-sha", required=True)
    parser.add_argument("--truth-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = page_oracle(args.layout, args.truth,
                         layout_sha=args.layout_sha, truth_sha=args.truth_sha)
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
