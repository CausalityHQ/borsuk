#!/usr/bin/env python3
"""Trace closed V206 FP16 misses through V121 nomination and physical ranges."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

SHA = {
    "layout": "419f9280d2e85f6fa275c115dd342c249ac31ec6af2d19a42f5b96c38ed247c1",
    "rosters": "eb24f9a22d237db7416696415220e3fba559eadf7c7165ac6c8fd230a1d91700",
    "replay": "ddc9af991bdc6d3ef77d34a156994daa43aeb78f67f18de2cd0dc5ebb93abe91",
    "raw": "3fb5ef49833d672b82346612bbd80d0d50cceaed72d5de1d6b5da797c5762dd0",
    "truth": "d305fcea7387988941defd2942cca1673693271329f977ba073da888cac3de8d",
}
ROWS, ROW_BYTES, QUERY_COUNT = 9_990_000, 108, 1_000
PAGE_ROWS, GET_CAP = 256, 32
PAGE_BYTES = PAGE_ROWS * ROW_BYTES
BYTE_CAP = 16_777_216


def authenticate(path: Path, expected: str) -> None:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(4 * 1024 * 1024):
            digest.update(block)
    if digest.hexdigest() != expected:
        raise ValueError(f"closed artifact identity differs: {path}")


def min_pages_by_hits(pages: np.ndarray, weights: np.ndarray,
                      get_cap: int) -> np.ndarray:
    """Exact GT-aware minimum full pages by hits with at most get_cap GETs."""
    hits = int(weights.sum())
    infinity = ROWS + 1
    initial = np.full((get_cap + 1, hits + 1), infinity, np.int32)
    initial[0, 0] = 0
    prefix = [initial]
    for end in range(len(pages)):
        next_state = prefix[-1].copy()  # skip the final GT-bearing page
        gained = 0
        for start in range(end, -1, -1):
            gained += int(weights[start])
            cost = int(pages[end] - pages[start] + 1)
            source = prefix[start][:get_cap]
            remaining = hits - gained + 1
            np.minimum(next_state[1:, gained:],
                       source[:, :remaining] + cost,
                       out=next_state[1:, gained:])
            if remaining < hits + 1:
                saturated = source[:, remaining:].min(axis=1) + cost
                np.minimum(next_state[1:, hits], saturated,
                           out=next_state[1:, hits])
        prefix.append(next_state)
    return prefix[-1].min(axis=0)


def oracle_page_bounds(positions: np.ndarray) -> tuple[int, int]:
    """Exact GT-aware maximum coverage and byte minimum for 90 hits.

    Endpoints at GT-bearing pages suffice: trimming an interval to those
    endpoints preserves covered GT and never increases either resource.
    """
    pages, weights = np.unique(positions // PAGE_ROWS, return_counts=True)
    if pages[-1] == (ROWS - 1) // PAGE_ROWS:
        raise ValueError("oracle needs exact short-last-page accounting")
    costs = min_pages_by_hits(pages, weights, GET_CAP)
    max_hits = int(np.flatnonzero(costs * PAGE_BYTES <= BYTE_CAP)[-1])
    pages_for_90 = int(costs[90])
    return max_hits, pages_for_90


def trace(paths: dict[str, Path]) -> dict:
    for name, path in paths.items():
        authenticate(path, SHA[name])
    layout = np.load(paths["layout"], mmap_mode="r", allow_pickle=False)
    if layout.shape != (ROWS,) or layout.dtype.kind not in "iu":
        raise ValueError("layout geometry differs")
    inverse = np.empty(ROWS, np.int32)
    inverse[layout] = np.arange(ROWS, dtype=np.int32)
    if not np.array_equal(layout[inverse], np.arange(ROWS)):
        raise ValueError("layout is not a permutation")
    truth = pq.read_table(paths["truth"], columns=["neighbors_id"])
    gold = truth["neighbors_id"].slice(0, QUERY_COUNT).to_pylist()
    totals = {name: 0 for name in ("nominated", "covered", "fp16_returned",
                                   "outside_nomination", "nominated_uncovered",
                                   "covered_unreturned")}
    tail = []
    with (paths["rosters"].open() as rosters, paths["replay"].open() as replays,
          paths["raw"].open() as raws):
        for ordinal, (roster_line, replay_line, raw_line) in enumerate(
                zip(rosters, replays, raws, strict=True)):
            if ordinal >= QUERY_COUNT:
                raise ValueError("query roster exceeds closed cohort")
            roster, replay, raw = (json.loads(line) for line in
                                   (roster_line, replay_line, raw_line))
            if (roster["query_ordinal"] != ordinal
                    or replay["query_ordinal"] != ordinal
                    or raw["ordinal"] != ordinal
                    or raw["ranges"] != replay["ranges"]
                    or len(roster["nominees"]) != 512
                    or len(replay["ranges"]) > 32):
                raise ValueError(f"paired query differs: {ordinal}")
            target = np.asarray(gold[ordinal][:100], np.int64)
            if target.size != 100 or np.unique(target).size != 100:
                raise ValueError(f"GT100 differs: {ordinal}")
            positions = inverse[target]
            nominated = np.isin(positions, roster["nominees"])
            covered = np.zeros(100, bool)
            for first, last in replay["ranges"]:
                if (first < 0 or first >= last or last > ROWS * ROW_BYTES
                        or first % ROW_BYTES or last % ROW_BYTES):
                    raise ValueError(f"physical range differs: {ordinal}")
                covered |= (positions >= first // ROW_BYTES) & (positions < last // ROW_BYTES)
            returned = np.isin(target, raw["fp16_ids"])
            if not np.all(returned <= covered):
                raise ValueError(f"FP16 return escaped physical ranges: {ordinal}")
            counts = {
                "nominated": int(nominated.sum()),
                "covered": int(covered.sum()),
                "fp16_returned": int(returned.sum()),
                "outside_nomination": int((~nominated).sum()),
                "nominated_uncovered": int((nominated & ~covered).sum()),
                "covered_unreturned": int((covered & ~returned).sum()),
            }
            for name, count in counts.items():
                totals[name] += count
            if counts["fp16_returned"] < 90:
                oracle_hits, oracle_pages_for_90 = oracle_page_bounds(positions)
                tail.append({"ordinal": ordinal, **counts,
                             "gets": len(replay["ranges"]),
                             "planned_bytes": replay["plan_bytes"],
                             "oracle_max_hits": oracle_hits,
                             "oracle_min_bytes_for_90": oracle_pages_for_90 * PAGE_BYTES})
    if ordinal != QUERY_COUNT - 1 or len(tail) != 12:
        raise ValueError("closed query cohort or tail differs")
    return {"schema": "borsuk-v206-tail-trace-v1", "queries": QUERY_COUNT,
            "totals": totals, "tail": tail}


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in SHA:
        parser.add_argument(f"--{name}", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(trace({name: getattr(args, name) for name in SHA}), sort_keys=True))


if __name__ == "__main__":
    main()
