#!/usr/bin/env python3
"""Seal three-arm D96 returned IDs before loading truth, then reduce quality."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

import numpy as np

from scripts.v114_1m_paired import score_sq8_ranges
from scripts.v124_source_tier_precision import rank, unit

ROWS = 100_000
DIMENSIONS = 96
QUERY_COUNT = 1000
MAX_BYTES = 16_777_216
V140_RAW_SHA = "e2a1b31161bb2190cb7dc36d0601c50449c6d6aba0cc1a4985c032d713bc54d5"
ROUTE_SHA = "388fe94cfd6c6ac59e7153c9947af5cdb07119e8fe5acabef4d5c1b3ffe422d4"
BASELINE_SOURCE_HITS = {"broad": 99_942, "control": 98_827}
BASELINE_SQ8_HITS = {"broad": 99_106, "control": 98_203}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def checked_lines(path: Path, expected_sha: str | None = None) -> list[dict]:
    if expected_sha and sha256(path) != expected_sha:
        raise ValueError(f"sealed input differs: {path}")
    values = [json.loads(line) for line in path.read_text().splitlines()]
    if len(values) != QUERY_COUNT:
        raise ValueError(f"query row count differs: {path}")
    return values


def page_ranges(value: object, rows: int, row_bytes: int) -> tuple[list[list[int]], int]:
    if not isinstance(value, list) or not 1 <= len(value) <= 32:
        raise ValueError("range GET count differs")
    previous = 0
    charged = 0
    for pair in value:
        if (not isinstance(pair, list) or len(pair) != 2
                or not all(isinstance(number, int) for number in pair)):
            raise ValueError("range pair differs")
        first, last = pair
        if (first < previous or first >= last or last > rows * row_bytes
                or first % (256 * row_bytes)
                or (last != rows * row_bytes and last % (256 * row_bytes))):
            raise ValueError("physical page range differs")
        charged += last - first
        previous = last
    if charged > MAX_BYTES:
        raise ValueError("range byte cap differs")
    return value, charged


def replay(args: argparse.Namespace) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    if args.sq8.stat().st_size != ROWS * (DIMENSIONS + 12):
        raise ValueError("SQ8 object length differs")
    record = np.dtype([("id", "<i8"), ("norm", "<f4"),
                       ("code", "u1", (DIMENSIONS,))])
    sq8 = np.memmap(args.sq8, dtype=record, mode="r", shape=(ROWS,))
    if not np.array_equal(np.sort(sq8["id"]), np.arange(ROWS)):
        raise ValueError("SQ8 source IDs differ")
    low = np.fromfile(args.low, dtype="<f4")
    step = np.fromfile(args.step, dtype="<f4")
    if (low.shape != (DIMENSIONS,) or step.shape != (DIMENSIONS,)
            or not np.isfinite(low).all() or not np.isfinite(step).all()
            or np.any(step <= 0)):
        raise ValueError("SQ8 coefficient plane differs")
    table = pq.read_table(args.source, columns=["feature_row_id", "embedding"])
    embedding = table.schema.field("embedding").type
    if (table.num_rows != ROWS or not pa.types.is_fixed_size_list(embedding)
            or embedding.list_size != DIMENSIONS
            or embedding.value_type != pa.float32()):
        raise ValueError("source geometry differs")
    source_ids = table["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)
    if not np.array_equal(source_ids, np.arange(ROWS)):
        raise ValueError("source ID order differs")
    source = np.asarray(
        table["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False),
        dtype=np.float32).reshape(ROWS, DIMENSIONS)
    if not np.isfinite(source).all():
        raise ValueError("source coordinates differ")
    queries = checked_lines(args.queries)
    routes = checked_lines(args.routes, ROUTE_SHA)
    plans = checked_lines(args.v140_raw, V140_RAW_SHA)
    with args.replay.open("x") as target:
        for ordinal, (request, route, plan) in enumerate(zip(queries, routes, plans)):
            if (request["query_ordinal"] != ordinal
                    or request["source_query_ordinal"] != 9000 + ordinal
                    or route["query_ordinal"] != ordinal
                    or route["source_query_ordinal"] != 9000 + ordinal
                    or plan["query_ordinal"] != ordinal):
                raise ValueError("replay query identity differs")
            query = np.asarray(request["query"], dtype=np.float32)
            if (query.shape != (DIMENSIONS,) or not np.isfinite(query).all()
                    or np.linalg.norm(query) <= 0):
                raise ValueError("query vector differs")
            nominees = route["nominees"]
            if (len(nominees) != 512 or len(set(nominees)) != 512
                    or min(nominees) < 0 or max(nominees) >= ROWS):
                raise ValueError("nominee roster differs")
            nominee_ids = set(map(int, sq8["id"][nominees]))
            beta = plan["variants"]["4"]
            arms = {
                "beta4": beta["ranges"],
                "broad": route["candidate_ranges"],
                "control": route["baseline_ranges"],
            }
            outcome = {"query_ordinal": ordinal,
                       "source_query_ordinal": 9000 + ordinal, "arms": {}}
            for name, raw_ranges in arms.items():
                ranges, charged = page_ranges(raw_ranges, ROWS, record.itemsize)
                if name == "beta4" and (charged != beta["planned_bytes"]
                                        or len(ranges) != beta["gets"]):
                    raise ValueError("V140 β4 route charge differs")
                started = time.perf_counter_ns()
                returned = score_sq8_ranges(
                    sq8, query, low, step, ranges, top_k=512)
                sq8_ns = time.perf_counter_ns() - started
                if len(returned) != 512 or len(set(returned)) != 512:
                    raise ValueError("returned SQ8 width differs")
                union = np.asarray(sorted(nominee_ids | set(returned)), dtype=np.int64)
                if union.size < 512 or union.size > 1024:
                    raise ValueError("source union width differs")
                started = time.perf_counter_ns()
                source_scores = unit(source[union].astype(np.float64)) @ query.astype(np.float64)
                exact = rank(union, source_scores, 100)
                source_ns = time.perf_counter_ns() - started
                outcome["arms"][name] = {
                    "ranges": ranges, "gets": len(ranges), "planned_bytes": charged,
                    "sq8_top100_ids": returned[:100],
                    "source_top100_ids": exact.tolist(),
                    "union_size": int(union.size),
                    "sq8_ns": sq8_ns, "source_ns": source_ns,
                }
            target.write(json.dumps(outcome, sort_keys=True, separators=(",", ":")) + "\n")


def reduce(args: argparse.Namespace) -> None:
    if args.truth is None or sha256(args.truth) != "9e29a3e07ee2fe199fb17d8ec19b62f158d0cebe86ed23db14edecfa877c1a7f":
        raise ValueError("V122 GT identity differs")
    truth = np.load(args.truth, allow_pickle=False)
    if truth.shape != (QUERY_COUNT, 100) or truth.min() < 0 or truth.max() >= ROWS:
        raise ValueError("V122 GT geometry differs")
    replayed = checked_lines(args.replay)
    record = np.dtype([("id", "<i8"), ("norm", "<f4"),
                       ("code", "u1", (DIMENSIONS,))])
    if args.sq8 is None or args.sq8.stat().st_size != ROWS * record.itemsize:
        raise ValueError("reduction SQ8 object geometry differs")
    sq8 = np.memmap(args.sq8, dtype=record, mode="r", shape=(ROWS,))
    inverse = np.empty(ROWS, dtype=np.int64)
    inverse[sq8["id"]] = np.arange(ROWS, dtype=np.int64)
    if not np.array_equal(np.sort(sq8["id"]), np.arange(ROWS)):
        raise ValueError("reduction SQ8 ID permutation differs")
    counts = {name: {"source": [], "sq8": [], "bytes": [], "gets": [],
                     "physical": [], "union": [], "sq8_ns": [], "source_ns": []}
              for name in ("beta4", "broad", "control")}
    paired = {name: {"wins": 0, "ties": 0, "losses": 0}
              for name in ("broad", "control")}
    with args.evidence.open("x") as output:
        for ordinal, (row, gold) in enumerate(zip(replayed, truth)):
            if (row["query_ordinal"] != ordinal
                    or row["source_query_ordinal"] != 9000 + ordinal
                    or len(set(map(int, gold))) != 100):
                raise ValueError("reduced query identity differs")
            gold_set = set(map(int, gold))
            result = {"query_ordinal": ordinal, "arms": {}}
            for name, arm in row["arms"].items():
                source_ids = arm["source_top100_ids"]
                sq8_ids = arm["sq8_top100_ids"]
                if (len(source_ids) != 100 or len(set(source_ids)) != 100
                        or len(sq8_ids) != 100 or len(set(sq8_ids)) != 100):
                    raise ValueError("returned IDs differ")
                source_hits = len(set(source_ids) & gold_set)
                sq8_hits = len(set(sq8_ids) & gold_set)
                physical_hits = sum(
                    any(first <= int(inverse[int(source_id)]) * record.itemsize < last
                        for first, last in arm["ranges"])
                    for source_id in gold)
                if sq8_hits > physical_hits:
                    raise ValueError("SQ8 hits exceed fetched-page coverage")
                result["arms"][name] = {"source_hits": source_hits,
                                        "sq8_hits": sq8_hits,
                                        "physical_hits": physical_hits}
                for field, value in (
                    ("source", source_hits), ("sq8", sq8_hits),
                    ("physical", physical_hits),
                    ("bytes", arm["planned_bytes"]), ("gets", arm["gets"]),
                    ("union", arm["union_size"]),
                    ("sq8_ns", arm["sq8_ns"]), ("source_ns", arm["source_ns"]),
                ):
                    counts[name][field].append(value)
            for baseline in paired:
                beta_hits = result["arms"]["beta4"]["source_hits"]
                baseline_hits = result["arms"][baseline]["source_hits"]
                outcome = ("wins" if beta_hits > baseline_hits else
                           "losses" if beta_hits < baseline_hits else "ties")
                paired[baseline][outcome] += 1
            output.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    for name in BASELINE_SOURCE_HITS:
        if (sum(counts[name]["source"]) != BASELINE_SOURCE_HITS[name]
                or sum(counts[name]["sq8"]) != BASELINE_SQ8_HITS[name]
                or sum(counts[name]["physical"]) != BASELINE_SOURCE_HITS[name]):
            raise ValueError(f"historical baseline reproduction differs: {name}")
    if sum(counts["beta4"]["physical"]) != 99_738:
        raise ValueError("V140 β4 fetched-range physical coverage differs")
    summary = {"schema": "borsuk-v141-deep-returned-quality-v1",
               "dataset": "deep-image-96-angular random100k train subset",
               "split": "used publication-test ordinals 9000-9999",
               "query_count": QUERY_COUNT,
               "replay_sha256": sha256(args.replay),
               "evidence_sha256": sha256(args.evidence),
               "paired_beta4_vs_broad": paired["broad"],
               "paired_beta4_vs_control": paired["control"],
               "arms": {}}
    for name, values in counts.items():
        source = sorted(values["source"])
        sq8 = sorted(values["sq8"])
        summary["arms"][name] = {
            "source_hits": sum(source), "sq8_hits": sum(sq8),
            "physical_hits": sum(values["physical"]),
            "source_p05_hits": source[49],
            "source_sub90_queries": sum(value < 90 for value in source),
            "sq8_p05_hits": sq8[49],
            "mean_planned_bytes": sum(values["bytes"]) / QUERY_COUNT,
            "p95_planned_bytes": sorted(values["bytes"])[949],
            "mean_gets": sum(values["gets"]) / QUERY_COUNT,
            "p95_gets": sorted(values["gets"])[949],
            "mean_union_size": sum(values["union"]) / QUERY_COUNT,
            "sq8_p95_ms": sorted(values["sq8_ns"])[949] / 1e6,
            "source_p95_ms": sorted(values["source_ns"])[949] / 1e6,
        }
    beta = summary["arms"]["beta4"]
    summary["passes_quality_screen"] = (
        beta["source_hits"] >= 99_500
        and beta["source_p05_hits"] >= 98
        and beta["source_sub90_queries"] == 0
        and beta["source_hits"] >= BASELINE_SOURCE_HITS["control"]
    )
    args.summary.write_text(json.dumps(summary, sort_keys=True, separators=(",", ":")) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("replay", "reduce"))
    for name in ("source", "sq8", "low", "step", "queries", "routes",
                 "v140_raw", "replay", "truth", "evidence", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path)
    args = parser.parse_args()
    if args.phase == "replay":
        needed = ("source", "sq8", "low", "step", "queries", "routes",
                  "v140_raw", "replay")
        if any(getattr(args, name) is None for name in needed):
            parser.error("replay inputs differ")
        replay(args)
    else:
        needed = ("replay", "truth", "sq8", "evidence", "summary")
        if any(getattr(args, name) is None for name in needed):
            parser.error("reduce inputs differ")
        reduce(args)


if __name__ == "__main__":
    main()
