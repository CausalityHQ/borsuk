#!/usr/bin/env python3
"""Seal V152 returned IDs without GT, then reduce against fresh sealed GT100."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.v114_1m_paired import score_sq8_ranges
from scripts.v124_source_tier_precision import rank, unit
from scripts.v141_deep_returned_quality import page_ranges
from scripts.v152_prepare_fresh import COUNT, FIRST, SOURCE_SHA, SQ8_SHA, sha256

ROWS = 100_000
DIMS = 96
ARMS = ("flat", "v150", "v151", "v152")


def lines(path: Path) -> list[dict]:
    values = [json.loads(line) for line in path.read_text().splitlines()]
    if len(values) != COUNT:
        raise ValueError(f"record count differs: {path}")
    return values


def checked_inputs(args: argparse.Namespace) -> tuple[list[dict], list[dict], dict]:
    sealed = json.loads(args.seal.read_text())
    if (sealed["schema"] != "borsuk-v152-fresh-inputs-v1"
            or sealed["source_query_start"] != FIRST
            or sealed["query_count"] != COUNT
            or sha256(args.queries) != sealed["queries_sha256"]
            or sha256(args.routing) != sealed["routing_sha256"]):
        raise ValueError("fresh query/route seal differs")
    return lines(args.queries), lines(args.routing), sealed


def replay(args: argparse.Namespace) -> None:
    requests, routes, _ = checked_inputs(args)
    plans = lines(args.plans)
    if (sha256(args.source) != SOURCE_SHA or sha256(args.sq8) != SQ8_SHA
            or args.sq8.stat().st_size != ROWS * (DIMS + 12)):
        raise ValueError("source or SQ8 input differs")
    dtype = np.dtype([("id", "<i8"), ("norm", "<f4"), ("code", "u1", (DIMS,))])
    sq8 = np.memmap(args.sq8, dtype=dtype, mode="r", shape=(ROWS,))
    if not np.array_equal(np.sort(sq8["id"]), np.arange(ROWS)):
        raise ValueError("SQ8 source IDs differ")
    low = np.fromfile(args.low, dtype="<f4")
    step = np.fromfile(args.step, dtype="<f4")
    if (low.shape != (DIMS,) or step.shape != (DIMS,)
            or not np.isfinite(low).all() or not np.isfinite(step).all()
            or np.any(step <= 0)):
        raise ValueError("SQ8 coefficients differ")
    table = pq.read_table(args.source, columns=["feature_row_id", "embedding"])
    field = table.schema.field("embedding").type
    if (table.num_rows != ROWS or not pa.types.is_fixed_size_list(field)
            or field.list_size != DIMS or field.value_type != pa.float32()):
        raise ValueError("source geometry differs")
    source_ids = table["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)
    if not np.array_equal(source_ids, np.arange(ROWS)):
        raise ValueError("source ID order differs")
    source = np.asarray(
        table["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False),
        dtype=np.float32,
    ).reshape(ROWS, DIMS)
    if not np.isfinite(source).all():
        raise ValueError("source coordinates differ")
    with args.replay.open("x") as output:
        for ordinal, (request, route, plan) in enumerate(zip(requests, routes, plans)):
            if any(record["query_ordinal"] != ordinal
                   or record["source_query_ordinal"] != FIRST + ordinal
                   for record in (request, route, plan)):
                raise ValueError("replay query identity differs")
            query = np.asarray(request["query"], dtype=np.float32)
            nominees = route["nominees"]
            if (query.shape != (DIMS,) or not np.isfinite(query).all()
                    or len(nominees) != 512 or len(set(nominees)) != 512
                    or min(nominees) < 0 or max(nominees) >= ROWS):
                raise ValueError("query or nominee roster differs")
            nominee_ids = set(map(int, sq8["id"][nominees]))
            result = {"query_ordinal": ordinal,
                      "source_query_ordinal": FIRST + ordinal, "arms": {}}
            for arm in ARMS:
                raw = plan["flat_plan"] if arm == "flat" else plan[arm]["plan"]
                ranges, charged = page_ranges(raw["ranges"], ROWS, dtype.itemsize)
                if charged != raw["planned_bytes"] or len(ranges) != raw["gets"]:
                    raise ValueError("sealed route charge differs")
                started = time.perf_counter_ns()
                returned = score_sq8_ranges(sq8, query, low, step, ranges, top_k=512)
                sq8_ns = time.perf_counter_ns() - started
                if len(returned) != 512 or len(set(returned)) != 512:
                    raise ValueError("returned SQ8 width differs")
                union = np.asarray(sorted(nominee_ids | set(returned)), dtype=np.int64)
                if not 512 <= union.size <= 1024:
                    raise ValueError("source union width differs")
                started = time.perf_counter_ns()
                scores = unit(source[union].astype(np.float64)) @ query.astype(np.float64)
                top = rank(union, scores, 100)
                source_ns = time.perf_counter_ns() - started
                result["arms"][arm] = {
                    "source_top100_ids": top.tolist(),
                    "sq8_top100_ids": returned[:100],
                    "ranges": ranges,
                    "gets": len(ranges),
                    "planned_bytes": charged,
                    "union_size": int(union.size),
                    "sq8_ns": sq8_ns,
                    "source_ns": source_ns,
                }
            output.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    args.replay_seal.write_text(json.dumps({
        "schema": "borsuk-v152-returned-seal-v1",
        "replay_sha256": sha256(args.replay),
        "plans_sha256": sha256(args.plans),
        "truth_opened": False,
    }, sort_keys=True, separators=(",", ":")) + "\n")


def reduce(args: argparse.Namespace) -> None:
    requests, routes, sealed = checked_inputs(args)
    del requests, routes
    replay_seal = json.loads(args.replay_seal.read_text())
    if (replay_seal["schema"] != "borsuk-v152-returned-seal-v1"
            or replay_seal["truth_opened"] is not False
            or replay_seal["replay_sha256"] != sha256(args.replay)
            or replay_seal["plans_sha256"] != sha256(args.plans)
            or sealed["truth_sha256"] != sha256(args.truth)):
        raise ValueError("returned or GT seal differs")
    gold = np.load(args.truth, allow_pickle=False)
    replayed = lines(args.replay)
    plans = lines(args.plans)
    if (gold.shape != (COUNT, 100) or gold.min() < 0 or gold.max() >= ROWS
            or args.sq8.stat().st_size != ROWS * (DIMS + 12)
            or sha256(args.sq8) != SQ8_SHA):
        raise ValueError("GT or SQ8 geometry differs")
    dtype = np.dtype([("id", "<i8"), ("norm", "<f4"), ("code", "u1", (DIMS,))])
    sq8 = np.memmap(args.sq8, dtype=dtype, mode="r", shape=(ROWS,))
    inverse = np.empty(ROWS, dtype=np.int64)
    inverse[sq8["id"]] = np.arange(ROWS)
    if not np.array_equal(np.sort(sq8["id"]), np.arange(ROWS)):
        raise ValueError("SQ8 ID permutation differs")
    values = {arm: {"source": [], "sq8": [], "physical": [],
                    "bytes": [], "gets": [], "union": [],
                    "sq8_ns": [], "source_ns": []} for arm in ARMS}
    with args.evidence.open("x") as output:
        for ordinal, (record, truth_ids, plan) in enumerate(zip(replayed, gold, plans)):
            if (record["query_ordinal"] != ordinal
                    or record["source_query_ordinal"] != FIRST + ordinal
                    or plan["query_ordinal"] != ordinal
                    or len(set(map(int, truth_ids))) != 100):
                raise ValueError("reduced query identity differs")
            gold_set = set(map(int, truth_ids))
            evidence = {"query_ordinal": ordinal, "arms": {}}
            for arm in ARMS:
                result = record["arms"][arm]
                source_ids = result["source_top100_ids"]
                sq8_ids = result["sq8_top100_ids"]
                if (len(source_ids) != 100 or len(set(source_ids)) != 100
                        or len(sq8_ids) != 100 or len(set(sq8_ids)) != 100):
                    raise ValueError("returned IDs differ")
                source_hits = len(set(source_ids) & gold_set)
                sq8_hits = len(set(sq8_ids) & gold_set)
                physical = sum(
                    any(first <= int(inverse[int(source_id)]) * dtype.itemsize < last
                        for first, last in result["ranges"])
                    for source_id in truth_ids
                )
                if sq8_hits > physical:
                    raise ValueError("SQ8 hits exceed fetched physical coverage")
                evidence["arms"][arm] = {
                    "source_hits": source_hits, "sq8_hits": sq8_hits,
                    "physical_hits": physical,
                }
                for key, value in (
                    ("source", source_hits), ("sq8", sq8_hits),
                    ("physical", physical), ("bytes", result["planned_bytes"]),
                    ("gets", result["gets"]), ("union", result["union_size"]),
                    ("sq8_ns", result["sq8_ns"]),
                    ("source_ns", result["source_ns"]),
                ):
                    values[arm][key].append(value)
            output.write(json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n")
    plan_summary = json.loads(args.plan_summary.read_text())
    summary = {
        "schema": "borsuk-v152-returned-quality-v1",
        "split": "deep-image-publication-test-3000-3999-fresh-development",
        "query_count": COUNT,
        "truth_sha256": sealed["truth_sha256"],
        "replay_sha256": replay_seal["replay_sha256"],
        "evidence_sha256": sha256(args.evidence),
        "plan_summary_sha256": sha256(args.plan_summary),
        "arms": {},
    }
    for arm, fields in values.items():
        source_hits = sorted(fields["source"])
        summary["arms"][arm] = {
            "source_hits": sum(source_hits),
            "source_recall_at_100": sum(source_hits) / (COUNT * 100),
            "source_p05_hits": source_hits[49],
            "source_sub90_queries": sum(hits < 90 for hits in source_hits),
            "sq8_hits": sum(fields["sq8"]),
            "physical_hits": sum(fields["physical"]),
            "planned_bytes_total": sum(fields["bytes"]),
            "gets_total": sum(fields["gets"]),
            "mean_gets": sum(fields["gets"]) / COUNT,
            "mean_union_size": sum(fields["union"]) / COUNT,
            "sq8_p95_ms": sorted(fields["sq8_ns"])[949] / 1e6,
            "source_p95_ms": sorted(fields["source_ns"])[949] / 1e6,
        }
    flat = summary["arms"]["flat"]
    candidate = summary["arms"]["v152"]
    paired = [(new - old) for new, old in
              zip(values["v152"]["source"], values["flat"]["source"])]
    summary["v152_vs_flat_paired_source_hits"] = {
        "wins": sum(delta > 0 for delta in paired),
        "ties": sum(delta == 0 for delta in paired),
        "losses": sum(delta < 0 for delta in paired),
        "mean_delta_hits": sum(paired) / COUNT,
    }
    summary["passes_frozen_gate"] = (
        plan_summary["all_primary_retained"] is True
        and plan_summary["all_plan_caps"] is True
        and plan_summary["max_abs_page_score_difference"] <= 0.0001
        and candidate["source_hits"] + 100 >= flat["source_hits"]
        and candidate["source_p05_hits"] >= 98
        and candidate["source_sub90_queries"] == 0
        and plan_summary["v152_p95_ms"] < plan_summary["flat_p95_ms"]
        and candidate["planned_bytes_total"] <= flat["planned_bytes_total"]
        and candidate["gets_total"] <= flat["gets_total"]
    )
    args.summary.write_text(json.dumps(summary, sort_keys=True, separators=(",", ":")) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("replay", "reduce"))
    for name in ("source", "sq8", "low", "step", "queries", "routing", "seal",
                 "plans", "plan_summary", "replay", "replay_seal", "truth",
                 "evidence", "summary"):
        parser.add_argument(f"--{name.replace('_', '-')}", type=Path)
    args = parser.parse_args()
    if args.phase == "replay":
        replay(args)
    else:
        reduce(args)


if __name__ == "__main__":
    main()
