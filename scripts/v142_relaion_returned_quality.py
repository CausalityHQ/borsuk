#!/usr/bin/env python3
"""Seal ReLAION three-arm returned IDs before reducing against GT100."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

import numpy as np

from scripts.v124_source_tier_precision import load_truth, rank, unit

ROWS = 1_000_000
DIMENSIONS = 768
QUERIES = 1000
MAX_BYTES = 16_777_216
REQUESTS_SHA = "c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9"
SEALED_SHA = "3bfd155ac5f9e1b7aacbc263e1732e2314c9722f235d3454e0f17d0c6bc3c960"
V140_RAW_SHA = "3dc54d101814b0f8e2b2d80f1b79e2762edcc885ad423ee14ae601669eb0804a"
TRUTH_SHA = "bf0fb0c934c986d05282e3d1c63dc351c553976ea05bfcab0cd3f06d2979e871"
BASE_SOURCE = {"broad": 99_563, "control": 99_432}
BASE_SQ8 = {"broad": 99_208, "control": 98_618}
ARMS = ("beta4", "broad", "control")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_rows(path: Path, expected_sha: str | None = None) -> list[dict]:
    if expected_sha and sha256(path) != expected_sha:
        raise ValueError(f"sealed input differs: {path}")
    with path.open() as source:
        rows = [json.loads(line) for line in source]
    if len(rows) != QUERIES:
        raise ValueError(f"query row count differs: {path}")
    return rows


def page_ranges(value: object, row_bytes: int) -> tuple[list[list[int]], int]:
    if not isinstance(value, list) or not 1 <= len(value) <= 32:
        raise ValueError("range GET count differs")
    previous = 0
    charged = 0
    for pair in value:
        if (not isinstance(pair, list) or len(pair) != 2
                or not all(type(number) is int for number in pair)):
            raise ValueError("range pair differs")
        first, last = pair
        if (first < previous or first >= last or last > ROWS * row_bytes
                or first % (256 * row_bytes)
                or (last != ROWS * row_bytes and last % (256 * row_bytes))):
            raise ValueError("physical page range differs")
        charged += last - first
        previous = last
    if charged > MAX_BYTES:
        raise ValueError("range byte cap differs")
    return value, charged


def replay(args: argparse.Namespace) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    record = np.dtype([("id", "<i8"), ("norm", "<f4"),
                       ("code", "u1", (DIMENSIONS,))])
    if args.sq8.stat().st_size != ROWS * record.itemsize:
        raise ValueError("SQ8 geometry differs")
    sq8 = np.memmap(args.sq8, dtype=record, mode="r", shape=(ROWS,))
    table = pq.read_table(args.source, columns=["feature_row_id", "embedding"])
    embedding = table.schema.field("embedding").type
    if (table.num_rows != ROWS or not pa.types.is_fixed_size_list(embedding)
            or embedding.list_size != DIMENSIONS
            or embedding.value_type != pa.float32()):
        raise ValueError("source geometry differs")
    source_ids = table["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)
    if (not np.issubdtype(source_ids.dtype, np.integer)
            or np.unique(source_ids).size != ROWS):
        raise ValueError("source IDs differ")
    source = np.asarray(
        table["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False),
        dtype=np.float32).reshape(ROWS, DIMENSIONS)
    if not np.isfinite(source).all():
        raise ValueError("source coordinates differ")
    layout = np.load(args.layout, mmap_mode="r", allow_pickle=False)
    if (layout.shape != (ROWS,) or layout.dtype not in (np.dtype("int32"), np.dtype("int64"))
            or layout.min() != 0 or layout.max() != ROWS - 1
            or np.unique(layout).size != ROWS
            or not np.array_equal(sq8["id"], source_ids[layout])):
        raise ValueError("SQ8, layout, and source-ID mapping differ")
    ids_to_rows = {int(value): ordinal for ordinal, value in enumerate(source_ids)}
    requests = read_rows(args.requests, REQUESTS_SHA)
    sealed = read_rows(args.sealed, SEALED_SHA)
    plans = read_rows(args.v140_raw, V140_RAW_SHA)
    scored = read_rows(args.scored)
    with args.replay.open("x") as output:
        for ordinal, (request, old, plan, rust) in enumerate(
                zip(requests, sealed, plans, scored)):
            if any(row.get("query_ordinal") != ordinal for row in (request, old, plan, rust)):
                raise ValueError("query ordinal differs")
            nominees = request["nominees"]
            if (len(nominees) != 512 or len(set(nominees)) != 512
                    or min(nominees) < 0 or max(nominees) >= ROWS
                    or nominees != old["nominees"]
                    or request["baseline_ranges"] != old["baseline_ranges"]):
                raise ValueError("nominee or fixed-control route differs")
            query = np.asarray(request["query"], dtype=np.float64)
            if (query.shape != (DIMENSIONS,) or not np.isfinite(query).all()
                    or np.linalg.norm(query) <= 0):
                raise ValueError("query differs")
            query /= np.linalg.norm(query)
            nominee_ids = set(map(int, sq8["id"][nominees]))
            beta = plan["variants"]["4"]
            ranges = {"beta4": beta["ranges"], "broad": old["ranges"],
                      "control": old["baseline_ranges"]}
            result = {"query_ordinal": ordinal, "arms": {}}
            for name, raw in ranges.items():
                selected, charged = page_ranges(raw, record.itemsize)
                scored_arm = rust["arms"][name]
                if (scored_arm["ranges"] != selected
                        or scored_arm["gets"] != len(selected)
                        or scored_arm["planned_bytes"] != charged):
                    raise ValueError("Rust fixed-range score route differs")
                if name == "beta4" and (charged != beta["planned_bytes"]
                                        or len(selected) != beta["gets"]):
                    raise ValueError("V140 β4 plan charge differs")
                if name == "broad" and charged != old["plan_bytes"]:
                    raise ValueError("V116 broad plan charge differs")
                if name == "control" and charged != old["baseline_bytes"]:
                    raise ValueError("V116 capped plan charge differs")
                returned = scored_arm["sq8_top512_ids"]
                sq8_ns = scored_arm["sq8_ns"]
                if len(returned) != 512 or len(set(returned)) != 512:
                    raise ValueError("fetched SQ8 width differs")
                historical_field = ("returned_ids" if name == "broad" else
                                    "baseline_returned_ids" if name == "control" else None)
                if historical_field and set(returned[:100]) != set(old[historical_field]):
                    raise ValueError(f"V116 SQ8 top-100 set parity differs: {name}, {ordinal}")
                union = np.asarray(sorted(nominee_ids | set(returned)), dtype=np.int64)
                if not 512 <= union.size <= 1024:
                    raise ValueError("source union width differs")
                rows = np.fromiter((ids_to_rows[int(value)] for value in union),
                                   dtype=np.int64, count=union.size)
                started = time.perf_counter_ns()
                exact = rank(union, unit(source[rows].astype(np.float64)) @ query, 100)
                source_ns = time.perf_counter_ns() - started
                result["arms"][name] = {
                    "ranges": selected, "gets": len(selected),
                    "planned_bytes": charged, "union_size": int(union.size),
                    "sq8_top100_ids": returned[:100],
                    "source_top100_ids": exact.tolist(),
                    "sq8_ns": sq8_ns, "source_ns": source_ns,
                }
            output.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")


def reduce(args: argparse.Namespace) -> None:
    if sha256(args.truth) != TRUTH_SHA:
        raise ValueError("GT identity differs")
    rows = read_rows(args.replay)
    record = np.dtype([("id", "<i8"), ("norm", "<f4"),
                       ("code", "u1", (DIMENSIONS,))])
    if args.sq8.stat().st_size != ROWS * record.itemsize:
        raise ValueError("SQ8 geometry differs at reduction")
    sq8 = np.memmap(args.sq8, dtype=record, mode="r", shape=(ROWS,))
    if np.unique(sq8["id"]).size != ROWS:
        raise ValueError("SQ8 source ID permutation differs")
    position_by_id = {int(value): ordinal for ordinal, value in enumerate(sq8["id"])}
    truth = load_truth(args.truth, sq8["id"])
    counts = {name: {field: [] for field in
                     ("source", "sq8", "physical", "bytes", "gets", "union",
                      "sq8_ns", "source_ns")}
              for name in ARMS}
    paired = {name: {"wins": 0, "ties": 0, "losses": 0}
              for name in ("broad", "control")}
    with args.evidence.open("x") as output:
        for ordinal, (row, gold) in enumerate(zip(rows, truth)):
            if row["query_ordinal"] != ordinal:
                raise ValueError("replay query identity differs")
            gold_set = set(map(int, gold[:100]))
            result = {"query_ordinal": ordinal, "arms": {}}
            for name in ARMS:
                arm = row["arms"][name]
                source = arm["source_top100_ids"]
                sq8_ids = arm["sq8_top100_ids"]
                if len(source) != 100 or len(set(source)) != 100 or len(set(sq8_ids)) != 100:
                    raise ValueError("returned ID roster differs")
                source_hits = len(set(source) & gold_set)
                sq8_hits = len(set(sq8_ids) & gold_set)
                physical_hits = sum(
                    any(first <= position_by_id[int(g)] * record.itemsize < last
                        for first, last in arm["ranges"])
                    for g in gold_set)
                if sq8_hits > physical_hits:
                    raise ValueError("SQ8 hits exceed fetched physical coverage")
                values = {"source": source_hits, "sq8": sq8_hits,
                          "physical": physical_hits, "bytes": arm["planned_bytes"],
                          "gets": arm["gets"], "union": arm["union_size"],
                          "sq8_ns": arm["sq8_ns"], "source_ns": arm["source_ns"]}
                result["arms"][name] = {"source_hits": source_hits,
                                        "sq8_hits": sq8_hits,
                                        "physical_hits": physical_hits}
                for field, value in values.items():
                    counts[name][field].append(value)
            for baseline in paired:
                a = result["arms"]["beta4"]["source_hits"]
                b = result["arms"][baseline]["source_hits"]
                paired[baseline]["wins" if a > b else "losses" if a < b else "ties"] += 1
            output.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    for name in BASE_SOURCE:
        if (sum(counts[name]["source"]) != BASE_SOURCE[name]
                or sum(counts[name]["sq8"]) != BASE_SQ8[name]):
            raise ValueError(f"historical returned baseline reproduction differs: {name}")
    summary = {"schema": "borsuk-v142-relaion-returned-quality-v1",
               "dataset": "ReLAION-1M", "split": "validation-1000-already-used",
               "query_count": QUERIES, "replay_sha256": sha256(args.replay),
               "evidence_sha256": sha256(args.evidence),
               "paired_beta4_vs_broad": paired["broad"],
               "paired_beta4_vs_control": paired["control"], "arms": {}}
    for name, values in counts.items():
        source = sorted(values["source"])
        sq8 = sorted(values["sq8"])
        summary["arms"][name] = {
            "source_hits": sum(source), "sq8_hits": sum(sq8),
            "physical_hits": sum(values["physical"]),
            "source_p05_hits": source[49],
            "source_sub90_queries": sum(value < 90 for value in source),
            "sq8_p05_hits": sq8[49],
            "mean_planned_bytes": sum(values["bytes"]) / QUERIES,
            "p95_planned_bytes": sorted(values["bytes"])[949],
            "mean_gets": sum(values["gets"]) / QUERIES,
            "p95_gets": sorted(values["gets"])[949],
            "mean_union_size": sum(values["union"]) / QUERIES,
            "sq8_p95_ms": sorted(values["sq8_ns"])[949] / 1e6,
            "source_p95_ms": sorted(values["source_ns"])[949] / 1e6,
        }
    beta = summary["arms"]["beta4"]
    summary["passes_quality_screen"] = (
        beta["source_hits"] >= 99_500
        and beta["source_p05_hits"] >= 98
        and beta["source_sub90_queries"] <= 2
        and beta["source_hits"] >= summary["arms"]["control"]["source_hits"])
    args.summary.write_text(json.dumps(summary, sort_keys=True, separators=(",", ":")) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("replay", "reduce"))
    for name in ("source", "layout", "sq8", "requests",
                 "sealed", "v140_raw", "scored", "replay", "truth", "evidence", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path)
    args = parser.parse_args()
    needed = (("source", "layout", "sq8", "requests", "sealed",
               "v140_raw", "scored", "replay") if args.phase == "replay" else
              ("replay", "sq8", "truth", "evidence", "summary"))
    if any(getattr(args, name) is None for name in needed):
        parser.error("required input differs")
    (replay if args.phase == "replay" else reduce)(args)


if __name__ == "__main__":
    main()
