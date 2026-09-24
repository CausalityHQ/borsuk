#!/usr/bin/env python3
"""Seal V154 flat/sparse returned IDs, then reduce against ReLAION GT100."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

ROWS = 1_000_000
DIMS = 768
QUERIES = 1000
ROW_BYTES = DIMS + 12
REQUEST_SHA = "c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9"
SEALED_SHA = "3bfd155ac5f9e1b7aacbc263e1732e2314c9722f235d3454e0f17d0c6bc3c960"
V140_SHA = "3dc54d101814b0f8e2b2d80f1b79e2762edcc885ad423ee14ae601669eb0804a"
V154_SHA = "ab9bac04c32445a85928dd8978468b1c40d937da2f89960e71d8d21d132ea4d1"
V142_SCORED_SHA = "a8288b8e050631c564bdecb87aecb506932e61435a340168f3a4cd86ba53ee0a"
SOURCE_SHA = "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86"
LAYOUT_SHA = "32cba9690cd9d0ed3809763e5a0fa3574b09a207da26e93404651acaa1a66a0b"
SQ8_SHA = "2284f24745f964ff2b125eedb883d5cd8ff6afab0738f49f9e16e593167a318b"
TRUTH_SHA = "bf0fb0c934c986d05282e3d1c63dc351c553976ea05bfcab0cd3f06d2979e871"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def records(path: Path, expected_sha: str | None = None) -> list[dict]:
    if expected_sha and sha256(path) != expected_sha:
        raise ValueError(f"frozen input differs: {path}")
    with path.open() as source:
        values = [json.loads(line) for line in source]
    if len(values) != QUERIES:
        raise ValueError(f"record count differs: {path}")
    return values


def validate_plan(plan: dict, ordinal: int) -> None:
    if (plan["query_ordinal"] != ordinal
            or plan["source_query_ordinal"] != ordinal):
        raise ValueError("V154 query identity differs")
    for arm in (plan["flat_plan"], plan["v152"]["plan"]):
        ranges = arm["ranges"]
        if (not 1 <= len(ranges) <= 32
                or sum(end - start for start, end in ranges) != arm["planned_bytes"]
                or arm["gets"] != len(ranges)
                or arm["planned_bytes"] > 16_777_216):
            raise ValueError("V154 transport plan differs")


def prepare(args: argparse.Namespace) -> None:
    plans = records(args.plans, V154_SHA)
    v140 = records(args.v140_raw, V140_SHA)
    with args.sparse_raw.open("x") as output:
        for ordinal, (plan, prior) in enumerate(zip(plans, v140)):
            validate_plan(plan, ordinal)
            if prior["query_ordinal"] != ordinal:
                raise ValueError("V140 query identity differs")
            flat = plan["flat_plan"]
            baseline = prior["variants"]["4"]
            for field in ("selected_pages", "ranges", "planned_bytes", "gets",
                          "target_pages", "target_shortfall"):
                if flat[field] != baseline[field]:
                    raise ValueError(f"V154 flat plan differs from V140: {ordinal}, {field}")
            output.write(json.dumps({"query_ordinal": ordinal,
                                     "variants": {"4": plan["v152"]["plan"]}},
                                    sort_keys=True, separators=(",", ":")) + "\n")


def replay(args: argparse.Namespace) -> None:
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    from scripts.v124_source_tier_precision import rank, unit

    for path, expected in ((args.source, SOURCE_SHA), (args.layout, LAYOUT_SHA),
                           (args.sq8, SQ8_SHA)):
        if sha256(path) != expected:
            raise ValueError(f"source/layout/SQ8 differs: {path}")
    dtype = np.dtype([("id", "<i8"), ("norm", "<f4"), ("code", "u1", (DIMS,))])
    if args.sq8.stat().st_size != ROWS * dtype.itemsize:
        raise ValueError("SQ8 geometry differs")
    sq8 = np.memmap(args.sq8, dtype=dtype, mode="r", shape=(ROWS,))
    table = pq.read_table(args.source, columns=["feature_row_id", "embedding"])
    embedding = table.schema.field("embedding").type
    if (table.num_rows != ROWS or not pa.types.is_fixed_size_list(embedding)
            or embedding.list_size != DIMS or embedding.value_type != pa.float32()):
        raise ValueError("source geometry differs")
    source_ids = table["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)
    if not np.issubdtype(source_ids.dtype, np.integer) or np.unique(source_ids).size != ROWS:
        raise ValueError("source IDs differ")
    source = np.asarray(table["embedding"].combine_chunks().values.to_numpy(
        zero_copy_only=False), dtype=np.float32).reshape(ROWS, DIMS)
    if not np.isfinite(source).all():
        raise ValueError("source coordinates differ")
    layout = np.load(args.layout, mmap_mode="r", allow_pickle=False)
    if (layout.shape != (ROWS,) or layout.dtype not in (np.dtype("int32"), np.dtype("int64"))
            or layout.min() != 0 or layout.max() != ROWS - 1
            or np.unique(layout).size != ROWS
            or not np.array_equal(sq8["id"], source_ids[layout])):
        raise ValueError("SQ8, layout and source-ID mapping differ")
    ids_to_rows = {int(value): ordinal for ordinal, value in enumerate(source_ids)}
    requests = records(args.requests, REQUEST_SHA)
    sealed = records(args.sealed, SEALED_SHA)
    plans = records(args.plans, V154_SHA)
    flat_scored = records(args.flat_scored)
    sparse_scored = records(args.sparse_scored)
    historical = records(args.v142_scored, V142_SCORED_SHA)
    with args.replay.open("x") as output:
        for ordinal, (request, old, plan, flat, sparse, reference) in enumerate(
                zip(requests, sealed, plans, flat_scored, sparse_scored, historical)):
            if any(row["query_ordinal"] != ordinal
                   for row in (request, old, plan, flat, sparse, reference)):
                raise ValueError("query identity differs")
            if (request["nominees"] != old["nominees"]
                    or request["baseline_ranges"] != old["baseline_ranges"]
                    or len(request["nominees"]) != 512
                    or len(set(request["nominees"])) != 512):
                raise ValueError("source-only router differs")
            validate_plan(plan, ordinal)
            if (flat["arms"]["beta4"]["sq8_top512_ids"]
                    != reference["arms"]["beta4"]["sq8_top512_ids"]):
                raise ValueError(f"V142 flat returned-ID parity differs: {ordinal}")
            query = np.asarray(request["query"], dtype=np.float64)
            if (query.shape != (DIMS,) or not np.isfinite(query).all()
                    or np.linalg.norm(query) <= 0):
                raise ValueError("query differs")
            query /= np.linalg.norm(query)
            nominee_ids = set(map(int, sq8["id"][request["nominees"]]))
            result = {"query_ordinal": ordinal, "arms": {}}
            for name, scored, expected in (
                    ("flat", flat["arms"]["beta4"], plan["flat_plan"]),
                    ("sparse", sparse["arms"]["beta4"], plan["v152"]["plan"])):
                if (scored["ranges"] != expected["ranges"]
                        or scored["planned_bytes"] != expected["planned_bytes"]
                        or scored["gets"] != expected["gets"]):
                    raise ValueError(f"Rust returned plan differs: {name}, {ordinal}")
                returned = scored["sq8_top512_ids"]
                if len(returned) != 512 or len(set(returned)) != 512:
                    raise ValueError("SQ8 returned width differs")
                union = np.asarray(sorted(nominee_ids | set(returned)), dtype=np.int64)
                if not 512 <= union.size <= 1024:
                    raise ValueError("source union width differs")
                source_rows = np.fromiter((ids_to_rows[int(value)] for value in union),
                                          dtype=np.int64, count=union.size)
                started = time.perf_counter_ns()
                exact = rank(union, unit(source[source_rows].astype(np.float64)) @ query, 100)
                source_ns = time.perf_counter_ns() - started
                result["arms"][name] = {
                    "ranges": expected["ranges"], "gets": expected["gets"],
                    "planned_bytes": expected["planned_bytes"],
                    "union_size": int(union.size),
                    "sq8_top512_ids": returned,
                    "sq8_top100_ids": returned[:100],
                    "source_top100_ids": exact.tolist(),
                    "sq8_ns": scored["sq8_ns"], "source_ns": source_ns,
                }
            output.write(json.dumps(result, sort_keys=True,
                                    separators=(",", ":")) + "\n")


def reduce(args: argparse.Namespace) -> None:
    import numpy as np
    from scripts.v124_source_tier_precision import load_truth
    seal = json.loads(args.seal.read_text())
    if (seal.get("schema") != "borsuk-v155-replay-seal-v1"
            or seal.get("gt_opened") is not False
            or seal.get("plans_sha256") != V154_SHA
            or seal.get("replay_sha256") != sha256(args.replay)):
        raise ValueError("GT-blind returned-ID seal differs")
    if sha256(args.truth) != TRUTH_SHA or sha256(args.sq8) != SQ8_SHA:
        raise ValueError("GT or SQ8 identity differs")
    dtype = np.dtype([("id", "<i8"), ("norm", "<f4"), ("code", "u1", (DIMS,))])
    sq8 = np.memmap(args.sq8, dtype=dtype, mode="r", shape=(ROWS,))
    if np.unique(sq8["id"]).size != ROWS:
        raise ValueError("SQ8 source IDs differ")
    position_by_id = {int(value): ordinal for ordinal, value in enumerate(sq8["id"])}
    truth = load_truth(args.truth, sq8["id"])
    np.asarray(sq8["id"], dtype="<i8").tofile(args.sq8_ids)
    np.asarray(truth, dtype="<i8").reshape(QUERIES, 100).tofile(args.truth_ids)
    rows = records(args.replay)
    fields = ("source", "sq8", "physical", "bytes", "gets", "union", "sq8_ns", "source_ns")
    values = {arm: {field: [] for field in fields} for arm in ("flat", "sparse")}
    paired = {"wins": 0, "ties": 0, "losses": 0}
    with args.evidence.open("x") as output:
        for ordinal, (row, gold) in enumerate(zip(rows, truth)):
            if row["query_ordinal"] != ordinal:
                raise ValueError("replay query identity differs")
            gold_set = set(map(int, gold[:100]))
            result = {"query_ordinal": ordinal, "arms": {}}
            for name in values:
                arm = row["arms"][name]
                if (len(arm["source_top100_ids"]) != 100
                        or len(set(arm["source_top100_ids"])) != 100
                        or len(arm["sq8_top100_ids"]) != 100
                        or len(set(arm["sq8_top100_ids"])) != 100):
                    raise ValueError("returned top-100 ID roster differs")
                source_hits = len(set(arm["source_top100_ids"]) & gold_set)
                sq8_hits = len(set(arm["sq8_top100_ids"]) & gold_set)
                physical_hits = sum(any(first <= position_by_id[int(g)] * ROW_BYTES < last
                                        for first, last in arm["ranges"]) for g in gold_set)
                if sq8_hits > physical_hits:
                    raise ValueError("SQ8 hits exceed fetched physical coverage")
                found = {"source": source_hits, "sq8": sq8_hits,
                         "physical": physical_hits, "bytes": arm["planned_bytes"],
                         "gets": arm["gets"], "union": arm["union_size"],
                         "sq8_ns": arm["sq8_ns"], "source_ns": arm["source_ns"]}
                for field, value in found.items():
                    values[name][field].append(value)
                result["arms"][name] = {"source_hits": source_hits,
                                        "sq8_hits": sq8_hits,
                                        "physical_hits": physical_hits,
                                        "planned_bytes": arm["planned_bytes"],
                                        "gets": arm["gets"]}
            left = result["arms"]["sparse"]["source_hits"]
            right = result["arms"]["flat"]["source_hits"]
            paired["wins" if left > right else "losses" if left < right else "ties"] += 1
            output.write(json.dumps(result, sort_keys=True,
                                    separators=(",", ":")) + "\n")
    if (sum(values["flat"]["source"]) != 99_607
            or sum(values["flat"]["sq8"]) != 99_255):
        raise ValueError("V142 flat baseline reproduction differs")
    summary = {"schema": "borsuk-v155-relaion-returned-quality-v1",
               "dataset": "ReLAION-1M", "split": "validation-1000-already-used",
               "query_count": QUERIES, "replay_sha256": sha256(args.replay),
               "evidence_sha256": sha256(args.evidence),
               "truth_sha256": TRUTH_SHA, "paired_sparse_vs_flat": paired,
               "arms": {}}
    for name, arm in values.items():
        summary["arms"][name] = {
            "source_hits": sum(arm["source"]),
            "source_recall_at_100": sum(arm["source"]) / (QUERIES * 100),
            "source_p05_hits": sorted(arm["source"])[49],
            "source_sub90_queries": sum(n < 90 for n in arm["source"]),
            "sq8_hits": sum(arm["sq8"]),
            "sq8_recall_at_100": sum(arm["sq8"]) / (QUERIES * 100),
            "sq8_p05_hits": sorted(arm["sq8"])[49],
            "sq8_sub90_queries": sum(n < 90 for n in arm["sq8"]),
            "physical_hits": sum(arm["physical"]),
            "physical_coverage_at_100": sum(arm["physical"]) / (QUERIES * 100),
            "planned_bytes_total": sum(arm["bytes"]),
            "mean_planned_bytes": sum(arm["bytes"]) / QUERIES,
            "p95_planned_bytes": sorted(arm["bytes"])[949],
            "gets_total": sum(arm["gets"]),
            "mean_gets": sum(arm["gets"]) / QUERIES,
            "p95_gets": sorted(arm["gets"])[949],
            "mean_union_size": sum(arm["union"]) / QUERIES,
            "sq8_p95_ms": sorted(arm["sq8_ns"])[949] / 1e6,
            "source_p95_ms": sorted(arm["source_ns"])[949] / 1e6,
        }
    flat = summary["arms"]["flat"]
    sparse = summary["arms"]["sparse"]
    summary["passes_frozen_gate"] = (
        sparse["source_hits"] >= 99_500
        and sparse["source_hits"] >= flat["source_hits"] - 100
        and sparse["source_p05_hits"] >= 98
        and sparse["source_sub90_queries"] <= 2
        and sparse["planned_bytes_total"] <= flat["planned_bytes_total"]
        and sparse["gets_total"] <= flat["gets_total"]
    )
    args.summary.write_text(json.dumps(summary, sort_keys=True,
                                       separators=(",", ":")) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--plans", type=Path, required=True)
    p.add_argument("--v140-raw", type=Path, required=True)
    p.add_argument("--sparse-raw", type=Path, required=True)
    p.set_defaults(function=prepare)
    p = sub.add_parser("replay")
    for name in ("source", "layout", "sq8", "requests", "sealed", "plans",
                 "flat_scored", "sparse_scored", "v142_scored", "replay"):
        p.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    p.set_defaults(function=replay)
    p = sub.add_parser("reduce")
    for name in ("replay", "seal", "truth", "sq8", "evidence", "summary",
                 "truth_ids", "sq8_ids"):
        p.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    p.set_defaults(function=reduce)
    args = parser.parse_args()
    args.function(args)


if __name__ == "__main__":
    main()
