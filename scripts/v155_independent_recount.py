#!/usr/bin/env python3
"""Recount closed V155 returned IDs, transport and GT hits with stdlib only."""

from __future__ import annotations

import argparse
import hashlib
import json
import struct
from pathlib import Path

ROWS = 1_000_000
QUERIES = 1000
ROW_BYTES = 780
TRUTH_SHA = "bf0fb0c934c986d05282e3d1c63dc351c553976ea05bfcab0cd3f06d2979e871"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def records(path: Path) -> list[dict]:
    with path.open() as source:
        result = [json.loads(line) for line in source]
    if len(result) != QUERIES:
        raise ValueError(f"row count differs: {path}")
    return result


def integers(path: Path, count: int) -> tuple[int, ...]:
    raw = path.read_bytes()
    if len(raw) != count * 8:
        raise ValueError(f"integer artifact size differs: {path}")
    return struct.unpack(f"<{count}q", raw)


def recount(replay_path: Path, evidence_path: Path, summary_path: Path,
            truth_path: Path, sq8_ids_path: Path, seal_path: Path,
            truth_parquet: Path | None = None) -> dict:
    replay = records(replay_path)
    evidence = records(evidence_path)
    summary = json.loads(summary_path.read_text())
    seal = json.loads(seal_path.read_text())
    if (summary["schema"] != "borsuk-v155-relaion-returned-quality-v1"
            or summary["query_count"] != QUERIES
            or summary["replay_sha256"] != sha256(replay_path)
            or summary["evidence_sha256"] != sha256(evidence_path)
            or seal["schema"] != "borsuk-v155-replay-seal-v1"
            or seal["gt_opened"] is not False
            or seal["replay_sha256"] != sha256(replay_path)):
        raise ValueError("V155 replay or summary seal differs")
    truth = integers(truth_path, QUERIES * 100)
    sq8_ids = integers(sq8_ids_path, ROWS)
    if truth_parquet is not None:
        if sha256(truth_parquet) != TRUTH_SHA:
            raise ValueError("GT100 parquet identity differs")
        import numpy as np
        from scripts.v124_source_tier_precision import load_truth
        independently_loaded = np.asarray(
            load_truth(truth_parquet, np.asarray(sq8_ids, dtype=np.int64)),
            dtype=np.int64).reshape(QUERIES * 100)
        if not np.array_equal(independently_loaded, np.asarray(truth, dtype=np.int64)):
            raise ValueError("worker-exported GT IDs differ from authenticated parquet")
    if len(set(sq8_ids)) != ROWS:
        raise ValueError("SQ8 source-ID permutation differs")
    position = {source_id: ordinal for ordinal, source_id in enumerate(sq8_ids)}
    fields = ("source", "sq8", "physical", "bytes", "gets", "union",
              "sq8_ns", "source_ns")
    values = {arm: {field: [] for field in fields} for arm in ("flat", "sparse")}
    paired = {"wins": 0, "ties": 0, "losses": 0}
    for ordinal, (row, observed) in enumerate(zip(replay, evidence)):
        if row["query_ordinal"] != ordinal or observed["query_ordinal"] != ordinal:
            raise ValueError("query identity differs")
        gold = set(truth[ordinal * 100:(ordinal + 1) * 100])
        if len(gold) != 100 or not gold.issubset(position):
            raise ValueError("GT100 IDs differ")
        hits = {}
        for name in values:
            arm = row["arms"][name]
            score_ids = arm["sq8_top512_ids"]
            sq8_top = arm["sq8_top100_ids"]
            source_top = arm["source_top100_ids"]
            if (len(score_ids) != 512 or len(set(score_ids)) != 512
                    or score_ids[:100] != sq8_top
                    or len(source_top) != 100 or len(set(source_top)) != 100
                    or not set(score_ids).issubset(position)
                    or not set(source_top).issubset(position)):
                raise ValueError("ordered returned IDs differ")
            ranges = arm["ranges"]
            if not 1 <= len(ranges) <= 32:
                raise ValueError("GET cap differs")
            prior_end = 0
            charged = 0
            for start, end in ranges:
                if (start < prior_end or start % (256 * ROW_BYTES)
                        or (end != ROWS * ROW_BYTES and end % (256 * ROW_BYTES))
                        or not start < end <= ROWS * ROW_BYTES):
                    raise ValueError("physical range differs")
                charged += end - start
                prior_end = end
            if (charged > 16_777_216 or charged != arm["planned_bytes"]
                    or len(ranges) != arm["gets"]):
                raise ValueError("transport charge differs")
            source_hits = len(set(source_top) & gold)
            sq8_hits = len(set(sq8_top) & gold)
            physical_hits = sum(any(first <= position[g] * ROW_BYTES < last
                                    for first, last in ranges) for g in gold)
            if sq8_hits > physical_hits or observed["arms"][name] != {
                    "source_hits": source_hits, "sq8_hits": sq8_hits,
                    "physical_hits": physical_hits,
                    "planned_bytes": charged, "gets": len(ranges)}:
                raise ValueError("returned or physical GT hits differ")
            found = {"source": source_hits, "sq8": sq8_hits,
                     "physical": physical_hits, "bytes": charged,
                     "gets": len(ranges), "union": arm["union_size"],
                     "sq8_ns": arm["sq8_ns"], "source_ns": arm["source_ns"]}
            for field, value in found.items():
                values[name][field].append(value)
            hits[name] = source_hits
        paired["wins" if hits["sparse"] > hits["flat"] else
               "losses" if hits["sparse"] < hits["flat"] else "ties"] += 1
    if summary["paired_sparse_vs_flat"] != paired:
        raise ValueError("paired hit counts differ")
    for name, arm in values.items():
        reported = summary["arms"][name]
        expected = {"source_hits": sum(arm["source"]),
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
                    "source_p95_ms": sorted(arm["source_ns"])[949] / 1e6}
        if reported != expected:
            raise ValueError(f"{name} quality summary differs")
    flat = summary["arms"]["flat"]
    sparse = summary["arms"]["sparse"]
    if flat["source_hits"] != 99_607 or flat["sq8_hits"] != 99_255:
        raise ValueError("V142 flat baseline differs")
    passed = (sparse["source_hits"] >= 99_500
              and sparse["source_hits"] >= flat["source_hits"] - 100
              and sparse["source_p05_hits"] >= 98
              and sparse["source_sub90_queries"] <= 2
              and sparse["planned_bytes_total"] <= flat["planned_bytes_total"]
              and sparse["gets_total"] <= flat["gets_total"])
    if summary["passes_frozen_gate"] is not passed:
        raise ValueError("V155 quality verdict differs")
    return {"verified_queries": QUERIES, "verdict": "pass" if passed else "reject",
            "flat_source_hits": flat["source_hits"],
            "sparse_source_hits": sparse["source_hits"],
            "paired_sparse_vs_flat": paired}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("replay", "evidence", "summary", "truth_ids", "sq8_ids", "seal"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    parser.add_argument("--truth-parquet", type=Path)
    args = parser.parse_args()
    result = recount(args.replay, args.evidence, args.summary,
                     args.truth_ids, args.sq8_ids, args.seal, args.truth_parquet)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
