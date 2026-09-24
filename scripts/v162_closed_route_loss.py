#!/usr/bin/env python3
"""Postterminal V161 1M page-headroom and admission decomposition."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import membership_schema
from scripts.v124_source_tier_precision import load_truth
from scripts.v155_relaion_returned_quality import (
    LAYOUT_SHA, QUERIES, SEALED_SHA, TRUTH_SHA, sha256,
)

ROWS = 1_000_000
DIMS = 768
PAGE_ROWS = 512
ROW_BYTES = DIMS + 12
CAP_BYTES = 16_777_216
CAP_GETS = 32
SCHEMA = "borsuk-v162-closed-1m-route-loss-v1"
MEMBERSHIP_SHA = "9e9f4a00b2ca802bdab694315a08362add45c20217fe70dd34c1bcce480508f4"
LAYOUT_SEAL_SHA = "ac4664e4a5969ee63428c9924e0e4fb0aa6b0137fd2de6bd9900bef71175e560"
PLAN_SEAL_SHA = "0744a76e9f7d58508a52a26301b5d5573ad929c0c8a239c9ba7a47362144b82e"
PLANS_SHA = "6e2e6e99a2da8686782d761822e4634afdc573d2b33e632c559f2a307d7cc9b9"
V161_RAW_SHA = "838108206bd8e42add8730993a1b36adf2747e2032461b667a532de48a1a18c1"
V161_TERMINAL_SHA = "7b370d8e3500ea5be069b5a5d0712d247b5c4c6c9b82f8a2dccc0032132676cf"
V155_TERMINAL_SHA = "784097f577f11bd49468473e43b1ba06642bf107ecde06a8b0b0ce09b0cd9cdb"
V155_EVIDENCE_SHA = "dd4d4a7c9448ce6833357e70bff5f72bd9a782afd8185114cb501b2a63a49a1a"
SOURCE_SHA = "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86"
FIELDS = ("nominee_rows", "nominee_pages", "primary_rows", "primary_pages",
          "final_plan", "v155_final_plan", "primary_distinct_pages",
          "primary_min_cover_bytes", "primary_original_runs", "primary_cover_gets")


def canonical(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def jsonl(path: Path):
    with path.open() as source:
        for line in source:
            yield json.loads(line)


def spread(values: list[int]) -> dict:
    ordered = sorted(values)
    return {"min": ordered[0], "p05": ordered[49], "p50": ordered[499],
            "p95": ordered[949], "p99": ordered[989], "max": ordered[-1],
            "sum": sum(ordered)}


def minimum_cover(pages: set[int]) -> tuple[int, int, int]:
    ordered = sorted(pages)
    if not ordered or ordered[-1] >= math.ceil(ROWS / PAGE_ROWS):
        raise ValueError("primary page geometry differs")
    runs: list[tuple[int, int]] = []
    for page in ordered:
        if runs and runs[-1][1] == page:
            runs[-1] = (runs[-1][0], page + 1)
        else:
            runs.append((page, page + 1))
    original_runs = len(runs)
    joins = max(0, original_runs - CAP_GETS)
    gaps = sorted((runs[index + 1][0] - runs[index][1], index)
                  for index in range(len(runs) - 1))
    bridged = {index for _, index in gaps[:joins]}
    cover: list[tuple[int, int]] = []
    for index, run in enumerate(runs):
        if cover and index - 1 in bridged:
            cover[-1] = (cover[-1][0], run[1])
        else:
            cover.append(run)
    amount = sum((min(end * PAGE_ROWS, ROWS) - start * PAGE_ROWS) * ROW_BYTES
                 for start, end in cover)
    if not 1 <= len(cover) <= CAP_GETS or amount <= 0:
        raise ValueError("minimum page cover differs")
    return amount, original_runs, len(cover)


def authenticated_maps(membership: Path, layout: Path, layout_seal: Path,
                       plan_seal: Path, plans: Path) -> tuple[np.ndarray, np.ndarray,
                                                                 np.ndarray]:
    import pyarrow.parquet as pq

    for path, digest in ((membership, MEMBERSHIP_SHA), (layout, LAYOUT_SHA),
                         (layout_seal, LAYOUT_SEAL_SHA),
                         (plan_seal, PLAN_SEAL_SHA), (plans, PLANS_SHA)):
        if sha256(path) != digest:
            raise ValueError(f"closed mapping/plan identity differs: {path}")
    authority = json.loads(layout_seal.read_text())
    plan_authority = json.loads(plan_seal.read_text())
    if (authority.get("membership_sha256") != MEMBERSHIP_SHA
            or authority.get("page_rows") != PAGE_ROWS
            or authority.get("rows") != ROWS
            or authority.get("source_sha256") != SOURCE_SHA
            or plan_authority.get("layout_seal_sha256") != LAYOUT_SEAL_SHA
            or plan_authority.get("plans_sha256") != PLANS_SHA
            or plan_authority.get("gt_opened") is not False):
        raise ValueError("V161 GT-blind construction/plan seal differs")
    if pq.read_schema(membership) != membership_schema():
        raise ValueError("V161 membership physical schema differs")
    table = pq.read_table(membership)
    if table.num_rows != ROWS:
        raise ValueError("V161 membership row count differs")
    source_ordinals = table["source_ordinal"].combine_chunks().to_numpy(
        zero_copy_only=False).astype(np.int64)
    stable_ids = table["stable_id"].combine_chunks().to_pylist()
    page_ordinals = table["page_ordinal"].combine_chunks().to_numpy(
        zero_copy_only=False)
    in_page = table["in_page_ordinal"].combine_chunks().to_numpy(
        zero_copy_only=False)
    source_digests = table["source_sha256"].combine_chunks().to_pylist()
    methods = table["method"].combine_chunks().to_pylist()
    if (not np.array_equal(np.sort(source_ordinals), np.arange(ROWS))
            or any(value != bytes.fromhex(SOURCE_SHA) for value in source_digests)
            or any(value != "balanced-two-means-480k" for value in methods)
            or any((page_ordinals[i], in_page[i]) >
                   (page_ordinals[i + 1], in_page[i + 1]) for i in range(ROWS - 1))):
        raise ValueError("V161 membership source order/authority differs")
    source_ids = np.empty(ROWS, dtype=np.int64)
    for physical, (source_ordinal, stable_id) in enumerate(zip(source_ordinals, stable_ids)):
        identifier = int(stable_id)
        if str(identifier).encode() != stable_id:
            raise ValueError("V161 stable ID encoding differs")
        source_ids[source_ordinal] = identifier
    if len(np.unique(source_ids)) != ROWS:
        raise ValueError("V161 stable IDs are not unique")
    new_physical = np.empty(ROWS, dtype=np.int64)
    new_physical[source_ordinals] = np.arange(ROWS, dtype=np.int64)
    old_layout = np.load(layout, mmap_mode="r", allow_pickle=False)
    if (old_layout.shape != (ROWS,)
            or not np.array_equal(np.sort(old_layout), np.arange(ROWS))):
        raise ValueError("V63 source permutation differs")
    return source_ids, new_physical, old_layout


def evaluate(membership: Path, layout: Path, layout_seal: Path, plan_seal: Path,
             plans: Path, sealed: Path, truth_path: Path, v161_terminal: Path,
             v161_raw: Path, v155_terminal: Path, v155_evidence: Path,
             raw_path: Path, summary_path: Path) -> None:
    source_ids, new_physical, old_layout = authenticated_maps(
        membership, layout, layout_seal, plan_seal, plans,
    )
    for path, digest in ((sealed, SEALED_SHA), (truth_path, TRUTH_SHA),
                         (v161_terminal, V161_TERMINAL_SHA),
                         (v161_raw, V161_RAW_SHA),
                         (v155_terminal, V155_TERMINAL_SHA),
                         (v155_evidence, V155_EVIDENCE_SHA)):
        if sha256(path) != digest:
            raise ValueError(f"closed cohort/terminal identity differs: {path}")
    t161 = json.loads(v161_terminal.read_text())
    t155 = json.loads(v155_terminal.read_text())
    if (t161.get("status") != "complete" or t155.get("status") != "complete"
            or t161.get("artifacts", {}).get("raw.jsonl", {}).get("sha256")
                != V161_RAW_SHA
            or t155.get("artifacts", {}).get("evidence.jsonl", {}).get("sha256")
                != V155_EVIDENCE_SHA):
        raise ValueError("closed terminal does not bind evidence")
    gold = load_truth(truth_path, source_ids)
    id_to_source = {int(identifier): i for i, identifier in enumerate(source_ids)}
    metrics = {name: [] for name in FIELDS}
    count = 0
    with raw_path.open("x") as output:
        for roster, planned, prior, control in itertools.zip_longest(
                jsonl(sealed), jsonl(plans), jsonl(v161_raw), jsonl(v155_evidence)):
            if any(row is None or row.get("query_ordinal") != count
                   for row in (roster, planned, prior, control)):
                raise ValueError("V162 closed query order differs")
            nominees = roster["nominees"]
            primary = roster["primary"]
            if (len(nominees) != 512 or len(primary) != 100
                    or not set(primary).issubset(nominees)):
                raise ValueError("V116 roster geometry differs")
            nominee_ids = {int(source_ids[old_layout[row]]) for row in nominees}
            primary_ids = {int(source_ids[old_layout[row]]) for row in primary}
            nominee_pages = {int(new_physical[old_layout[row]]) // PAGE_ROWS
                             for row in nominees}
            primary_pages = {int(new_physical[old_layout[row]]) // PAGE_ROWS
                             for row in primary}
            truth_ids = set(map(int, gold[count][:100]))
            def page_hits(pages: set[int]) -> int:
                return sum(int(new_physical[id_to_source[identifier]]) // PAGE_ROWS in pages
                           for identifier in truth_ids)
            ranges = planned["ranges"]
            def fetched(identifier: int) -> bool:
                offset = int(new_physical[id_to_source[identifier]]) * ROW_BYTES
                return any(start <= offset < end for start, end in ranges)
            min_bytes, original_runs, cover_gets = minimum_cover(primary_pages)
            values = {
                "nominee_rows": len(truth_ids & nominee_ids),
                "nominee_pages": page_hits(nominee_pages),
                "primary_rows": len(truth_ids & primary_ids),
                "primary_pages": page_hits(primary_pages),
                "final_plan": sum(fetched(identifier) for identifier in truth_ids),
                "v155_final_plan": control["arms"]["sparse"]["physical_hits"],
                "primary_distinct_pages": len(primary_pages),
                "primary_min_cover_bytes": min_bytes,
                "primary_original_runs": original_runs,
                "primary_cover_gets": cover_gets,
            }
            if (values["final_plan"] != prior["candidate_physical_hits"]
                    or values["v155_final_plan"] != prior["control_physical_hits"]
                    or values["primary_distinct_pages"]
                        != planned["distinct_primary_pages"]
                    or not values["primary_rows"] <= values["primary_pages"]
                    or not values["nominee_rows"] <= values["nominee_pages"]):
                raise ValueError("V162 closed decomposition differs")
            for name, value in values.items():
                metrics[name].append(value)
            output.write(canonical({"query_ordinal": count, **values}))
            count += 1
    if count != QUERIES:
        raise ValueError("V162 query count differs")
    primary_hits = metrics["primary_pages"]
    final_hits = metrics["final_plan"]
    headroom = sum(primary_hits) >= 99_400 and sorted(primary_hits)[49] >= 97
    final = sum(final_hits) >= 99_400 and sorted(final_hits)[49] >= 97
    classification = ("admission-loss" if headroom and not final else
                      "layout-and-admission-loss" if not headroom and
                      sum(final_hits) < sum(primary_hits) else
                      "layout-headroom-loss" if not headroom else "no-transfer-loss")
    summary_path.write_text(canonical({
        "schema": SCHEMA + "-summary", "dataset": "ReLAION-1M",
        "split": "validation-1000-used", "queries": QUERIES,
        "classification": classification,
        "primary_cover_within_cap": sum(value <= CAP_BYTES for value in
                                            metrics["primary_min_cover_bytes"]),
        "metrics": {name: spread(values) for name, values in metrics.items()},
        "raw_sha256": sha256(raw_path),
        "v161_terminal_sha256": V161_TERMINAL_SHA,
        "v155_terminal_sha256": V155_TERMINAL_SHA,
    }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("membership", "layout", "layout_seal", "plan_seal", "plans",
                 "sealed", "truth", "v161_terminal", "v161_raw",
                 "v155_terminal", "v155_evidence", "raw", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    args = parser.parse_args()
    evaluate(args.membership, args.layout, args.layout_seal, args.plan_seal,
             args.plans, args.sealed, args.truth, args.v161_terminal,
             args.v161_raw, args.v155_terminal, args.v155_evidence,
             args.raw, args.summary)
