#!/usr/bin/env python3
"""GT-blind SQ8 relayout/plan, then a frozen returned-quality reduction."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import (
    ArtifactIdentity, LayoutAuthority, LayoutMethod, read_membership_parquet,
)
from scripts.v114_1m_paired import score_sq8_ranges
from scripts.v114_exact_local_100k import SOURCE_SHA256, route_reference
from scripts.v114_weighted_interval_plan import optimal_weighted_intervals
from scripts.v158_pq_primary_returned import (
    CAP_BYTES, CAP_GETS, DIMS, QUERIES, ROWS, authenticate, jsonl, load_truth,
    sha256, spread,
)

MEMBERSHIP_SHA = "f72b80f1341bd64599e51a69e627f0b9a2280be4f5235a6c5995fef8eacd866c"
MEMBERSHIP_BYTES = 762_442
V158_TERMINAL_SHA = "09306fa1aca94748635eca32ac9259e934249ee1fb620c3ec333977517a2dd35"
V158_RAW_SHA = "12a77f91b6b0898ae2b9ed17f0450556eec7bf0a2bdc9d2f4130bd71193e00ba"
SOURCE_URI = ("s3://borsuk-bench-453182569524-euc1/research/"
              "v85-pq16-page-nomination/24383d853474a19702d18d2de700bee3618167f5/"
              "100k-a0023/attempt/inputs/source-100k.parquet")
ROW_BYTES = DIMS + 12
DTYPE = np.dtype([("id", "<i8"), ("norm", "<f4"), ("code", "u1", (DIMS,))])
SCHEMA = "borsuk-v160-relayout-primary-v1"


def canonical(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def page_rows() -> int:
    maximum = CAP_BYTES // (CAP_GETS * ROW_BYTES)
    if maximum < 32:
        raise ValueError("transport cap cannot fit one 32-row unit per GET")
    return 1 << (maximum.bit_length() - 1)


def membership_order(membership: Path, membership_seal: Path,
                     old_sq8: np.memmap) -> np.ndarray:
    if membership.stat().st_size != MEMBERSHIP_BYTES or sha256(membership) != MEMBERSHIP_SHA:
        raise ValueError("closed geometric membership identity differs")
    declared = json.loads(membership_seal.read_text())
    if (declared.get("schema") != "borsuk-native-geometric-layout-membership-seal-v1"
            or not any(item.get("sha256") == MEMBERSHIP_SHA
                       and item.get("encoded_bytes") == MEMBERSHIP_BYTES
                       and item.get("role") == "membership:balanced-two-means-480k"
                       for item in declared.get("memberships", []))):
        raise ValueError("closed membership seal differs")
    authority = LayoutAuthority(
        "borsuk-native-geometric-layout-authority-v1",
        ArtifactIdentity("source", SOURCE_URI, SOURCE_SHA256, 145_121_661),
        ROWS, DIMS, "l2", 20260921, LayoutMethod.TWO_MEANS_480K,
        65535, 491520,
    )
    source_ids = tuple(str(int(identifier)).encode() for identifier in old_sq8["id"])
    rows = read_membership_parquet(membership, authority, source_ids)
    order = np.fromiter((row.source_ordinal for row in rows), dtype=np.int64, count=ROWS)
    if (order.size != ROWS or not np.array_equal(np.sort(order), np.arange(ROWS))):
        raise ValueError("geometric membership is not a source permutation")
    return order


def construct(old_sq8_path: Path, old_manifest: Path, membership: Path,
              membership_seal: Path,
              new_sq8_path: Path, layout_seal: Path) -> None:
    authenticate(old_sq8_path, "sq8")
    authenticate(old_manifest, "manifest")
    old = np.memmap(old_sq8_path, dtype=DTYPE, mode="r", shape=(ROWS,))
    authority = json.loads(old_manifest.read_text())
    if (authority.get("object_sha256") != sha256(old_sq8_path)
            or authority.get("geometry") != {"rows": ROWS, "dimensions": DIMS}):
        raise ValueError("V114 source mirror authority differs")
    order = membership_order(membership, membership_seal, old)
    with new_sq8_path.open("xb") as output:
        for start in range(0, ROWS, 4096):
            output.write(old[order[start:start + 4096]].tobytes())
    if new_sq8_path.stat().st_size != ROWS * ROW_BYTES:
        raise ValueError("relayout SQ8 length differs")
    layout_seal.write_text(canonical({
        "schema": SCHEMA + "-layout-seal", "gt_opened": False,
        "source_sha256": SOURCE_SHA256, "membership_sha256": MEMBERSHIP_SHA,
        "membership_seal_sha256": sha256(membership_seal),
        "old_sq8_sha256": sha256(old_sq8_path),
        "new_sq8_sha256": sha256(new_sq8_path),
        "order_sha256": hashlib.sha256(order.astype("<i8").tobytes()).hexdigest(),
        "page_rows": page_rows(), "rows": ROWS, "dimensions": DIMS,
        "row_bytes": ROW_BYTES, "cap_gets": CAP_GETS, "cap_bytes": CAP_BYTES,
    }))


def route(primary: list[int], nominees: list[int], inverse: np.ndarray,
          width: int) -> tuple[list[list[int]], int, int, int]:
    if (len(primary) != 100 or len(nominees) != 512
            or len(set(primary)) != 100 or len(set(nominees)) != 512
            or not set(primary).issubset(nominees)
            or any(type(row) is not int or not 0 <= row < ROWS
                   for row in primary + nominees)):
        raise ValueError("frozen roster geometry differs")
    primary_set = set(primary)
    votes: dict[int, int] = {}
    for old in nominees:
        page = int(inverse[old]) // width
        votes[page] = votes.get(page, 0) + (513 if old in primary_set else 1)
    unit_bytes = 32 * ROW_BYTES
    final_rows = ROWS % width or width
    if final_rows % 32:
        raise ValueError("final page is not unit aligned")
    score, intervals = optimal_weighted_intervals(
        votes, page_count=math.ceil(ROWS / width), max_gets=CAP_GETS,
        max_units=CAP_BYTES // unit_bytes, full_page_units=width // 32,
        last_page_units=final_rows // 32,
    )
    full_bytes = width * ROW_BYTES
    object_bytes = ROWS * ROW_BYTES
    ranges = [[start * full_bytes,
               object_bytes if end == (ROWS - 1) // width else (end + 1) * full_bytes]
              for start, end in intervals]
    amount = sum(end - start for start, end in ranges)
    if (not 1 <= len(ranges) <= CAP_GETS or amount > CAP_BYTES
            or any(left[1] >= right[0] for left, right in zip(ranges, ranges[1:]))):
        raise ValueError("relayout plan exceeds transport cap")
    return ranges, amount, len({int(inverse[row]) // width for row in primary}), score


def plan(requests: Path, reference: Path, old_sq8_path: Path, membership: Path,
         membership_seal: Path,
         new_sq8_path: Path, layout_seal: Path, plans: Path, plan_seal: Path) -> None:
    for role, path in (("requests", requests), ("reference", reference),
                       ("sq8", old_sq8_path)):
        authenticate(path, role)
    layout = json.loads(layout_seal.read_text())
    if (layout.get("gt_opened") is not False
            or layout.get("new_sq8_sha256") != sha256(new_sq8_path)
            or layout.get("page_rows") != page_rows()):
        raise ValueError("relayout construction seal differs")
    old = np.memmap(old_sq8_path, dtype=DTYPE, mode="r", shape=(ROWS,))
    order = membership_order(membership, membership_seal, old)
    inverse = np.empty(ROWS, dtype=np.int64)
    inverse[order] = np.arange(ROWS, dtype=np.int64)
    with plans.open("x") as output:
        count = 0
        for request, ref in itertools.zip_longest(jsonl(requests), jsonl(reference)):
            if request is None or ref is None or any(
                    row.get("query_ordinal") != count for row in (request, ref)):
                raise ValueError("frozen request/reference count or order differs")
            nominees, primary = request["nominees"], ref["primary"]
            _, old_ranges, old_amount, old_score = route_reference(
                primary, nominees, rows=ROWS, dimensions=DIMS,
            )
            if (old_ranges != ref["ranges"] or old_amount != ref["plan_bytes"]
                    or old_score != ref["plan_score"]):
                raise ValueError("source-order exact-primary replay differs")
            ranges, amount, distinct, score = route(primary, nominees, inverse,
                                                    page_rows())
            output.write(canonical({"query_ordinal": count,
                                    "ranges": ranges, "bytes": amount,
                                    "distinct_primary_pages": distinct,
                                    "plan_score": score}))
            count += 1
    if count != QUERIES:
        raise ValueError("frozen query count differs")
    plan_seal.write_text(canonical({
        "schema": SCHEMA + "-plan-seal", "gt_opened": False,
        "queries": count, "layout_seal_sha256": sha256(layout_seal),
        "requests_sha256": sha256(requests),
        "reference_sha256": sha256(reference),
        "plans_sha256": sha256(plans),
    }))


def reduce(requests: Path, plans: Path, plan_seal: Path, layout_seal: Path,
           new_sq8_path: Path,
           old_manifest: Path, truth_path: Path, v158_terminal: Path,
           v158_raw: Path, raw: Path, summary: Path) -> None:
    authenticate(requests, "requests")
    authenticate(old_manifest, "manifest")
    authenticate(truth_path, "truth")
    if (sha256(v158_terminal) != V158_TERMINAL_SHA
            or sha256(v158_raw) != V158_RAW_SHA):
        raise ValueError("closed V158 control identity differs")
    terminal = json.loads(v158_terminal.read_text())
    if (terminal.get("status") != "complete"
            or terminal.get("artifacts", {}).get("raw.jsonl", {}).get("sha256")
                != V158_RAW_SHA):
        raise ValueError("V158 terminal does not bind raw control")
    seal = json.loads(plan_seal.read_text())
    if (seal.get("gt_opened") is not False or seal.get("queries") != QUERIES
            or seal.get("plans_sha256") != sha256(plans)
            or seal.get("requests_sha256") != sha256(requests)
            or seal.get("layout_seal_sha256") != sha256(layout_seal)
            or json.loads(layout_seal.read_text()).get("new_sq8_sha256")
                != sha256(new_sq8_path)):
        raise ValueError("GT-blind plan seal differs")
    manifest = json.loads(old_manifest.read_text())
    low = np.asarray(manifest["low"], dtype=np.float32)
    step = np.asarray(manifest["step"], dtype=np.float32)
    if (low.shape != (DIMS,) or step.shape != (DIMS,)
            or not np.isfinite(low).all() or not np.isfinite(step).all()
            or (step <= 0).any()):
        raise ValueError("SQ8 quantizer differs")
    sq8 = np.memmap(new_sq8_path, dtype=DTYPE, mode="r", shape=(ROWS,))
    gold = load_truth(truth_path)
    positions = {int(identifier): position
                 for position, identifier in enumerate(sq8["id"])}
    if len(positions) != ROWS:
        raise ValueError("relayout SQ8 identifiers are not unique")
    values = {name: [] for name in ("hits100", "hits10", "bytes", "gets",
                                   "distinct_primary_pages", "physical_coverage",
                                   "control_hits100")}
    wins = ties = losses = 0
    with raw.open("x") as output:
        for ordinal, (request, planned, control) in enumerate(itertools.zip_longest(
                jsonl(requests), jsonl(plans), jsonl(v158_raw))):
            if any(row is None or row.get("query_ordinal") != ordinal
                   for row in (request, planned, control)):
                raise ValueError("reduction query order differs")
            query = np.asarray(request["query"], dtype=np.float32)
            if query.shape != (DIMS,) or not np.isfinite(query).all():
                raise ValueError("frozen query vector differs")
            ranges = planned["ranges"]
            if (not 1 <= len(ranges) <= CAP_GETS
                    or planned["bytes"] != sum(end - start for start, end in ranges)
                    or planned["bytes"] > CAP_BYTES
                    or any(start < 0 or start >= end or end > ROWS * ROW_BYTES
                           or start % (page_rows() * ROW_BYTES)
                           or (end != ROWS * ROW_BYTES
                               and end % (page_rows() * ROW_BYTES))
                           for start, end in ranges)
                    or any(left[1] >= right[0] for left, right in zip(ranges, ranges[1:]))):
                raise ValueError("sealed physical ranges differ")
            returned = score_sq8_ranges(sq8, query, low, step, ranges, top_k=100)
            if len(returned) != 100 or len(set(returned)) != 100:
                raise ValueError("returned ID width differs")
            truth100 = set(map(int, gold[ordinal]))
            truth10 = set(map(int, gold[ordinal, :10]))
            hits100 = len(set(returned) & truth100)
            hits10 = len(set(returned[:10]) & truth10)
            physical = sum(any(start <= positions[identifier] * ROW_BYTES < end
                               for start, end in ranges) for identifier in truth100)
            if hits100 > physical or control["arms"]["exact"]["hits"] < 0:
                raise ValueError("physical/control coverage differs")
            baseline = control["arms"]["exact"]["hits"]
            wins += hits100 > baseline
            ties += hits100 == baseline
            losses += hits100 < baseline
            observed = {"hits100": hits100, "hits10": hits10,
                        "bytes": planned["bytes"], "gets": len(ranges),
                        "distinct_primary_pages": planned["distinct_primary_pages"],
                        "physical_coverage": physical,
                        "control_hits100": baseline}
            for name, number in observed.items():
                values[name].append(number)
            output.write(canonical({"query_ordinal": ordinal,
                                    "returned_ids": returned, **observed}))
    if len(values["hits100"]) != QUERIES:
        raise ValueError("reduction query count differs")
    passed = (sum(values["hits100"]) >= 97_500
              and sorted(values["hits100"])[49] >= 90
              and sum(values["hits10"]) >= 9_600)
    summary.write_text(canonical({
        "schema": SCHEMA + "-summary", "queries": QUERIES,
        "dataset": "ReLAION-100k", "split": "development-1000-used",
        "decision": "candidate-advance" if passed else "killed",
        "paired": {"wins": wins, "ties": ties, "losses": losses},
        "metrics": {name: spread(numbers) for name, numbers in values.items()},
        "raw_sha256": sha256(raw), "plan_seal_sha256": sha256(plan_seal),
        "v158_terminal_sha256": V158_TERMINAL_SHA,
    }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("construct", "plan", "reduce"))
    for name in ("requests", "reference", "old_sq8", "old_manifest",
                 "membership", "membership_seal", "new_sq8", "layout_seal", "plans", "plan_seal",
                 "truth", "v158_terminal", "v158_raw", "raw", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path)
    args = parser.parse_args()
    if args.phase == "construct":
        construct(args.old_sq8, args.old_manifest, args.membership,
                  args.membership_seal,
                  args.new_sq8, args.layout_seal)
    elif args.phase == "plan":
        plan(args.requests, args.reference, args.old_sq8, args.membership,
             args.membership_seal,
             args.new_sq8, args.layout_seal, args.plans, args.plan_seal)
    else:
        reduce(args.requests, args.plans, args.plan_seal, args.layout_seal,
               args.new_sq8,
               args.old_manifest, args.truth, args.v158_terminal,
               args.v158_raw, args.raw, args.summary)
