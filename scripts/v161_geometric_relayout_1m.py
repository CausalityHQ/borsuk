#!/usr/bin/env python3
"""V161 source-only layout, GT-blind exact-primary plan and exact rerank."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import (
    ArtifactIdentity, LayoutAuthority, LayoutMethod, _source_arrays,
    construct_layout, read_membership_parquet, write_membership_parquet,
)
from scripts.v114_1m_paired import score_sq8_ranges
from scripts.v114_weighted_interval_plan import optimal_weighted_intervals
from scripts.v124_source_tier_precision import load_truth, rank, unit
from scripts.v155_relaion_returned_quality import (
    DIMS, LAYOUT_SHA, QUERIES, REQUEST_SHA, ROWS, SEALED_SHA,
    SOURCE_SHA, SQ8_SHA, TRUTH_SHA, sha256,
)

CAP_GETS = 32
CAP_BYTES = 16_777_216
ROW_BYTES = DIMS + 12
SCHEMA = "borsuk-v161-geometric-relayout-1m-v1"
SOURCE_URI = ("s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/"
              "runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet")
SOURCE_BYTES = 1_458_450_077
MANIFEST_SHA = "48e01d4488d0c84b991baef521a02bca75ce096c0f9a1154f779c046732707f8"
V155_TERMINAL_SHA = "784097f577f11bd49468473e43b1ba06642bf107ecde06a8b0b0ce09b0cd9cdb"
V155_REPLAY_SHA = "a4f9e39e674ce22fc65ee82836731a22e2c094f7267439888b45f9320c3cb78f"
V155_EVIDENCE_SHA = "dd4d4a7c9448ce6833357e70bff5f72bd9a782afd8185114cb501b2a63a49a1a"
DTYPE = np.dtype([("id", "<i8"), ("norm", "<f4"), ("code", "u1", (DIMS,))])


def canonical(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def jsonl(path: Path):
    with path.open() as source:
        for line in source:
            yield json.loads(line)


def distribution(values: list[int]) -> dict:
    ordered = sorted(values)
    return {"min": ordered[0], "p05": ordered[49], "p50": ordered[499],
            "p95": ordered[949], "p99": ordered[989], "max": ordered[-1],
            "sum": sum(ordered)}


def page_rows() -> int:
    maximum = CAP_BYTES // (CAP_GETS * ROW_BYTES)
    if maximum < 32:
        raise ValueError("transport cap cannot fit a 32-row unit")
    return 1 << (maximum.bit_length() - 1)


def authority() -> LayoutAuthority:
    return LayoutAuthority(
        "borsuk-native-geometric-layout-authority-v1",
        ArtifactIdentity("source", SOURCE_URI, SOURCE_SHA, SOURCE_BYTES),
        ROWS, DIMS, "cosine", 20260921, LayoutMethod.TWO_MEANS_480K,
        65535, 491520,
    )


def old_layout_and_sq8(layout_path: Path, old_sq8_path: Path,
                       source_ids: np.ndarray | None = None):
    if sha256(layout_path) != LAYOUT_SHA or sha256(old_sq8_path) != SQ8_SHA:
        raise ValueError("frozen V63/V70 identity differs")
    layout = np.load(layout_path, mmap_mode="r", allow_pickle=False)
    sq8 = np.memmap(old_sq8_path, dtype=DTYPE, mode="r", shape=(ROWS,))
    if (layout.shape != (ROWS,) or layout.dtype not in (np.dtype("int32"), np.dtype("int64"))
            or not np.array_equal(np.sort(layout), np.arange(ROWS))
            or old_sq8_path.stat().st_size != ROWS * ROW_BYTES):
        raise ValueError("V63/V70 geometry differs")
    if source_ids is not None and not np.array_equal(sq8["id"], source_ids[layout]):
        raise ValueError("V70 IDs do not match V36/V63 mapping")
    return layout, sq8


def source_arrays(source_path: Path) -> tuple[np.ndarray, np.ndarray, tuple[bytes, ...]]:
    stable_ids, vectors = _source_arrays(source_path, authority())
    source_ids = np.fromiter((int(value) for value in stable_ids),
                             dtype=np.int64, count=ROWS)
    if (len(stable_ids) != ROWS or len(set(stable_ids)) != ROWS
            or len(np.unique(source_ids)) != ROWS):
        raise ValueError("V36 source ID mapping differs")
    return source_ids, vectors, stable_ids


def construct(source_path: Path, old_layout_path: Path, old_sq8_path: Path,
              membership_path: Path, new_sq8_path: Path, layout_seal: Path) -> None:
    source_ids, vectors, stable_ids = source_arrays(source_path)
    old_order, old_sq8 = old_layout_and_sq8(old_layout_path, old_sq8_path, source_ids)
    membership = construct_layout(authority(), stable_ids, vectors)
    identity = write_membership_parquet(membership_path, authority(), membership)
    order = np.fromiter((row.source_ordinal for row in membership),
                        dtype=np.int64, count=ROWS)
    if not np.array_equal(np.sort(order), np.arange(ROWS)):
        raise ValueError("V161 source-only membership is not a permutation")
    old_physical = np.empty(ROWS, dtype=np.int64)
    old_physical[old_order] = np.arange(ROWS, dtype=np.int64)
    with new_sq8_path.open("xb") as output:
        for start in range(0, ROWS, 4096):
            output.write(old_sq8[old_physical[order[start:start + 4096]]].tobytes())
    if new_sq8_path.stat().st_size != ROWS * ROW_BYTES:
        raise ValueError("V161 relaid SQ8 length differs")
    layout_seal.write_text(canonical({
        "schema": SCHEMA + "-layout-seal", "gt_opened": False,
        "source_sha256": SOURCE_SHA, "old_layout_sha256": LAYOUT_SHA,
        "old_sq8_sha256": SQ8_SHA, "membership_sha256": identity.sha256,
        "membership_bytes": identity.encoded_bytes,
        "new_sq8_sha256": sha256(new_sq8_path),
        "order_sha256": hashlib.sha256(order.astype("<i8").tobytes()).hexdigest(),
        "page_rows": page_rows(), "rows": ROWS, "dimensions": DIMS,
        "row_bytes": ROW_BYTES, "cap_gets": CAP_GETS, "cap_bytes": CAP_BYTES,
    }))


def read_order(membership_path: Path, old_layout_path: Path,
               old_sq8_path: Path) -> tuple[np.ndarray, np.ndarray, np.memmap]:
    old_order, old_sq8 = old_layout_and_sq8(old_layout_path, old_sq8_path)
    source_ids = np.empty(ROWS, dtype=np.int64)
    source_ids[old_order] = old_sq8["id"]
    stable_ids = tuple(str(int(identifier)).encode() for identifier in source_ids)
    membership = read_membership_parquet(membership_path, authority(), stable_ids)
    order = np.fromiter((row.source_ordinal for row in membership),
                        dtype=np.int64, count=ROWS)
    if not np.array_equal(np.sort(order), np.arange(ROWS)):
        raise ValueError("V161 membership permutation differs")
    return order, old_order, old_sq8


def route(primary: list[int], nominees: list[int], old_order: np.ndarray,
          inverse_new: np.ndarray) -> tuple[list[list[int]], int, int, int]:
    if (len(primary) != 100 or len(nominees) != 512
            or len(set(primary)) != 100 or len(set(nominees)) != 512
            or not set(primary).issubset(nominees)
            or any(type(value) is not int or not 0 <= value < ROWS
                   for value in primary + nominees)):
        raise ValueError("frozen V116 roster differs")
    width = page_rows()
    primary_set = set(primary)
    votes: dict[int, int] = {}
    for physical in nominees:
        page = int(inverse_new[old_order[physical]]) // width
        votes[page] = votes.get(page, 0) + (513 if physical in primary_set else 1)
    final_rows = ROWS % width or width
    if final_rows % 32:
        raise ValueError("last page is not unit aligned")
    score, intervals = optimal_weighted_intervals(
        votes, page_count=math.ceil(ROWS / width), max_gets=CAP_GETS,
        max_units=CAP_BYTES // (32 * ROW_BYTES), full_page_units=width // 32,
        last_page_units=final_rows // 32,
    )
    full_bytes = width * ROW_BYTES
    ranges = [[start * full_bytes,
               ROWS * ROW_BYTES if end == (ROWS - 1) // width
               else (end + 1) * full_bytes] for start, end in intervals]
    amount = sum(end - start for start, end in ranges)
    if (not 1 <= len(ranges) <= CAP_GETS or amount > CAP_BYTES
            or any(left[1] >= right[0] for left, right in zip(ranges, ranges[1:]))):
        raise ValueError("V161 physical cap differs")
    distinct = len({int(inverse_new[old_order[row]]) // width for row in primary})
    return ranges, amount, distinct, score


def plan(requests_path: Path, sealed_path: Path, old_layout_path: Path,
         old_sq8_path: Path, membership_path: Path, new_sq8_path: Path,
         layout_seal: Path, plans_path: Path, plan_seal: Path) -> None:
    if sha256(requests_path) != REQUEST_SHA or sha256(sealed_path) != SEALED_SHA:
        raise ValueError("frozen V116 roster identity differs")
    layout = json.loads(layout_seal.read_text())
    if (layout.get("gt_opened") is not False
            or layout.get("membership_sha256") != sha256(membership_path)
            or layout.get("new_sq8_sha256") != sha256(new_sq8_path)):
        raise ValueError("GT-blind V161 layout seal differs")
    order, old_order, _ = read_order(membership_path, old_layout_path, old_sq8_path)
    inverse = np.empty(ROWS, dtype=np.int64)
    inverse[order] = np.arange(ROWS, dtype=np.int64)
    count = 0
    with plans_path.open("x") as output:
        for request, sealed in itertools.zip_longest(jsonl(requests_path),
                                                     jsonl(sealed_path)):
            if (request is None or sealed is None
                    or request.get("query_ordinal") != count
                    or sealed.get("query_ordinal") != count
                    or request.get("nominees") != sealed.get("nominees")):
                raise ValueError("V116 roster order differs")
            ranges, amount, distinct, score = route(
                sealed["primary"], request["nominees"], old_order, inverse,
            )
            output.write(canonical({"query_ordinal": count,
                                    "ranges": ranges, "bytes": amount,
                                    "distinct_primary_pages": distinct,
                                    "plan_score": score}))
            count += 1
    if count != QUERIES:
        raise ValueError("V116 query count differs")
    plan_seal.write_text(canonical({
        "schema": SCHEMA + "-plan-seal", "gt_opened": False,
        "queries": count, "layout_seal_sha256": sha256(layout_seal),
        "requests_sha256": REQUEST_SHA, "sealed_sha256": SEALED_SHA,
        "plans_sha256": sha256(plans_path),
    }))


def score(source_path: Path, old_layout_path: Path, old_sq8_path: Path,
          new_sq8_path: Path, manifest_path: Path, requests_path: Path,
          plans_path: Path, plan_seal: Path, scored_path: Path,
          score_seal: Path) -> None:
    if (sha256(requests_path) != REQUEST_SHA
            or sha256(manifest_path) != MANIFEST_SHA
            or json.loads(plan_seal.read_text()).get("plans_sha256")
                != sha256(plans_path)):
        raise ValueError("V161 score input identity differs")
    source_ids, vectors, _ = source_arrays(source_path)
    old_order, old_sq8 = old_layout_and_sq8(old_layout_path, old_sq8_path,
                                             source_ids)
    sq8 = np.memmap(new_sq8_path, dtype=DTYPE, mode="r", shape=(ROWS,))
    id_to_source = {int(identifier): i for i, identifier in enumerate(source_ids)}
    if (len(id_to_source) != ROWS or len(np.unique(sq8["id"])) != ROWS):
        raise ValueError("source/SQ8 stable IDs differ")
    manifest = json.loads(manifest_path.read_text())
    low = np.asarray(manifest["low"], dtype=np.float32)
    step = np.asarray(manifest["step"], dtype=np.float32)
    if (low.shape != (DIMS,) or step.shape != (DIMS,)
            or not np.isfinite(low).all() or not np.isfinite(step).all()
            or (step <= 0).any()):
        raise ValueError("V114 SQ8 quantizer differs")
    count = 0
    with scored_path.open("x") as output:
        for request, plan_row in itertools.zip_longest(jsonl(requests_path),
                                                       jsonl(plans_path)):
            if (request is None or plan_row is None
                    or request.get("query_ordinal") != count
                    or plan_row.get("query_ordinal") != count):
                raise ValueError("V161 score row count/order differs")
            query_f32 = np.asarray(request["query"], dtype=np.float32)
            query = query_f32.astype(np.float64)
            if query.shape != (DIMS,) or not np.isfinite(query).all():
                raise ValueError("V116 query vector differs")
            query /= np.linalg.norm(query)
            returned = score_sq8_ranges(
                sq8, query_f32, low, step,
                plan_row["ranges"], top_k=512,
            )
            if len(returned) != 512 or len(set(returned)) != 512:
                raise ValueError("V161 SQ8 returned width differs")
            nominee_ids = {int(old_sq8[row]["id"]) for row in request["nominees"]}
            union = np.asarray(sorted(nominee_ids | set(returned)), dtype=np.int64)
            if not 512 <= union.size <= 1024:
                raise ValueError("V161 source union width differs")
            source_rows = np.fromiter((id_to_source[int(identifier)] for identifier in union),
                                      dtype=np.int64, count=union.size)
            exact = rank(union, unit(vectors[source_rows].astype(np.float64)) @ query, 100)
            output.write(canonical({"query_ordinal": count,
                                    "sq8_top512_ids": returned,
                                    "sq8_top100_ids": returned[:100],
                                    "source_top100_ids": exact.tolist(),
                                    "union_size": int(union.size)}))
            count += 1
    if count != QUERIES:
        raise ValueError("V161 score query count differs")
    score_seal.write_text(canonical({
        "schema": SCHEMA + "-score-seal", "gt_opened": False,
        "queries": count, "plan_seal_sha256": sha256(plan_seal),
        "new_sq8_sha256": sha256(new_sq8_path),
        "scored_sha256": sha256(scored_path),
    }))


def reduce(source_path: Path, new_sq8_path: Path, plans_path: Path,
           score_seal: Path, scored_path: Path, truth_path: Path,
           v155_terminal: Path, v155_replay: Path, v155_evidence: Path,
           raw_path: Path, summary_path: Path) -> None:
    if (sha256(truth_path) != TRUTH_SHA
            or sha256(v155_terminal) != V155_TERMINAL_SHA
            or sha256(v155_replay) != V155_REPLAY_SHA
            or sha256(v155_evidence) != V155_EVIDENCE_SHA):
        raise ValueError("V155/GT frozen identity differs")
    terminal = json.loads(v155_terminal.read_text())
    if (terminal.get("status") != "complete"
            or terminal.get("artifacts", {}).get("replay.jsonl", {}).get("sha256")
                != V155_REPLAY_SHA):
        raise ValueError("V155 terminal binding differs")
    seal = json.loads(score_seal.read_text())
    if (seal.get("gt_opened") is not False
            or seal.get("scored_sha256") != sha256(scored_path)):
        raise ValueError("GT-blind score seal differs")
    source_ids, _, _ = source_arrays(source_path)
    truth = load_truth(truth_path, source_ids)
    sq8 = np.memmap(new_sq8_path, dtype=DTYPE, mode="r", shape=(ROWS,))
    positions = {int(identifier): i for i, identifier in enumerate(sq8["id"])}
    if len(positions) != ROWS:
        raise ValueError("relaid SQ8 IDs differ")
    fields = ("candidate_source_hits", "candidate_sq8_hits", "candidate_physical_hits",
              "control_source_hits", "control_sq8_hits", "control_physical_hits",
              "bytes", "gets", "distinct_primary_pages", "union_size")
    metrics = {name: [] for name in fields}
    wins = ties = losses = 0
    count = 0
    with raw_path.open("x") as output:
        for plan_row, scored, control, prior in itertools.zip_longest(
                jsonl(plans_path), jsonl(scored_path), jsonl(v155_replay),
                jsonl(v155_evidence)):
            if any(row is None or row.get("query_ordinal") != count
                   for row in (plan_row, scored, control, prior)):
                raise ValueError("V161 reduction query order differs")
            truth100 = set(map(int, truth[count][:100]))
            source_hits = len(set(scored["source_top100_ids"]) & truth100)
            sq8_hits = len(set(scored["sq8_top100_ids"]) & truth100)
            ranges = plan_row["ranges"]
            physical = sum(any(start <= positions[identifier] * ROW_BYTES < end
                               for start, end in ranges) for identifier in truth100)
            if sq8_hits > physical:
                raise ValueError("candidate physical coverage differs")
            baseline = control["arms"]["sparse"]
            baseline_source = len(set(baseline["source_top100_ids"]) & truth100)
            baseline_sq8 = len(set(baseline["sq8_top100_ids"]) & truth100)
            baseline_physical = prior["arms"]["sparse"]["physical_hits"]
            if (prior["arms"]["sparse"]["source_hits"] != baseline_source
                    or prior["arms"]["sparse"]["sq8_hits"] != baseline_sq8):
                raise ValueError("V155 same-cohort baseline recount differs")
            values = {
                "candidate_source_hits": source_hits,
                "candidate_sq8_hits": sq8_hits,
                "candidate_physical_hits": physical,
                "control_source_hits": baseline_source,
                "control_sq8_hits": baseline_sq8,
                "control_physical_hits": baseline_physical,
                "bytes": plan_row["bytes"], "gets": len(ranges),
                "distinct_primary_pages": plan_row["distinct_primary_pages"],
                "union_size": scored["union_size"],
            }
            for name, value in values.items():
                metrics[name].append(value)
            wins += source_hits > baseline_source
            ties += source_hits == baseline_source
            losses += source_hits < baseline_source
            output.write(canonical({"query_ordinal": count, **values}))
            count += 1
    if count != QUERIES:
        raise ValueError("V161 reduction query count differs")
    candidate = metrics["candidate_source_hits"]
    if (sum(metrics["control_source_hits"]) != 99_567
            or sum(metrics["control_sq8_hits"]) != 99_222):
        raise ValueError("V155 baseline totals differ")
    transfer = sum(candidate) >= 99_400 and sorted(candidate)[49] >= 97
    competitive = (transfer and sum(candidate) >= 99_567
                   and sorted(candidate)[49] >= 98
                   and sum(metrics["bytes"]) <= 11_134_007_040
                   and sum(metrics["gets"]) <= 22_126)
    decision = ("baseline-competitive" if competitive else
                "transfer-pass" if transfer else "killed")
    summary_path.write_text(canonical({
        "schema": SCHEMA + "-summary", "queries": QUERIES,
        "dataset": "ReLAION-1M", "split": "validation-1000-used",
        "decision": decision,
        "paired": {"wins": wins, "ties": ties, "losses": losses},
        "metrics": {name: distribution(values) for name, values in metrics.items()},
        "raw_sha256": sha256(raw_path),
        "score_seal_sha256": sha256(score_seal),
        "v155_terminal_sha256": V155_TERMINAL_SHA,
    }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("construct", "plan", "score", "reduce"))
    for name in ("source", "old_layout", "old_sq8", "manifest", "membership",
                 "new_sq8", "layout_seal", "requests", "sealed", "plans",
                 "plan_seal", "scored", "score_seal", "truth", "v155_terminal",
                 "v155_replay", "v155_evidence", "raw", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path)
    args = parser.parse_args()
    if args.phase == "construct":
        construct(args.source, args.old_layout, args.old_sq8,
                  args.membership, args.new_sq8, args.layout_seal)
    elif args.phase == "plan":
        plan(args.requests, args.sealed, args.old_layout, args.old_sq8,
             args.membership, args.new_sq8, args.layout_seal,
             args.plans, args.plan_seal)
    elif args.phase == "score":
        score(args.source, args.old_layout, args.old_sq8, args.new_sq8,
              args.manifest, args.requests, args.plans, args.plan_seal,
              args.scored, args.score_seal)
    else:
        reduce(args.source, args.new_sq8, args.plans, args.score_seal,
               args.scored, args.truth, args.v155_terminal, args.v155_replay,
               args.v155_evidence, args.raw, args.summary)
