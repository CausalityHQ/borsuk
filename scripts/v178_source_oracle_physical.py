#!/usr/bin/env python3
"""V178 truth-aware source oracle for 1M relaid SQ8 physical caps."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import numpy as np

from scripts.v155_relaion_returned_quality import LAYOUT_SHA, ROWS, SQ8_SHA, sha256
from scripts.v164_smooth_layout_1m import DTYPE
from scripts.v165_unit_interval_resources import (
    UNIT_BYTES, UNIT_ROWS, V164_ORDER_SHA, V164_TERMINAL_SHA, load_orders,
)
from scripts.v166_surrogate_probe import modeled_plan
from scripts.v166_surrogate_ranking_run import canonical, records
from scripts.v114_weighted_interval_plan import optimal_weighted_intervals
from scripts.v177_source_candidate_ceiling import (
    COUNT, FIRST, FIT, TARGET_HITS_PER_100K, expanded_units,
)

SCHEMA = "borsuk-v178-source-oracle-physical-v1"
V177_COMMIT = "3d85095e5094742fc1272824dda24263f5ea3fc5"
V177_TERMINAL_SHA = "41acf150e2c76eae346d56fd9db049321348932fd934d8fa58a8ab9fdbc559e7"
V177_SEAL_SHA = "b39e8744b9bc5e222c74ee52c54f082a766f2a8edf9b8798c2bcfce6e261f2a2"
V177_ROSTERS_SHA = "6be71fbd2282f79e9ed97809c5f04f50ebb077e0574d5a1c919951261962482c"
V177_LABELS_SHA = "0c998c9e46b140906d843b16d57ae3c966a5414031254e40d050cb29e19e0743"
V177_SUMMARY_SHA = "7cdec10aa487f1730d70bd3851206dbee12d91076cb4b87fe1622a73baca5556"
CAP_GETS = 32
CAP_BYTES = 16_777_216
CAP_UNITS = CAP_BYTES // UNIT_BYTES
UNIT_COUNT = ROWS // UNIT_ROWS
WIDTH = 8
ARMS = ("unit_transport", "primary_constrained", "width8_weighted")


def _sealed_v177(root: Path) -> tuple[list[dict], list[dict]]:
    expected = {
        "terminal.json": V177_TERMINAL_SHA,
        "prepare-seal.json": V177_SEAL_SHA,
        "rosters.jsonl": V177_ROSTERS_SHA,
        "source-labels.jsonl": V177_LABELS_SHA,
        "summary.json": V177_SUMMARY_SHA,
    }
    if any(sha256(root / name) != digest for name, digest in expected.items()):
        raise ValueError("V178 closed V177 artifact identity differs")
    terminal = json.loads((root / "terminal.json").read_text())
    seal = json.loads((root / "prepare-seal.json").read_text())
    summary = json.loads((root / "summary.json").read_text())
    if (terminal.get("status") != "complete" or terminal.get("exit_code") != 0
            or terminal.get("source_commit") != V177_COMMIT
            or terminal.get("artifacts", {}).get("out/rosters.jsonl", {}).get("sha256")
                != V177_ROSTERS_SHA
            or terminal.get("artifacts", {}).get("out/source-labels.jsonl", {}).get("sha256")
                != V177_LABELS_SHA
            or seal.get("source_truth_opened") is not False
            or seal.get("rosters_sha256") != V177_ROSTERS_SHA
            or summary.get("source_labels_sha256") != V177_LABELS_SHA
            or summary.get("prepare_seal_sha256") != V177_SEAL_SHA):
        raise ValueError("V178 V177 seal/terminal binding differs")
    rosters = records(root / "rosters.jsonl")
    labels = records(root / "source-labels.jsonl")
    if (len(rosters) != COUNT or len(labels) != COUNT
            or [row.get("ordinal") for row in rosters] != list(range(FIRST, FIRST + COUNT))
            or [row.get("ordinal") for row in labels] != list(range(FIRST, FIRST + COUNT))
            or [row.get("source_id") for row in rosters]
                != [row.get("source_id") for row in labels]):
        raise ValueError("V178 V177 query roster differs")
    return rosters, labels


def _stable_id_units(old_sq8: Path, old_layout: Path, order: Path,
                     v164_terminal: Path) -> dict[int, int]:
    if (sha256(old_sq8) != SQ8_SHA or sha256(old_layout) != LAYOUT_SHA
            or sha256(order) != V164_ORDER_SHA
            or sha256(v164_terminal) != V164_TERMINAL_SHA):
        raise ValueError("V178 SQ8/layout/order identity differs")
    old, inverse_new = load_orders(old_layout, order, v164_terminal)
    sq8 = np.memmap(old_sq8, dtype=DTYPE, mode="r", shape=(ROWS,))
    stable_ids = sq8["id"]
    if len(np.unique(stable_ids)) != ROWS:
        raise ValueError("V178 duplicate stable SQ8 ID")
    units = inverse_new[old] // UNIT_ROWS
    return dict(zip(map(int, stable_ids), map(int, units), strict=True))


def truth_unit_masses(truth_ids: list[int], source_id: int,
                      id_to_unit: dict[int, int]) -> dict[int, int]:
    if (len(truth_ids) != 100 or len(set(truth_ids)) != 100
            or source_id in truth_ids or any(value not in id_to_unit
                                            for value in truth_ids)):
        raise ValueError("V178 source truth IDs differ")
    return dict(Counter(id_to_unit[value] for value in truth_ids))


def _score_plan(intervals: tuple[tuple[int, int], ...],
                masses: dict[int, int], mandatory: list[int]) -> dict:
    if (not intervals or len(intervals) > CAP_GETS
            or any(start < 0 or end < start or end >= UNIT_COUNT
                   for start, end in intervals)
            or any(left[1] >= right[0] for left, right in zip(intervals, intervals[1:]))):
        raise ValueError("V178 oracle interval geometry differs")
    units = sum(end - start + 1 for start, end in intervals)
    if units > CAP_UNITS or any(
        not any(start <= unit <= end for start, end in intervals)
        for unit in mandatory
    ):
        raise ValueError("V178 oracle physical cap or primary cover differs")
    hits = sum(count for unit, count in masses.items()
               if any(start <= unit <= end for start, end in intervals))
    return {"intervals": [list(interval) for interval in intervals],
            "gets": len(intervals), "units": units,
            "bytes": units * UNIT_BYTES, "source_hits": hits}


def _oracle(masses: dict[int, int], mandatory: list[int]) -> dict:
    try:
        if mandatory:
            intervals = modeled_plan(
                masses, mandatory, page_count=UNIT_COUNT,
                max_gets=CAP_GETS, max_units=CAP_UNITS,
                nominee_count=512, unit_rows=UNIT_ROWS)
        else:
            _, intervals = optimal_weighted_intervals(
                masses, page_count=UNIT_COUNT, max_gets=CAP_GETS,
                max_units=CAP_UNITS, full_page_units=1, last_page_units=1)
    except ValueError as exc:
        if str(exc) == "V166 modeled plan misses an exact-primary unit":
            return {"infeasible_primary": True}
        raise
    return _score_plan(intervals, masses, mandatory)


def decision_for(hits: dict[str, int], p05: dict[str, int],
                 threshold: int) -> str:
    if (set(hits) != set(ARMS) or set(p05) != set(ARMS)
            or threshold <= 0 or any(not 0 <= value <= COUNT * 100
                                      for value in hits.values())
            or any(not 0 <= value <= 100 for value in p05.values())):
        raise ValueError("V178 oracle decision totals differ")
    for arm, failed in (("unit_transport", "unit-transport-killed"),
                        ("primary_constrained", "primary-layout-killed"),
                        ("width8_weighted", "candidate-witness-failed")):
        if hits[arm] < threshold or p05[arm] < 98:
            return failed
    return "advance-to-gt-blind-calibration"


def run(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise ValueError("V178 output already exists")
    if ROWS % UNIT_ROWS or CAP_UNITS != 672:
        raise ValueError("V178 frozen whole-unit physical geometry differs")
    rosters, labels = _sealed_v177(args.v177)
    id_to_unit = _stable_id_units(
        args.old_sq8, args.old_layout, args.order, args.v164_terminal)
    args.output.mkdir(parents=True)
    totals = {split: {arm: {"hits": 0, "gets": 0, "bytes": 0,
                             "infeasible_primary": 0}
                      for arm in ARMS}
              for split in ("fit", "holdout")}
    per_query_hits = {arm: [] for arm in ARMS}
    with (args.output / "oracle-plans.jsonl").open("x") as output:
        for index, (roster, label) in enumerate(zip(rosters, labels, strict=True)):
            masses = truth_unit_masses(
                label["truth_ids"], roster["source_id"], id_to_unit)
            base = roster["base_units"]
            mandatory = roster["mandatory_units"]
            if (base != sorted(set(base)) or mandatory != sorted(set(mandatory))
                    or not set(mandatory).issubset(base)
                    or any(type(value) is not int or not 0 <= value < UNIT_COUNT
                           for value in base + mandatory)):
                raise ValueError("V178 sealed unit roster geometry differs")
            candidate = set(expanded_units(base, WIDTH, UNIT_COUNT))
            candidate_masses = {unit: count for unit, count in masses.items()
                                if unit in candidate}
            if sum(candidate_masses.values()) != label["coverage"][str(WIDTH)]:
                raise ValueError("V178 independent V177 width-8 coverage differs")
            results = {
                "unit_transport": _oracle(masses, []),
                "primary_constrained": _oracle(masses, mandatory),
                "width8_weighted": _oracle(candidate_masses, mandatory),
            }
            split = "fit" if index < FIT else "holdout"
            for arm, result in results.items():
                if result.get("infeasible_primary"):
                    totals[split][arm]["infeasible_primary"] += 1
                    per_query_hits[arm].append(0)
                else:
                    # A bridging interval may collect truth outside the
                    # candidate-weighted objective; count actual fetched truth.
                    result["source_hits"] = sum(
                        count for unit, count in masses.items()
                        if any(start <= unit <= end
                               for start, end in result["intervals"]))
                    for key in ("source_hits", "gets", "bytes"):
                        totals[split][arm]["hits" if key == "source_hits" else key] += result[key]
                    per_query_hits[arm].append(result["source_hits"])
            output.write(canonical({"ordinal": roster["ordinal"],
                                    "source_id": roster["source_id"],
                                    "split": split, "oracle": results}))
    # For two 64-query splits, 128 source queries contain 12,800 truth slots.
    threshold = (TARGET_HITS_PER_100K * COUNT * 100 + 99_999) // 100_000
    hits = {arm: sum(totals[split][arm]["hits"] for split in totals)
            for arm in ARMS}
    p05 = {arm: sorted(hits)[6] for arm, hits in per_query_hits.items()}
    decision = decision_for(hits, p05, threshold)
    (args.output / "summary.json").write_text(canonical({
        "schema": SCHEMA + "-summary", "source_only": True,
        "truth_aware_oracle": True, "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-641-768",
        "query_count": COUNT, "target_hits": threshold,
        "target_basis": "V155 used validation-1000 rate, unpaired",
        "totals": totals, "p05": p05, "decision": decision,
        "v177_terminal_sha256": V177_TERMINAL_SHA,
        "plans_sha256": sha256(args.output / "oracle-plans.jsonl"),
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--v177", type=Path, required=True)
    parser.add_argument("--old-sq8", type=Path, required=True)
    parser.add_argument("--old-layout", type=Path, required=True)
    parser.add_argument("--order", type=Path, required=True)
    parser.add_argument("--v164-terminal", type=Path, required=True)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
