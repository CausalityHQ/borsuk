#!/usr/bin/env python3
"""V177 sealed source-only candidate-universe ceiling for relaid 1M SQ8."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from scripts.v114_1m_paired import nominate_region_pq64
from scripts.v155_relaion_returned_quality import (
    DIMS, LAYOUT_SHA, ROWS, SOURCE_SHA, SQ8_SHA, sha256,
)
from scripts.v164_smooth_layout_1m import DTYPE, source_arrays
from scripts.v165_unit_interval_resources import (
    UNIT_ROWS, V164_ORDER_SHA, V164_TERMINAL_SHA, load_orders,
)
from scripts.v166_surrogate_probe import select_pseudoqueries
from scripts.v166_surrogate_ranking_run import (
    ROUTER_MANIFEST_SHA, _score, canonical, load_frozen_v115_router, records,
)

SCHEMA = "borsuk-v177-source-candidate-ceiling-v1"
FIRST = 640
COUNT = 128
FIT = 64
WIDTHS = (0, 1, 2, 4, 8)
TARGET_HITS_PER_100K = 99567  # V155 used 1M comparator, not a model constant.


def expanded_units(base: list[int] | tuple[int, ...], width: int,
                   unit_count: int) -> tuple[int, ...]:
    """One generic physical neighborhood around a sealed nominee-unit set."""
    if (width < 0 or unit_count <= 0 or not base
            or len(set(base)) != len(base)
            or any(type(unit) is not int or not 0 <= unit < unit_count
                   for unit in base)):
        raise ValueError("V177 candidate-unit geometry differs")
    return tuple(sorted({neighbor for unit in base
                         for neighbor in range(max(0, unit - width),
                                               min(unit_count, unit + width + 1))}))


def coverage(truth_rows: np.ndarray, inverse_new: np.ndarray,
             base: list[int], unit_count: int) -> dict[str, int]:
    if (truth_rows.shape != (100,) or len(set(map(int, truth_rows))) != 100
            or np.any(truth_rows < 0) or np.any(truth_rows >= len(inverse_new))):
        raise ValueError("V177 source truth geometry differs")
    truth_units = inverse_new[truth_rows] // UNIT_ROWS
    return {str(width): int(np.isin(
        truth_units, expanded_units(base, width, unit_count)).sum())
        for width in WIDTHS}


def _inputs(args: argparse.Namespace):
    if (sha256(args.source) != SOURCE_SHA or sha256(args.old_layout) != LAYOUT_SHA
            or sha256(args.old_sq8) != SQ8_SHA):
        raise ValueError("V177 source/layout/SQ8 identity differs")
    manifest, planes = load_frozen_v115_router(
        args.router, ROUTER_MANIFEST_SHA, ROWS, DIMS)
    if (manifest["source_sha256"] != SOURCE_SHA
            or manifest["layout_sha256"] != LAYOUT_SHA
            or manifest["sq8_sha256"] != SQ8_SHA):
        raise ValueError("V177 router authority differs")
    old, inverse_new = load_orders(
        args.old_layout, args.order, args.v164_terminal)
    source_ids, vectors, _ = source_arrays(args.source)
    sq8 = np.memmap(args.old_sq8, dtype=DTYPE, mode="r", shape=(ROWS,))
    if not np.array_equal(sq8["id"], source_ids[old]):
        raise ValueError("V177 SQ8/source mapping differs")
    return old, inverse_new, source_ids, vectors, sq8, planes


def prepare(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise ValueError("V177 output already exists")
    old, inverse_new, source_ids, vectors, sq8, planes = _inputs(args)
    chosen = select_pseudoqueries(source_ids, FIRST + COUNT)[FIRST:]
    id_to_source = {int(identifier): row for row, identifier in enumerate(source_ids)}
    old_inverse = np.empty(ROWS, dtype=np.int64)
    old_inverse[old] = np.arange(ROWS, dtype=np.int64)
    args.output.mkdir(parents=True)
    with (args.output / "rosters.jsonl").open("x") as output:
        for ordinal, stable_id in enumerate(chosen, start=FIRST):
            source_row = id_to_source[stable_id]
            query = np.asarray(vectors[source_row], dtype=np.float32)
            _, _, nominated = nominate_region_pq64(
                query, planes["summaries"], planes["books"], planes["codes"],
                page_rows=256, blocks_per_page=2, regions=1024, shortlist=512)
            nominees = np.asarray(nominated, dtype=np.int64)
            if nominees.shape != (512,) or len(np.unique(nominees)) != 512:
                raise ValueError("V177 nominee roster differs")
            eligible = nominees[nominees != old_inverse[source_row]]
            _, scores = _score(sq8, query, eligible, planes["low"],
                               planes["step"], 1)
            primary_rank = np.lexsort((sq8["id"][eligible], scores))[:100]
            primary = eligible[primary_rank]
            base = sorted(set((inverse_new[old[nominees]] // UNIT_ROWS).tolist()))
            mandatory = sorted(set((inverse_new[old[primary]] // UNIT_ROWS).tolist()))
            if not set(mandatory).issubset(base):
                raise ValueError("V177 primary units outside nominees")
            output.write(canonical({
                "ordinal": ordinal, "source_id": stable_id,
                "source_row": int(source_row), "base_units": base,
                "mandatory_units": mandatory,
            }))
    (args.output / "prepare-seal.json").write_text(canonical({
        "schema": SCHEMA + "-prepare-seal", "dataset": "ReLAION-1M",
        "split": "source-pseudoquery-hash-ranks-641-768",
        "fit": "641-704", "holdout": "705-768",
        "source_truth_opened": False, "source_sha256": SOURCE_SHA,
        "old_layout_sha256": LAYOUT_SHA, "old_sq8_sha256": SQ8_SHA,
        "order_sha256": V164_ORDER_SHA,
        "v164_terminal_sha256": V164_TERMINAL_SHA,
        "router_manifest_sha256": ROUTER_MANIFEST_SHA,
        "pseudo_ids": list(chosen),
        "rosters_sha256": sha256(args.output / "rosters.jsonl"),
        "widths": list(WIDTHS), "unit_rows": UNIT_ROWS,
        "target_hits_per_100k": TARGET_HITS_PER_100K,
    }))


def evaluate(args: argparse.Namespace) -> None:
    seal = json.loads((args.output / "prepare-seal.json").read_text())
    rows = records(args.output / "rosters.jsonl")
    if (seal.get("schema") != SCHEMA + "-prepare-seal"
            or seal.get("source_truth_opened") is not False
            or seal.get("rosters_sha256") != sha256(args.output / "rosters.jsonl")
            or seal.get("widths") != list(WIDTHS)
            or len(rows) != COUNT
            or [row.get("ordinal") for row in rows] != list(range(FIRST, FIRST + COUNT))
            or [row.get("source_id") for row in rows] != seal.get("pseudo_ids")):
        raise ValueError("V177 sealed roster differs")
    old, inverse_new, source_ids, vectors, _, _ = _inputs(args)
    del old
    if (sha256(args.output / "prepare-seal.json") != args.prepare_sha256
            or seal["pseudo_ids"] != list(
                select_pseudoqueries(source_ids, FIRST + COUNT)[FIRST:])):
        raise ValueError("V177 prepare seal identity differs")
    source = np.empty((ROWS, DIMS), dtype=np.float64)
    for start in range(0, ROWS, 8192):
        stop = min(start + 8192, ROWS)
        block = np.asarray(vectors[start:stop], dtype=np.float64)
        norms = np.linalg.norm(block, axis=1)
        if not np.isfinite(norms).all() or (norms <= 0).any():
            raise ValueError("V177 source norm differs")
        source[start:stop] = block / norms[:, None]
    unit_count = (ROWS + UNIT_ROWS - 1) // UNIT_ROWS
    totals = {split: {str(width): 0 for width in WIDTHS}
              for split in ("fit", "holdout")}
    with (args.output / "source-labels.jsonl").open("x") as output:
        for index, row in enumerate(rows):
            source_row = row["source_row"]
            if (type(source_row) is not int or not 0 <= source_row < ROWS
                    or int(source_ids[source_row]) != row["source_id"]):
                raise ValueError("V177 source row identity differs")
            base = row["base_units"]
            mandatory = row["mandatory_units"]
            if (not isinstance(base, list) or not isinstance(mandatory, list)
                    or base != sorted(set(base)) or mandatory != sorted(set(mandatory))
                    or not set(mandatory).issubset(base)):
                raise ValueError("V177 unit roster differs")
            scores = source @ source[source_row]
            scores[source_row] = -np.inf
            top = np.argpartition(scores, ROWS - 100)[ROWS - 100:]
            tied = np.flatnonzero(scores >= scores[top].min())
            truth = tied[np.lexsort((source_ids[tied], -scores[tied]))[:100]]
            counts = coverage(truth, inverse_new, base, unit_count)
            split = "fit" if index < FIT else "holdout"
            for width, count in counts.items():
                totals[split][width] += count
            output.write(canonical({
                "ordinal": row["ordinal"], "source_id": row["source_id"],
                "split": split, "coverage": counts,
                "truth_ids": [int(value) for value in source_ids[truth]],
            }))
    holdout = totals["holdout"][str(WIDTHS[-1])]
    # 64 queries contain 6,400 truth slots. This is a source-only screen.
    qualified = holdout * 100_000 >= TARGET_HITS_PER_100K * FIT * 100
    (args.output / "summary.json").write_text(canonical({
        "schema": SCHEMA + "-summary", "source_only": True,
        "metric": "exact-cosine-source-neighbor-candidate-coverage@100",
        "query_count_per_split": FIT, "totals": totals,
        "holdout_width8_decision": "advance-to-calibration" if qualified else "kill-width8",
        "target_hits_per_100k": TARGET_HITS_PER_100K,
        "prepare_seal_sha256": args.prepare_sha256,
        "source_labels_sha256": sha256(args.output / "source-labels.jsonl"),
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "evaluate"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--old-layout", type=Path, required=True)
    parser.add_argument("--old-sq8", type=Path, required=True)
    parser.add_argument("--router", type=Path, required=True)
    parser.add_argument("--order", type=Path, required=True)
    parser.add_argument("--v164-terminal", type=Path, required=True)
    parser.add_argument("--prepare-sha256", default="")
    args = parser.parse_args()
    if args.phase == "prepare":
        prepare(args)
    else:
        if len(args.prepare_sha256) != 64:
            raise ValueError("V177 evaluate requires externally sealed prepare SHA")
        evaluate(args)


if __name__ == "__main__":
    main()
