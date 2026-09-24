#!/usr/bin/env python3
"""GT-free exact lower bound for V171's mandatory-primary resource profile."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import tempfile
from dataclasses import dataclass
from pathlib import Path

import numpy as np

ROWS = 1_000_000
QUERIES = 1_000
UNIT_ROWS = 32
UNIT_BYTES = 32 * (768 + 12)
SCHEMA = "borsuk-v171-primary-feasibility-v1"
HASHES = {
    "sealed": (13_455_525, "3bfd155ac5f9e1b7aacbc263e1732e2314c9722f235d3454e0f17d0c6bc3c960"),
    "old_layout": (4_000_128, "32cba9690cd9d0ed3809763e5a0fa3574b09a207da26e93404651acaa1a66a0b"),
    "order": (8_000_128, "5b5ef48d86570e5ca68fdaaac9aef231ec7368dd526baef00474cd0a2f59a06f"),
    "v164_terminal": (2_038, "daa4025093ddef883358a200751972b9d953cd53be80681b9055677c3c7793c7"),
    "layout_seal": (860, "9ddf446e2d925e390037931dffaf6abb22dbc08523686a28e71cab31695a704b"),
    "v154_terminal": (2_672, "09301ea5dfbb75aef49e7f34869903c15def2dfcd0eb787214f924fb191c5068"),
    "science": (99_226_409, "ab9bac04c32445a85928dd8978468b1c40d937da2f89960e71d8d21d132ea4d1"),
}


@dataclass(frozen=True)
class MinimumIntervals:
    intervals: tuple[tuple[int, int], ...]
    units: int
    gets: int
    initial_runs: int
    bridge_units: int


def minimum_primary_intervals(
    mandatory: tuple[int, ...], *, max_gets: int,
) -> MinimumIntervals:
    """Bridge the smallest positive gaps until all primary runs fit."""
    if (not mandatory or len(set(mandatory)) != len(mandatory)
            or any(type(unit) is not int or unit < 0 for unit in mandatory)
            or type(max_gets) is not int or max_gets <= 0):
        raise ValueError("V171 mandatory unit geometry differs")
    ordered = sorted(mandatory)
    runs: list[tuple[int, int]] = []
    for unit in ordered:
        if runs and unit == runs[-1][1] + 1:
            runs[-1] = (runs[-1][0], unit)
        else:
            runs.append((unit, unit))
    gaps = [(runs[i + 1][0] - runs[i][1] - 1, i)
            for i in range(len(runs) - 1)]
    selected = {index for _, index in sorted(gaps)[:max(0, len(runs) - max_gets)]}
    intervals: list[tuple[int, int]] = []
    begin, end = runs[0]
    for index, (left, right) in enumerate(runs[1:]):
        if index in selected:
            end = right
        else:
            intervals.append((begin, end))
            begin, end = left, right
    intervals.append((begin, end))
    actual_units = sum(end - start + 1 for start, end in intervals)
    bridge = actual_units - len(ordered)
    if (len(intervals) > max_gets or bridge != sum(gaps[i][0] for i in selected)
            or any(not any(a <= unit <= b for a, b in intervals)
                   for unit in ordered)):
        raise AssertionError("V171 minimum-unit witness differs")
    return MinimumIntervals(tuple(intervals), actual_units, len(intervals),
                            len(runs), bridge)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def authenticate(args: argparse.Namespace) -> tuple[np.ndarray, np.ndarray]:
    for field, (size, digest) in HASHES.items():
        path = getattr(args, field)
        if path.stat().st_size != size or sha256(path) != digest:
            raise ValueError(f"V171 frozen input differs: {field}")
    v164 = json.loads(args.v164_terminal.read_text())
    v154 = json.loads(args.v154_terminal.read_text())
    seal = json.loads(args.layout_seal.read_text())
    if (v164.get("status") != "complete"
            or v164.get("artifacts", {}).get("order.npy", {}).get("sha256")
                != HASHES["order"][1]
            or v164.get("artifacts", {}).get("layout-seal.json", {}).get("sha256")
                != HASHES["layout_seal"][1]
            or seal.get("source_sha256")
                != "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86"
            or seal.get("old_layout_sha256") != HASHES["old_layout"][1]
            or seal.get("order_file_sha256") != HASHES["order"][1]
            or v154.get("status") != "complete"
            or v154.get("artifacts", {}).get("science.jsonl", {}).get("sha256")
                != HASHES["science"][1]):
        raise ValueError("V171 closed source/resource authority differs")
    old = np.load(args.old_layout, allow_pickle=False)
    order = np.load(args.order, allow_pickle=False)
    if (old.shape != (ROWS,) or old.dtype not in (np.int32, np.int64)
            or order.shape != (ROWS,) or order.dtype != np.int64
            or not np.array_equal(np.sort(old), np.arange(ROWS))
            or not np.array_equal(np.sort(order), np.arange(ROWS))):
        raise ValueError("V171 old/new source permutations differ")
    inverse = np.empty(ROWS, dtype=np.int64)
    inverse[order] = np.arange(ROWS, dtype=np.int64)
    return old, inverse


def records(path: Path):
    with path.open() as source:
        for line in source:
            yield json.loads(line)


def canonical(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def evaluate(args: argparse.Namespace) -> None:
    old, inverse = authenticate(args)
    rows = 0
    infeasible: list[int] = []
    control_bytes = control_gets = 0
    min_bytes = witness_gets = 0
    with args.raw.open("x") as output:
        for sealed, science in itertools.zip_longest(
                records(args.sealed), records(args.science)):
            if sealed is None or science is None:
                raise ValueError("V171 paired query count differs")
            primary = sealed.get("primary")
            plan = science.get("v152", {}).get("plan", {})
            bytes_cap, gets_cap = plan.get("planned_bytes"), plan.get("gets")
            ranges = plan.get("ranges")
            if (sealed.get("query_ordinal") != rows
                    or science.get("query_ordinal") != rows
                    or not isinstance(primary, list) or len(primary) != 100
                    or len(set(primary)) != 100
                    or any(type(row) is not int or not 0 <= row < ROWS
                           for row in primary)
                    or type(bytes_cap) is not int or type(gets_cap) is not int
                    or not 0 < bytes_cap <= 16_777_216
                    or bytes_cap % UNIT_BYTES or not 0 < gets_cap <= 32
                    or not isinstance(ranges, list) or len(ranges) != gets_cap
                    or bytes_cap != sum(end - start for start, end in ranges)):
                raise ValueError("V171 V116/V154 query resource pair differs")
            units = tuple(sorted({int(inverse[int(old[row])]) // UNIT_ROWS
                                  for row in primary}))
            witness = minimum_primary_intervals(units, max_gets=gets_cap)
            fits = witness.units * UNIT_BYTES <= bytes_cap
            infeasible.extend([rows] if not fits else [])
            control_bytes += bytes_cap
            control_gets += gets_cap
            min_bytes += witness.units * UNIT_BYTES
            witness_gets += witness.gets
            output.write(canonical({
                "query_ordinal": rows, "primary_units": len(units),
                "initial_runs": witness.initial_runs,
                "witness_gets": witness.gets,
                "minimum_units": witness.units,
                "bridge_units": witness.bridge_units,
                "control_bytes": bytes_cap, "control_gets": gets_cap,
                "feasible": fits,
            }))
            rows += 1
    if (rows != QUERIES or control_bytes != 11_134_007_040
            or control_gets != 22_126):
        raise ValueError("V171 V155 frozen resource totals differ")
    args.summary.write_text(canonical({
        "schema": SCHEMA + "-summary", "dataset": "ReLAION-1M D768",
        "split": "validation-1000-used", "queries": rows,
        "decision": "advance-to-scored-cell" if not infeasible
            else "per-query-profile-infeasible",
        "infeasible_queries": infeasible,
        "minimum_primary_bytes": min_bytes,
        "minimum_unit_witness_gets": witness_gets,
        "control_bytes": control_bytes, "control_gets": control_gets,
        "raw_sha256": sha256(args.raw),
        "input_sha256": {key: digest for key, (_, digest) in HASHES.items()},
    }))


def check(args: argparse.Namespace) -> dict:
    with tempfile.TemporaryDirectory(prefix="v171-feasibility-") as root:
        replay = argparse.Namespace(**vars(args))
        replay.raw = Path(root) / "raw.jsonl"
        replay.summary = Path(root) / "summary.json"
        evaluate(replay)
        if (sha256(args.raw) != sha256(replay.raw)
                or sha256(args.summary) != sha256(replay.summary)):
            raise ValueError("V171 primary feasibility replay differs")
    return {"schema": SCHEMA + "-check", "status": "pass",
            "decision": json.loads(args.summary.read_text())["decision"]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("evaluate", "check"))
    for name in (*HASHES, "raw", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    args = parser.parse_args()
    if args.phase == "evaluate":
        evaluate(args)
    else:
        print(canonical(check(args)), end="")


if __name__ == "__main__":
    main()
