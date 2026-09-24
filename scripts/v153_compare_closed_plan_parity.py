#!/usr/bin/env python3
"""Compare deterministic plans from two authenticated, terminal-closed JSONL files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def rows(path: Path) -> list[dict]:
    with path.open() as source:
        return [json.loads(line) for line in source]


def compare(before: list[dict], after: list[dict]) -> dict:
    if len(before) != len(after) or not before:
        raise ValueError("row counts differ or are empty")
    arms = ("flat_plan", "v150", "v151", "v152")
    mismatches = {arm: 0 for arm in arms}
    rosters = 0
    flat_scores = 0
    for left, right in zip(before, after, strict=True):
        if (left["query_ordinal"], left["source_query_ordinal"]) != (
            right["query_ordinal"], right["source_query_ordinal"]
        ):
            raise ValueError("query identity or row order differs")
        rosters += left["primary_pages"] != right["primary_pages"]
        flat_scores += left["flat_scores"] != right["flat_scores"]
        for arm in arms:
            old_plan = left[arm] if arm == "flat_plan" else left[arm]["plan"]
            new_plan = right[arm] if arm == "flat_plan" else right[arm]["plan"]
            mismatches[arm] += old_plan != new_plan
    return {
        "rows": len(before),
        "plan_mismatches": mismatches,
        "primary_roster_mismatches": rosters,
        "flat_score_mismatches": flat_scores,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("before", type=Path)
    parser.add_argument("after", type=Path)
    args = parser.parse_args()
    result = compare(rows(args.before), rows(args.after))
    print(json.dumps(result, sort_keys=True))
    if any(result["plan_mismatches"].values()) or result[
        "primary_roster_mismatches"
    ]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
