#!/usr/bin/env python3
"""Independent geometry and truth-mask replay for the sealed code-wave projection."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path

import numpy as np

from scripts.native_one_million_selector_cell import DEVELOPMENT_IDENTITIES
from scripts.native_one_million_selector_evaluation import _query_truth
from scripts.native_one_million_two_bit_code_projection_cell import (
    EVIDENCE_FILE,
    PLAN_FILE,
    RESULT_FILE,
    SCHEMA,
    _global_priority,
    _prior_plans,
    _read_json,
    _sealed_plans,
    _source_geometry,
)
from scripts.recount_native_one_million_data_range import _geometry


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _replay_cover(
    priority: Sequence[tuple[str, int]], lengths: Mapping[str, Sequence[int]],
    maximum_gets: int, maximum_bytes: int,
) -> dict[str, object]:
    """Replay greedy admission directly from physical spans and cheapest gaps."""
    if (
        set(lengths) not in ({"base"}, {"base", "delta"})
        or len(priority) != len(set(priority))
        or any(
            role not in lengths or type(page) is not int or not 0 <= page < len(lengths[role])
            for role, page in priority
        )
    ):
        raise ValueError("code-wave independent priority differs")
    prefixes: dict[str, list[int]] = {}
    for role, values in lengths.items():
        prefix = [0]
        for value in values:
            if type(value) is not int or value <= 0:
                raise ValueError("code-wave independent page length differs")
            prefix.append(prefix[-1] + value)
        prefixes[role] = prefix

    def cover(targets: Sequence[tuple[str, int]]) -> tuple[list[tuple[str, int, int]], int] | None:
        runs: list[tuple[str, int, int]] = []
        for role, page in sorted(targets):
            if runs and runs[-1][0] == role and runs[-1][2] == page:
                last = runs[-1]
                runs[-1] = role, last[1], page + 1
            else:
                runs.append((role, page, page + 1))
        gaps = [
            (prefixes[left[0]][right[1]] - prefixes[left[0]][left[2]],
             left[0], left[2], index)
            for index, (left, right) in enumerate(zip(runs, runs[1:], strict=False))
            if left[0] == right[0]
        ]
        needed = max(0, len(runs) - maximum_gets)
        if needed > len(gaps):
            return None
        bridged = {item[3] for item in sorted(gaps)[:needed]}
        intervals: list[tuple[str, int, int]] = []
        for index, (role, start, end) in enumerate(runs):
            if index > 0 and index - 1 in bridged:
                previous = intervals[-1]
                intervals[-1] = role, previous[1], end
            else:
                intervals.append((role, start, end))
        used = sum(prefixes[role][end] - prefixes[role][start]
                   for role, start, end in intervals)
        return intervals, used

    targets: list[tuple[str, int]] = []
    accepted: list[tuple[str, int, int]] = []
    used = 0
    for page in priority:
        proposed = cover((*targets, page))
        if proposed is not None and proposed[1] <= maximum_bytes:
            targets.append(page)
            accepted, used = proposed
    included = [
        (role, page)
        for role, first, end in accepted
        for page in range(first, end)
    ]
    return {
        "priority_pages": [list(page) for page in priority],
        "target_pages": [list(page) for page in targets],
        "ranges": [list(item) for item in accepted],
        "included_pages": [list(page) for page in included],
        "gets": len(accepted),
        "encoded_bytes": used,
    }


def validate_closed(
    root: Path, out: Path, *, query_count: int = 1000,
    expected_rows: int = 1_000_000, expected_pages: int = 7_278,
) -> dict[str, object]:
    """Recount all query byte covers and ordered truth-owner masks."""
    lengths, base_pages, membership = _source_geometry(
        root, expected_rows=expected_rows, expected_pages=expected_pages,
    )
    plans = _sealed_plans(root, query_count)
    historical = _prior_plans(root)
    evidence = _read_json(out / EVIDENCE_FILE)
    result = _read_json(out / RESULT_FILE)
    evidence_body = (out / EVIDENCE_FILE).read_bytes()
    if (
        evidence.get("schema") != SCHEMA + "-evidence"
        or evidence.get("plans_sha256") != hashlib.sha256((root / PLAN_FILE).read_bytes()).hexdigest()
        or evidence.get("truth_identity") != dataclasses.asdict(DEVELOPMENT_IDENTITIES["truth"])
        or len(evidence.get("samples", [])) != query_count
        or result.get("schema") != SCHEMA + "-result"
        or result.get("plans_sha256") != evidence["plans_sha256"]
        or result.get("evidence_sha256") != hashlib.sha256(evidence_body).hexdigest()
    ):
        raise ValueError("code-wave evidence identity differs")
    _, truth_ids = _query_truth(
        root / "queries.parquet", root / "truth.parquet",
        DEVELOPMENT_IDENTITIES, query_count=query_count,
    )
    positions = np.searchsorted(membership["id"], truth_ids)
    if (
        np.any(positions >= len(membership))
        or not np.array_equal(membership["id"][positions], truth_ids)
    ):
        raise ValueError("code-wave validation truth owner differs")
    truth_pages = membership["page"][positions]
    independent_counts = np.bincount(
        membership["page"].astype(np.intp), minlength=expected_pages,
    )
    independent_lengths = {
        "base": tuple(int(200 * count) for count in independent_counts[:base_pages]),
        "delta": tuple(int(200 * count) for count in independent_counts[base_pages:]),
    }
    if independent_lengths != lengths:
        raise ValueError("code-wave independent payload lengths differ")
    replay = []
    for ordinal, (item, prior, recorded) in enumerate(zip(
        plans["samples"], historical["samples"], evidence["samples"], strict=True,
    )):
        if item.get("query_ordinal") != ordinal or recorded.get("query_ordinal") != ordinal:
            raise ValueError("code-wave validation query order differs")
        priority = _global_priority(prior, base_pages, expected_pages)
        code = item["code"]
        if code["priority_pages"] != prior["candidate"]["priority_pages"]:
            raise ValueError("code-wave validation prior priority differs")
        ranked = tuple(tuple(page) for page in code["priority_pages"])
        if code != _replay_cover(ranked, independent_lengths, 32, 16_777_216):
            raise ValueError("code-wave independent admission differs")
        included = _geometry(code, lengths)
        targets = {tuple(page) for page in code["target_pages"]}
        priority_rank = {
            tuple(page): index + 1
            for index, page in enumerate(code["priority_pages"])
        }
        if len(priority_rank) != len(priority):
            raise ValueError("code-wave validation priority length differs")
        owners = [
            ("base", int(page)) if page < base_pages
            else ("delta", int(page) - base_pages)
            for page in truth_pages[ordinal]
        ]
        mask = "".join("1" if owner in included else "0" for owner in owners)
        checked = {
            "query_ordinal": ordinal,
            "hit_mask": mask,
            "hits_at_10": mask[:10].count("1"),
            "hits_at_100": mask.count("1"),
            "priority_owner_ranks": [priority_rank.get(owner) for owner in owners],
            "hit_kinds": [
                "target" if owner in targets else "bridge" if owner in included else "miss"
                for owner in owners
            ],
        }
        if checked != recorded:
            raise ValueError(f"code-wave validation truth mask differs at {ordinal}")
        replay.append(checked)
    hits = [item["hits_at_100"] for item in replay]
    plan_items = [item["code"] for item in plans["samples"]]
    metrics = {
        "gt100_hits": sum(hits),
        "gt10_hits": sum(item["hits_at_10"] for item in replay),
        "target_gt100_hits": sum(item["hit_kinds"].count("target") for item in replay),
        "bridge_gt100_hits": sum(item["hit_kinds"].count("bridge") for item in replay),
        "p05_gt100_hits": sorted(hits)[math.ceil(len(hits) * .05) - 1],
        "sub90_queries": sum(value < 90 for value in hits),
        "maximum_gets": max(item["gets"] for item in plan_items),
        "maximum_bytes": max(item["encoded_bytes"] for item in plan_items),
        "total_gets": sum(item["gets"] for item in plan_items),
        "total_bytes": sum(item["encoded_bytes"] for item in plan_items),
        "minimum_target_pages": min(len(item["target_pages"]) for item in plan_items),
        "maximum_target_pages": max(len(item["target_pages"]) for item in plan_items),
        "minimum_included_pages": min(len(item["included_pages"]) for item in plan_items),
        "maximum_included_pages": max(len(item["included_pages"]) for item in plan_items),
    }
    advance = (
        metrics["gt100_hits"] >= 98_651
        and metrics["p05_gt100_hits"] >= 93
        and metrics["maximum_gets"] <= 32
        and metrics["maximum_bytes"] <= 16_777_216
    )
    if (
        evidence.get("metrics") != metrics
        or result.get("metrics") != metrics
        or result.get("quality_advance_candidate") is not advance
    ):
        raise ValueError("code-wave validation metrics differ")
    validation = {
        "schema": SCHEMA + "-validation",
        "valid": True,
        "queries": query_count,
        "plans_sha256": evidence["plans_sha256"],
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
        "result_sha256": hashlib.sha256((out / RESULT_FILE).read_bytes()).hexdigest(),
        "metrics": metrics,
    }
    (out / "two-bit-code-wave-validation.json").write_bytes(_canonical(validation))
    return validation


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--out", type=Path, default=Path("evaluation"))
    args = parser.parse_args(argv)
    validate_closed(args.root, args.out)


if __name__ == "__main__":
    main()
