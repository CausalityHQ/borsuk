#!/usr/bin/env python3
"""Independent all-query replay of the sealed 1M page-count ceiling."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
from collections.abc import Mapping
from pathlib import Path

import numpy as np

from scripts.native_one_million_page_oracle_cell import (
    PRIOR,
    SCHEMA,
    _canonical,
    _prior_authority,
    _read_map,
)
from scripts.native_one_million_page_selector import _page_membership
from scripts.native_one_million_selector_cell import (
    DEVELOPMENT_IDENTITIES,
    SOURCE_IDENTITIES,
)
from scripts.native_one_million_selector_evaluation import _query_truth
from scripts.v97_row_width_screen import _authenticate_object


def validate(
    root: Path,
    out: Path,
    *,
    expected_rows: int = 1_000_000,
    expected_pages: int = 7_278,
    expected_group_hits: Mapping[str, tuple[int, int]] | None = None,
) -> dict[str, object]:
    if expected_group_hits is None:
        expected_group_hits = {"candidate": (98_985, 9_956), "control": (98_151, 9_928)}
    _, plans = _prior_authority(root)
    seal, _, membership, page_groups, page_bytes = _read_map(root)
    for role, filename in (
        ("generation", "generation.json"),
        ("base", "base.arrow"),
        ("delta", "delta.arrow"),
    ):
        _authenticate_object(role, root / filename, SOURCE_IDENTITIES[role])
    rebuilt_groups, rebuilt_pages, rebuilt_page_groups, rebuilt_order_sha = (
        _page_membership(root, SOURCE_IDENTITIES)
    )
    generation = json.loads((root / "generation.json").read_bytes())
    rebuilt_bytes = np.asarray(
        [int(page["bytes"]) for run in generation["runs"] for page in run["pages"]],
        dtype="<u4",
    )
    if (
        rebuilt_order_sha != seal["page_order_sha256"]
        or [dataclasses.asdict(group) for group in rebuilt_groups] != seal["groups"]
        or not np.array_equal(rebuilt_page_groups, page_groups)
        or not np.array_equal(rebuilt_bytes, page_bytes)
        or len(rebuilt_pages) != len(membership)
        or any(
            rebuilt_pages.get(int(item["id"])) != int(item["page"])
            for item in membership
        )
    ):
        raise ValueError("page oracle physical map validation differs")
    evidence_body = (root / "evidence.json").read_bytes()
    result_body = (root / "result.json").read_bytes()
    evidence = json.loads(evidence_body)
    result = json.loads(result_body)
    if (
        evidence_body != _canonical(evidence)
        or result_body != _canonical(result)
        or evidence.get("schema") != SCHEMA + "-evidence"
        or result.get("schema") != SCHEMA + "-result"
        or evidence.get("source_seal_sha256") != PRIOR["source-seal"][1]
        or evidence.get("plans_sha256") != PRIOR["plans"][1]
        or evidence.get("map_seal_sha256")
        != hashlib.sha256((root / "seal.json").read_bytes()).hexdigest()
        or result.get("evidence_sha256") != hashlib.sha256(evidence_body).hexdigest()
        or len(evidence.get("samples", [])) != 1000
        or len(plans["samples"]) != 1000
        or seal["rows"] != expected_rows
        or seal["pages"] != expected_pages
    ):
        raise ValueError("page oracle evidence authority differs")
    _, truth_ids = _query_truth(
        root / "queries.parquet",
        root / "truth.parquet",
        DEVELOPMENT_IDENTITIES,
        query_count=1000,
    )
    positions = np.searchsorted(membership["id"], truth_ids)
    if np.any(positions >= len(membership)) or not np.array_equal(
        membership["id"][positions], truth_ids
    ):
        raise ValueError("page oracle validation truth differs")
    owners = membership["page"][positions]
    metrics: dict[str, dict[str, int]] = {}
    for arm in ("candidate", "control"):
        group_hits = []
        group_hits10 = []
        upper100 = []
        upper10 = []
        for ordinal, sample in enumerate(evidence["samples"]):
            if sample.get("query_ordinal") != ordinal:
                raise ValueError("page oracle validation order differs")
            selected_groups = set(plans["samples"][ordinal][arm]["selected_groups"])
            pages = np.asarray(owners[ordinal], dtype=np.intp)
            selected = np.isin(page_groups[pages], tuple(selected_groups))
            group_mask = "".join("1" if hit else "0" for hit in selected)
            available = np.flatnonzero(np.isin(page_groups, tuple(selected_groups)))
            counts = np.bincount(pages, minlength=len(page_groups))
            ranked = sorted(
                (int(page) for page in available if counts[page] > 0),
                key=lambda page: (-int(counts[page]), page),
            )[:32]
            chosen = np.isin(pages, ranked)
            oracle_mask = "".join("1" if hit else "0" for hit in chosen)
            counts10 = np.bincount(pages[:10], minlength=len(page_groups))
            ranked10 = sorted(
                (int(page) for page in available if counts10[page] > 0),
                key=lambda page: (-int(counts10[page]), page),
            )[:32]
            hits10 = int(np.isin(pages[:10], ranked10).sum())
            row = sample[arm]
            if row != {
                "selected_pages": ranked,
                "oracle_witness_bytes": sum(int(page_bytes[page]) for page in ranked),
                "group_hit_mask": group_mask,
                "group_hits_at_100": group_mask.count("1"),
                "oracle_hit_mask": oracle_mask,
                "oracle_hits_at_100": oracle_mask.count("1"),
                "oracle_upper_hits_at_10": hits10,
            }:
                raise ValueError("page oracle validation sample differs")
            group_hits.append(group_mask.count("1"))
            group_hits10.append(group_mask[:10].count("1"))
            upper100.append(oracle_mask.count("1"))
            upper10.append(hits10)
        metrics[arm] = {
            "group_gt100_hits": sum(group_hits),
            "group_gt10_hits": sum(group_hits10),
            "oracle_gt100_hits": sum(upper100),
            "oracle_gt10_upper_hits": sum(upper10),
            "oracle_p05_gt100_hits": sorted(upper100)[math.ceil(0.05 * 1000) - 1],
            "oracle_sub90_queries": sum(value < 90 for value in upper100),
        }
    candidate = metrics["candidate"]
    if any(
        (metrics[arm]["group_gt100_hits"], metrics[arm]["group_gt10_hits"])
        != expected_group_hits[arm]
        for arm in ("candidate", "control")
    ):
        raise ValueError("page oracle prior group validation differs")
    passes = (
        candidate["oracle_gt100_hits"] >= 98_151
        and candidate["oracle_p05_gt100_hits"] >= 90
        and candidate["oracle_gt10_upper_hits"] >= 9_928
        and candidate["oracle_sub90_queries"] <= 49
    )
    decision = (
        "page-count-ceiling-advance-source-scores"
        if passes
        else "page-count-ceiling-killed"
    )
    if (
        evidence["metrics"] != metrics
        or result.get("metrics") != metrics
        or result.get("decision") != decision
    ):
        raise ValueError("page oracle validation aggregate differs")
    validation = {
        "schema": SCHEMA + "-validation",
        "decision": decision,
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
        "result_sha256": hashlib.sha256(result_body).hexdigest(),
        "metrics": metrics,
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "validation.json").write_bytes(_canonical(validation))
    return validation


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    validate(args.root, args.out)
