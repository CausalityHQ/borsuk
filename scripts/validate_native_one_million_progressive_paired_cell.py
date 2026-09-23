#!/usr/bin/env python3
"""Independent page-object and truth-mask replay for the paired score gate."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
from collections.abc import Sequence
from pathlib import Path

import numpy as np

from scripts.native_one_million_data_range_query import plan_ranked_pages
from scripts.native_one_million_page_oracle_cell import _read_map
from scripts.native_one_million_progressive_paired_cell import (
    EVIDENCE_FILE,
    PLAN_FILE,
    RESULT_FILE,
    SCHEMA,
    _code_authority,
    _data_lengths,
    _included_pages,
    _prior_plans,
    _read_json,
    _sealed_plans,
)
from scripts.native_one_million_selector_cell import DEVELOPMENT_IDENTITIES
from scripts.native_one_million_selector_evaluation import _query_truth
from scripts.native_progressive_page_objects import read_authenticated_page
from scripts.recount_native_one_million_data_range import recount_range_masks


def _canonical(value: object) -> bytes:
    import json

    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_closed(root: Path, out: Path, *, query_count: int = 1000) -> dict[str, object]:
    """Verify every split page, candidate set, range and ordered truth mask."""
    prior = _prior_plans(root)
    plans = _sealed_plans(root, query_count)
    code_seal, _mean, records = _code_authority(root)
    generation = _read_json(root / "generation.json")
    base_pages = len(generation["runs"][0]["pages"])
    counts = code_seal["page_row_counts"]
    first = 0
    for page, count in enumerate(counts):
        decoded = read_authenticated_page(root, code_seal, page, authenticated_seal=True)
        if not np.array_equal(decoded, records[first:first + count]):
            raise ValueError(f"progressive paired page {page} rejoin differs")
        first += count
    if first != len(records):
        raise ValueError("progressive paired record count differs")
    lengths = _data_lengths(root, base_pages)
    for plane, width in (("sign", 104), ("magnitude", 96)):
        if any(
            code_seal["pages"][plane][page][3] != count * width
            for page, count in enumerate(counts)
        ):
            raise ValueError("progressive paired code page length differs")
    if len(plans["samples"]) != len(prior["samples"]):
        raise ValueError("progressive paired cohort differs")
    for ordinal, (item, old) in enumerate(zip(plans["samples"], prior["samples"], strict=True)):
        if item.get("query_ordinal") != ordinal or old.get("query_ordinal") != ordinal:
            raise ValueError("progressive paired query order differs")
        cover = _included_pages(old, base_pages)
        for plane in ("sign", "magnitude"):
            prior_wave = old["code"][plane]
            expected_bytes = sum(
                code_seal["pages"][plane][page if role == "base" else base_pages + page][3]
                for role, first, end in prior_wave["ranges"]
                for page in range(first, end)
            )
            if (
                expected_bytes != prior_wave["encoded_bytes"]
                or prior_wave["gets"] != len(prior_wave["ranges"])
                or prior_wave["gets"] > 32
                or expected_bytes > 16_777_216
            ):
                raise ValueError("progressive paired code-wave geometry differs")
        for arm in ("two_bit", "source"):
            priorities = tuple(
                page if role == "base" else base_pages + page
                for role, page in item[arm]["priority_pages"]
            )
            if len(priorities) != len(cover) or set(priorities) != set(cover):
                raise ValueError("progressive paired priority leaves code cover")
            if item[arm] != plan_ranked_pages(priorities, lengths):
                raise ValueError("progressive paired data admission differs")
    evidence = _read_json(out / EVIDENCE_FILE)
    result = _read_json(out / RESULT_FILE)
    if (
        evidence.get("schema") != SCHEMA + "-evidence"
        or evidence.get("plans_sha256") != _sha(root / PLAN_FILE)
        or evidence.get("truth_identity") != dataclasses.asdict(DEVELOPMENT_IDENTITIES["truth"])
        or result.get("schema") != SCHEMA + "-result"
        or result.get("evidence_sha256") != _sha(out / EVIDENCE_FILE)
        or len(evidence.get("samples", [])) != query_count
    ):
        raise ValueError("progressive paired evidence identity differs")
    _, _, membership, _, _ = _read_map(root)
    _, truth_ids = _query_truth(
        root / "queries.parquet", root / "truth.parquet",
        DEVELOPMENT_IDENTITIES, query_count=query_count,
    )
    positions = np.searchsorted(membership["id"], truth_ids)
    if np.any(positions >= len(membership)) or not np.array_equal(membership["id"][positions], truth_ids):
        raise ValueError("progressive paired truth owner differs")
    truth_pages = membership["page"][positions]
    replay, metrics = recount_range_masks(
        plans["samples"], [[int(page) for page in row] for row in truth_pages],
        lengths, arms=("two_bit", "source"),
    )
    gross_lost = sum(
        sum(a == "1" and b == "0" for a, b in zip(
            item["source"]["hit_mask"], item["two_bit"]["hit_mask"], strict=True,
        ))
        for item in replay
    )
    recovered = sum(
        sum(a == "0" and b == "1" for a, b in zip(
            item["source"]["hit_mask"], item["two_bit"]["hit_mask"], strict=True,
        ))
        for item in replay
    )
    net_loss = metrics["source"]["gt100_hits"] - metrics["two_bit"]["gt100_hits"]
    if net_loss != gross_lost - recovered:
        raise ValueError("progressive paired independent loss differs")
    metrics["paired_loss"] = {
        "gross_lost": gross_lost, "recovered": recovered, "net_loss": net_loss,
    }
    coded = metrics["two_bit"]
    advance = (
        coded["gt100_hits"] >= 98_151 and coded["gt10_hits"] >= 9_928
        and coded["p05_gt100_hits"] >= 90 and coded["sub90_queries"] <= 49
        and coded["maximum_gets"] <= 32 and coded["maximum_bytes"] <= 16_777_216
        and net_loss <= 300
    )
    if (
        evidence.get("samples") != replay or evidence.get("metrics") != metrics
        or result.get("metrics") != metrics
        or result.get("advance_candidate") is not advance
        or result.get("claim_eligible") is not False
    ):
        raise ValueError("progressive paired result replay differs")
    validation = {
        "schema": SCHEMA + "-validation", "valid": True, "queries": query_count,
        "plans_sha256": _sha(root / PLAN_FILE),
        "evidence_sha256": _sha(out / EVIDENCE_FILE),
        "result_sha256": _sha(out / RESULT_FILE), "metrics": metrics,
    }
    (out / "progressive-paired-validation.json").write_bytes(_canonical(validation))
    return validation


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    validate_closed(args.root, args.out)


if __name__ == "__main__":
    main()
