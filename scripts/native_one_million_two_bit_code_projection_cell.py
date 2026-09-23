#!/usr/bin/env python3
"""Truth-separated 1M projection of a page-local 200-byte code wave."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import math
from collections.abc import Sequence
from pathlib import Path

import numpy as np

from scripts.native_one_million_data_range_evaluation import evaluate_query_pages
from scripts.native_one_million_page_oracle_cell import _read_map
from scripts.native_one_million_selector_cell import (
    DEVELOPMENT_IDENTITIES,
    SOURCE_IDENTITIES,
)
from scripts.native_one_million_selector_evaluation import _query_truth
from scripts.native_one_million_two_bit_code_projection import (
    code_page_lengths,
    plan_code_wave,
)
from scripts.v97_row_width_screen import _authenticate_object

SCHEMA = "borsuk-one-million-two-bit-page-code-projection-v1"
PRIOR_PREFIX = (
    "s3://borsuk-bench-453182569524-euc1/research/"
    "native-one-million-data-range-selector/"
    "d9dd64ac00920eabd2200078291666757b2d25df/"
    "runs/relaion-1m-dev1000-a0001/"
)
PRIOR_TERMINAL_SHA256 = "ce0bf33bf97ab390e011474b180ea7c419f47ad1ea0c61d2c47ca4593fe725b5"
PRIOR_PLAN_SHA256 = "327b5739e67b6417664af545592bbe5d14448713d26ff8f8bd9ceee670b8e86d"
PRIOR_PLAN_SEAL_SHA256 = "25e03ffa1cd7d5a7eb3534e351fe32b26574afcfe33cc0bcbe24037bee488f53"
PRIOR_RANGE_SEAL_SHA256 = "4055a845a10e164b316d9aaa5961822aa4f9c2a8fdd3422a28ca582628a1ce5f"
PLAN_FILE = "two-bit-code-wave-plans.json"
PLAN_SEAL_FILE = "two-bit-code-wave-plan-seal.json"
EVIDENCE_FILE = "two-bit-code-wave-evidence.json"
RESULT_FILE = "two-bit-code-wave-result.json"


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, object]:
    body = path.read_bytes()
    value = json.loads(body)
    if type(value) is not dict or body != _canonical(value):
        raise ValueError(f"{path.name} canonical authority differs")
    return value


def _prior_authority(root: Path) -> dict[str, object]:
    terminal_path = root / "prior-range-terminal.json"
    if _sha(terminal_path) != PRIOR_TERMINAL_SHA256:
        raise ValueError("prior range terminal identity differs")
    terminal = _read_json(terminal_path)
    if (
        terminal.get("schema") != "borsuk-one-million-opq8-data-range-terminal-v1"
        or terminal.get("status") != "complete"
        or terminal.get("source_commit") != "d9dd64ac00920eabd2200078291666757b2d25df"
        or terminal.get("exit_code") != 0
    ):
        raise ValueError("prior range terminal differs")
    artifacts = terminal["artifacts"]
    for role, filename in (
        ("range-plans", "prior-range-plans.json"),
        ("range-plan-seal", "prior-range-plan-seal.json"),
        ("range-seal", "prior-range-seal.json"),
        ("seal", "seal.json"),
        ("page-map", "page-map.bin"),
        ("page-groups", "page-groups.bin"),
        ("page-bytes", "page-bytes.bin"),
    ):
        receipt = artifacts[role]
        path = root / filename
        if (
            receipt["role"] != role
            or receipt["uri"] != PRIOR_PREFIX + "artifacts/" + {
                "prior-range-plans.json": "range-plans.json",
                "prior-range-plan-seal.json": "range-plan-seal.json",
                "prior-range-seal.json": "range-seal.json",
            }.get(filename, filename)
            or path.stat().st_size != receipt["encoded_bytes"]
            or _sha(path) != receipt["sha256"]
        ):
            raise ValueError(f"prior range {role} artifact differs")
    if (
        artifacts["range-plans"]["sha256"] != PRIOR_PLAN_SHA256
        or artifacts["range-plan-seal"]["sha256"] != PRIOR_PLAN_SEAL_SHA256
        or artifacts["range-seal"]["sha256"] != PRIOR_RANGE_SEAL_SHA256
    ):
        raise ValueError("prior range frozen plan differs")
    return terminal


def _source_geometry(
    root: Path, *, expected_rows: int = 1_000_000,
    expected_pages: int = 7_278,
) -> tuple[dict[str, tuple[int, ...]], int, np.ndarray]:
    _authenticate_object("generation", root / "generation.json", SOURCE_IDENTITIES["generation"])
    generation = _read_json(root / "generation.json")
    if [run.get("kind") for run in generation.get("runs", [])] != ["base", "delta"]:
        raise ValueError("code-wave generation roles differ")
    base_pages = len(generation["runs"][0]["pages"])
    seal, _, membership, page_groups, _ = _read_map(root)
    range_seal = _read_json(root / "prior-range-seal.json")
    if (
        len(membership) != expected_rows
        or len(page_groups) != expected_pages
        or base_pages <= 0 or base_pages >= expected_pages
        or range_seal.get("map_seal_sha256") != _sha(root / "seal.json")
        or seal.get("rows") != expected_rows
        or seal.get("pages") != expected_pages
    ):
        raise ValueError("code-wave source geometry differs")
    counts = np.bincount(membership["page"].astype(np.intp), minlength=expected_pages)
    lengths = code_page_lengths(tuple(int(count) for count in counts), base_pages=base_pages)
    return lengths, base_pages, membership


def _prior_plans(root: Path) -> dict[str, object]:
    _prior_authority(root)
    plans = _read_json(root / "prior-range-plans.json")
    seal = _read_json(root / "prior-range-plan-seal.json")
    if (
        plans.get("schema") != "borsuk-one-million-opq8-data-range-v1-plans"
        or len(plans.get("samples", [])) != 1000
        or plans.get("source_seal_sha256") != PRIOR_RANGE_SEAL_SHA256
        or seal.get("plans_sha256") != PRIOR_PLAN_SHA256
        or seal.get("source_seal_sha256") != PRIOR_RANGE_SEAL_SHA256
    ):
        raise ValueError("prior range query plans differ")
    return plans


def _global_priority(
    prior: dict[str, object], base_pages: int, total_pages: int,
) -> tuple[int, ...]:
    values = prior["candidate"]["priority_pages"]
    priority: list[int] = []
    for item in values:
        if type(item) is not list or len(item) != 2:
            raise ValueError("code-wave page priority differs")
        role, page = item
        if role == "base" and type(page) is int and 0 <= page < base_pages:
            priority.append(page)
        elif role == "delta" and type(page) is int and 0 <= page < total_pages - base_pages:
            priority.append(base_pages + page)
        else:
            raise ValueError("code-wave page priority differs")
    if not priority or len(priority) != len(set(priority)):
        raise ValueError("code-wave page priority differs")
    return tuple(priority)


def run_plan(
    root: Path, out: Path | None = None, *, query_count: int = 1000, expected_rows: int = 1_000_000,
    expected_pages: int = 7_278,
) -> dict[str, object]:
    """Replan the sealed OPQ8 page order without any truth capability."""
    if (root / "truth.parquet").exists():
        raise ValueError("code-wave plan must be truth-free")
    lengths, base_pages, _ = _source_geometry(
        root, expected_rows=expected_rows, expected_pages=expected_pages,
    )
    historical = _prior_plans(root)
    if len(historical["samples"]) != query_count:
        raise ValueError("code-wave query cohort differs")
    samples = []
    for ordinal, prior in enumerate(historical["samples"]):
        if prior.get("query_ordinal") != ordinal:
            raise ValueError("code-wave query order differs")
        priority = _global_priority(prior, base_pages, expected_pages)
        samples.append({"query_ordinal": ordinal, "code": plan_code_wave(priority, lengths)})
    plans = {
        "schema": SCHEMA + "-plans",
        "prior_terminal_sha256": PRIOR_TERMINAL_SHA256,
        "prior_plans_sha256": PRIOR_PLAN_SHA256,
        "map_seal_sha256": _sha(root / "seal.json"),
        "generation_identity": dataclasses.asdict(SOURCE_IDENTITIES["generation"]),
        "row_bytes": 200,
        "samples": samples,
    }
    out = root if out is None else out
    out.mkdir(parents=True, exist_ok=True)
    body = _canonical(plans)
    (out / PLAN_FILE).write_bytes(body)
    (out / PLAN_SEAL_FILE).write_bytes(_canonical({
        "schema": SCHEMA + "-plan-seal",
        "plans_sha256": hashlib.sha256(body).hexdigest(),
        "prior_plans_sha256": PRIOR_PLAN_SHA256,
        "map_seal_sha256": plans["map_seal_sha256"],
    }))
    return plans


def _sealed_plans(root: Path, query_count: int) -> dict[str, object]:
    body = (root / PLAN_FILE).read_bytes()
    plans = _read_json(root / PLAN_FILE)
    seal = _read_json(root / PLAN_SEAL_FILE)
    _prior_authority(root)
    if (
        plans.get("schema") != SCHEMA + "-plans"
        or plans.get("prior_terminal_sha256") != PRIOR_TERMINAL_SHA256
        or plans.get("prior_plans_sha256") != PRIOR_PLAN_SHA256
        or plans.get("map_seal_sha256") != _sha(root / "seal.json")
        or plans.get("generation_identity") != dataclasses.asdict(SOURCE_IDENTITIES["generation"])
        or plans.get("row_bytes") != 200
        or len(plans.get("samples", [])) != query_count
        or seal != {
            "schema": SCHEMA + "-plan-seal",
            "plans_sha256": hashlib.sha256(body).hexdigest(),
            "prior_plans_sha256": PRIOR_PLAN_SHA256,
            "map_seal_sha256": plans["map_seal_sha256"],
        }
    ):
        raise ValueError("code-wave plan seal differs")
    return plans


def _metrics(samples: list[dict[str, object]], plans: list[dict[str, object]]) -> dict[str, int]:
    hits = [sample["hits_at_100"] for sample in samples]
    code_plans = [plan["code"] for plan in plans]
    return {
        "gt100_hits": sum(hits),
        "gt10_hits": sum(sample["hits_at_10"] for sample in samples),
        "target_gt100_hits": sum(sample["hit_kinds"].count("target") for sample in samples),
        "bridge_gt100_hits": sum(sample["hit_kinds"].count("bridge") for sample in samples),
        "p05_gt100_hits": sorted(hits)[math.ceil(len(hits) * .05) - 1],
        "sub90_queries": sum(hit < 90 for hit in hits),
        "maximum_gets": max(plan["gets"] for plan in code_plans),
        "maximum_bytes": max(plan["encoded_bytes"] for plan in code_plans),
        "total_gets": sum(plan["gets"] for plan in code_plans),
        "total_bytes": sum(plan["encoded_bytes"] for plan in code_plans),
        "minimum_target_pages": min(len(plan["target_pages"]) for plan in code_plans),
        "maximum_target_pages": max(len(plan["target_pages"]) for plan in code_plans),
        "minimum_included_pages": min(len(plan["included_pages"]) for plan in code_plans),
        "maximum_included_pages": max(len(plan["included_pages"]) for plan in code_plans),
    }


def run_evaluate(
    root: Path, out: Path, *, query_count: int = 1000,
    expected_rows: int = 1_000_000, expected_pages: int = 7_278,
) -> dict[str, object]:
    """Open truth only after the query-only code-wave plan is sealed."""
    lengths, base_pages, membership = _source_geometry(
        root, expected_rows=expected_rows, expected_pages=expected_pages,
    )
    plans = _sealed_plans(root, query_count)
    historical = _prior_plans(root)
    _, truth_ids = _query_truth(
        root / "queries.parquet", root / "truth.parquet",
        DEVELOPMENT_IDENTITIES, query_count=query_count,
    )
    positions = np.searchsorted(membership["id"], truth_ids)
    if (
        np.any(positions >= len(membership))
        or not np.array_equal(membership["id"][positions], truth_ids)
    ):
        raise ValueError("code-wave truth owner differs")
    truth_pages = membership["page"][positions]
    samples = []
    for ordinal, (item, prior) in enumerate(zip(plans["samples"], historical["samples"], strict=True)):
        if (
            item.get("query_ordinal") != ordinal
            or prior.get("query_ordinal") != ordinal
            or item["code"]["priority_pages"] != prior["candidate"]["priority_pages"]
        ):
            raise ValueError("code-wave query plan differs")
        priority = _global_priority(prior, base_pages, expected_pages)
        if item["code"] != plan_code_wave(priority, lengths):
            raise ValueError("code-wave sealed plan replay differs")
        evaluation = evaluate_query_pages(
            item["code"], tuple(int(page) for page in truth_pages[ordinal]), lengths,
        )
        samples.append({"query_ordinal": ordinal, **evaluation})
    metrics = _metrics(samples, plans["samples"])
    quality_advance = (
        metrics["gt100_hits"] >= 98_651
        and metrics["p05_gt100_hits"] >= 93
        and metrics["maximum_gets"] <= 32
        and metrics["maximum_bytes"] <= 16_777_216
    )
    evidence = {
        "schema": SCHEMA + "-evidence",
        "plans_sha256": _sha(root / PLAN_FILE),
        "truth_identity": dataclasses.asdict(DEVELOPMENT_IDENTITIES["truth"]),
        "samples": samples,
        "metrics": metrics,
    }
    out.mkdir(parents=True, exist_ok=True)
    evidence_body = _canonical(evidence)
    (out / EVIDENCE_FILE).write_bytes(evidence_body)
    result = {
        "schema": SCHEMA + "-result",
        "plans_sha256": evidence["plans_sha256"],
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
        "quality_advance_candidate": quality_advance,
        "metrics": metrics,
    }
    (out / RESULT_FILE).write_bytes(_canonical(result))
    return result


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("plan", "evaluate"))
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--out", type=Path, default=Path("evaluation"))
    args = parser.parse_args(argv)
    if args.phase == "plan":
        run_plan(args.root, args.out)
    else:
        run_evaluate(args.root, args.out)


if __name__ == "__main__":
    main()
