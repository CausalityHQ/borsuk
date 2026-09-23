"""Independent replay of closed 100k pq96 range plans and truth masks."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections.abc import Sequence
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import read_geometric_router_parquet
from scripts.native_hundred_thousand_pq96_cell import (
    PLANS_FILE,
    SCHEMA,
    SOURCE_SEAL_FILE,
    _sealed_plans,
    _source_authority,
)
from scripts.native_one_million_data_range_evaluation import evaluate_query_pages
from scripts.native_one_million_source_priority import source_distance_batch
from scripts.native_page_microcluster_cell import FROZEN_INPUTS, _read_queries_truth
from scripts.native_pq96_groups import open_pq96_groups, score_pq96
from scripts.native_rotated_sign96 import BATCH_ROWS
from scripts.native_rotated_two_bit_cell import PRIOR_PAGES, PRIOR_TREE
from scripts.validate_native_geometric_layout_result import _read_geometric_queries


def _canonical(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _read_canonical(path: Path) -> tuple[dict[str, object], bytes]:
    body = path.read_bytes()
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{path.name} differs") from error
    if type(value) is not dict or body != _canonical(value):
        raise ValueError(f"{path.name} differs")
    return value, body


def _metrics(samples: list[dict[str, object]], plans: list[dict[str, object]]) -> dict[str, object]:
    metrics: dict[str, object] = {}
    for arm in ("pq96", "source"):
        hits = [sample[arm]["hits_at_100"] for sample in samples]
        hits10 = [sample[arm]["hits_at_10"] for sample in samples]
        arm_plans = [plan[arm] for plan in plans]
        metrics[arm] = {
            "gt100_hits": sum(hits),
            "gt10_hits": sum(hits10),
            "target_gt100_hits": sum(sample[arm]["hit_kinds"].count("target") for sample in samples),
            "bridge_gt100_hits": sum(sample[arm]["hit_kinds"].count("bridge") for sample in samples),
            "p05_gt100_hits": sorted(hits)[math.ceil(len(hits) * .05) - 1],
            "sub90_queries": sum(hit < 90 for hit in hits),
            "maximum_gets": max(plan["gets"] for plan in arm_plans),
            "maximum_bytes": max(plan["encoded_bytes"] for plan in arm_plans),
            "total_gets": sum(plan["gets"] for plan in arm_plans),
            "total_bytes": sum(plan["encoded_bytes"] for plan in arm_plans),
        }
    metrics["paired_gt100"] = {
        "pq96_better": sum(sample["pq96"]["hits_at_100"] > sample["source"]["hits_at_100"] for sample in samples),
        "source_better": sum(sample["pq96"]["hits_at_100"] < sample["source"]["hits_at_100"] for sample in samples),
        "tied": sum(sample["pq96"]["hits_at_100"] == sample["source"]["hits_at_100"] for sample in samples),
    }
    return metrics


def _independent_page_priority(
    scores: np.ndarray, pages: np.ndarray, positions: np.ndarray,
) -> list[list[object]]:
    """Rebuild top-row owner priority without the production rank helper."""
    top = np.lexsort((positions, scores))[:min(100, len(scores))]
    counts = np.bincount(pages[top], minlength=int(pages.max()) + 1)
    minima = np.full(len(counts), np.inf, dtype=np.float64)
    np.minimum.at(minima, pages, scores.astype(np.float64))
    present = np.unique(pages)
    ordered = sorted(
        (int(page) for page in present),
        key=lambda page: (-int(counts[page] > 0), -int(counts[page]),
                          float(minima[page]), page),
    )
    return [["base", page] for page in ordered]


def validate_closed(
    root: Path, out: Path, *, query_count: int = 1000,
    expected_rows: int = 100_000, expected_pages: int = 166,
    maximum_gets: int = 32, maximum_bytes: int = 16_777_216,
) -> dict[str, object]:
    """Validate every plan and hit mask without the source-score planner."""
    _, vectors, membership, ordinals, counts, order_sha = _source_authority(
        root, expected_rows, expected_pages
    )
    source_seal, _ = _read_canonical(root / SOURCE_SEAL_FILE)
    if (
        source_seal.get("prior_physical_order_sha256") != order_sha
        or source_seal.get("rows") != expected_rows
        or source_seal.get("pages") != expected_pages
    ):
        raise ValueError("pq96 validation source seal differs")
    reader = open_pq96_groups(
        root / "pq96", source_sha256=FROZEN_INPUTS.layout.source.sha256,
        layout_sha256=FROZEN_INPUTS.membership.sha256,
        physical_order_sha256=order_sha,
        seal_sha256=source_seal["pq96_seal_sha256"],
    )
    if reader.page_row_counts != counts:
        raise ValueError("pq96 validation code layout differs")
    plans = _sealed_plans(root, query_count)
    evidence, evidence_body = _read_canonical(out / "pq96-evidence.json")
    result, result_body = _read_canonical(out / "pq96-result.json")
    if (
        evidence.get("schema") != SCHEMA + "-evidence"
        or evidence.get("plans_sha256") != hashlib.sha256((root / PLANS_FILE).read_bytes()).hexdigest()
        or result.get("schema") != SCHEMA + "-result"
        or result.get("plans_sha256") != evidence["plans_sha256"]
        or result.get("evidence_sha256") != hashlib.sha256(evidence_body).hexdigest()
        or len(evidence.get("samples", [])) != query_count
        or result.get("resource_gate_pending") is not True
    ):
        raise ValueError("pq96 validation evidence identity differs")
    _, truth = _read_queries_truth(root, FROZEN_INPUTS)
    if len(truth) != query_count:
        raise ValueError("pq96 validation truth cohort differs")
    owner = {row.stable_id: row.page_ordinal for row in membership}
    router = read_geometric_router_parquet(
        root / "tree.parquet", root / "pages.parquet", FROZEN_INPUTS.layout,
        membership, PRIOR_TREE, PRIOR_PAGES,
    )
    page_lengths = {"base": tuple(int(page.encoded_page_bytes) for page in router.pages)}
    if len(page_lengths["base"]) != len(counts):
        raise ValueError("pq96 validation page layout differs")
    queries = _read_geometric_queries(
        root / "queries.parquet", FROZEN_INPUTS.queries, 768,
    )
    if len(queries) != query_count:
        raise ValueError("pq96 validation query cohort differs")
    records = np.concatenate(
        [reader.group_records(index) for index in range(len(reader.ranges))]
    )
    physical_vectors = vectors[np.asarray(ordinals, dtype=np.intp)]
    row_pages = np.repeat(np.arange(len(counts), dtype=np.intp), counts)
    row_groups = row_pages // 4
    group_positions = {
        group: np.flatnonzero(row_groups == group)
        for group in range(len(reader.ranges))
    }
    for query, sample in zip(queries, plans["samples"], strict=True):
        positions = np.concatenate(
            [group_positions[group] for group in sample["selected_groups"]]
        )
        positions.sort()
        pages = row_pages[positions]
        pq_scores = score_pq96(query, reader, records[positions])
        source_scores = np.empty(len(positions), dtype=np.float64)
        for first in range(0, len(positions), BATCH_ROWS):
            last = min(first + BATCH_ROWS, len(positions))
            source_scores[first:last] = source_distance_batch(
                query[None, :], physical_vectors[positions[first:last]]
            )[0]
        if (
            sample["pq96"]["priority_pages"] != _independent_page_priority(
                pq_scores, pages, positions,
            )
            or sample["source"]["priority_pages"] != _independent_page_priority(
                source_scores, pages, positions,
            )
        ):
            raise ValueError("pq96 validation row-score priority differs")
    recounted: list[dict[str, object]] = []
    for ordinal, (plan, neighbors) in enumerate(zip(plans["samples"], truth, strict=True)):
        if (
            len(neighbors) != 100 or len(set(neighbors)) != 100
            or any(item not in owner for item in neighbors)
        ):
            raise ValueError("pq96 validation truth owners differ")
        pages = [owner[item] for item in neighbors]
        sample: dict[str, object] = {"query_ordinal": ordinal}
        for arm in ("pq96", "source"):
            sample[arm] = evaluate_query_pages(
                plan[arm], pages, page_lengths,
                maximum_gets=maximum_gets, maximum_bytes=maximum_bytes,
            )
        recounted.append(sample)
    metrics = _metrics(recounted, plans["samples"])
    paired_deltas = []
    for sample, plan in zip(recounted, plans["samples"], strict=True):
        signed, exact = plan["pq96"], plan["source"]
        paired_deltas.append({
            "query_ordinal": sample["query_ordinal"],
            "gt100_delta": sample["pq96"]["hits_at_100"] - sample["source"]["hits_at_100"],
            "gt10_delta": sample["pq96"]["hits_at_10"] - sample["source"]["hits_at_10"],
            "target_page_symmetric_difference": len(
                set(map(tuple, signed["target_pages"]))
                ^ set(map(tuple, exact["target_pages"]))
            ),
            "included_page_symmetric_difference": len(
                set(map(tuple, signed["included_pages"]))
                ^ set(map(tuple, exact["included_pages"]))
            ),
            "interval_symmetric_difference": len(
                set(map(tuple, signed["ranges"]))
                ^ set(map(tuple, exact["ranges"]))
            ),
            "data_get_delta": signed["gets"] - exact["gets"],
        })
    projection = {
        "total_code_gets": sum(plan["projected_code_gets"] for plan in plans["samples"]),
        "total_code_bytes": sum(plan["projected_code_bytes"] for plan in plans["samples"]),
        "maximum_code_gets": max(plan["projected_code_gets"] for plan in plans["samples"]),
        "maximum_code_bytes": max(plan["projected_code_bytes"] for plan in plans["samples"]),
        "model_bytes": (root / "pq96" / "model.bin").stat().st_size,
    }
    pq = metrics["pq96"]
    source = metrics["source"]
    advance = (
        source["gt100_hits"] >= 98_000
        and pq["gt100_hits"] >= source["gt100_hits"] - 300
        and pq["p05_gt100_hits"] >= 90
        and pq["sub90_queries"] <= source["sub90_queries"] + 10
        and pq["maximum_gets"] <= 32
        and pq["maximum_bytes"] <= 16_777_216
    )
    if (
        evidence["samples"] != recounted
        or evidence["metrics"] != metrics
        or evidence.get("paired_deltas") != paired_deltas
        or evidence.get("code_projection") != projection
        or result["metrics"] != metrics
        or result.get("code_projection") != projection
        or result.get("quality_advance_candidate") is not advance
    ):
        raise ValueError("pq96 validation masks or metrics differ")
    validation = {
        "schema": SCHEMA + "-validation",
        "plans_sha256": evidence["plans_sha256"],
        "evidence_sha256": result["evidence_sha256"],
        "result_sha256": hashlib.sha256(result_body).hexdigest(),
        "query_count": query_count,
        "valid": True,
    }
    (out / "pq96-validation.json").write_bytes(_canonical(validation))
    return validation


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--out", type=Path, default=Path("evaluation"))
    args = parser.parse_args(argv)
    validate_closed(args.root, args.out)


if __name__ == "__main__":
    main()
